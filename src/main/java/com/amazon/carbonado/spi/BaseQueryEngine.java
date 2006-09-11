/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.spi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.cojen.util.BeanComparator;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;

import com.amazon.carbonado.cursor.FilteredCursor;
import com.amazon.carbonado.cursor.MergeSortBuffer;
import com.amazon.carbonado.cursor.SortBuffer;
import com.amazon.carbonado.cursor.SortedCursor;
import com.amazon.carbonado.cursor.UnionCursor;

import com.amazon.carbonado.qe.BoundaryType;

/**
 * Basis for a rule-based query engine. It takes care of index selection,
 * filtering, sorting, and unions.
 *
 * @author Brian S O'Neill
 * @deprecated Use {@link com.amazon.carbonado.qe.QueryEngine}
 */
public abstract class BaseQueryEngine<S extends Storable> extends BaseQueryCompiler<S> {
    private static PropertyFilter[] NO_FILTERS = new PropertyFilter[0];

    /**
     * Compares two objects which are assumed to be Comparable. If one value is
     * null, it is treated as being higher. This consistent with all other
     * property value comparisons in carbonado.
     */
    static int compareWithNullHigh(Object a, Object b) {
        return a == null ? (b == null ? 0 : -1) : (b == null ? 1 : ((Comparable) a).compareTo(b));
    }

    private final Repository mRepository;
    private final Storage<S> mStorage;
    private final StorableIndex<S> mPrimaryKeyIndex;
    private final StorableIndexSet<S> mIndexSet;

    String mMergeSortTempDir;

    /**
     * @param info info for Storable
     * @param repo repository for entering transactions
     * @param storage source for queried objects
     * @param primaryKeyIndex optional parameter representing primary key index
     * @param indexSet optional parameter representing all available indexes.
     * Constructor makes a local copy of the set.
     * @throws IllegalArgumentException if primaryKeyIndex is null and indexSet
     * is empty
     */
    protected BaseQueryEngine(StorableInfo<S> info,
                              Repository repo,
                              Storage<S> storage,
                              StorableIndex<S> primaryKeyIndex,
                              StorableIndexSet<S> indexSet) {
        super(info);
        if (primaryKeyIndex == null && (indexSet == null || indexSet.size() == 0)) {
            throw new IllegalArgumentException();
        }
        mRepository = repo;
        mStorage = storage;
        mPrimaryKeyIndex = primaryKeyIndex;
        mIndexSet = (indexSet == null || indexSet.size() == 0) ? null
            : new StorableIndexSet<S>(indexSet);
    }

    /**
     * @param tempDir directory to store temp files for merge sorting, or null
     * for default
     */
    protected void setMergeSortTempDirectory(String tempDir) {
        mMergeSortTempDir = tempDir;
    }

    @SuppressWarnings("unchecked")
    protected Query<S> compileQuery(final FilterValues<S> values,
                                    final OrderedProperty<S>[] orderings)
        throws FetchException, UnsupportedOperationException
    {
        if (values == null) {
            // Perform requested full scan.
            return fullScan(values, orderings);
        }

        final Filter<S> originalFilter = values.getFilter();
        final Filter<S> dnfFilter = originalFilter.disjunctiveNormalForm();

        // Analyze the disjunctive normal form, breaking down the query into
        // separate queries that can be unioned together.

        IndexAnalysis<S> analysis = new IndexAnalysis<S>(mPrimaryKeyIndex, mIndexSet, orderings);
        dnfFilter.accept(analysis, null);

        if (analysis.noBestIndex()) {
            // Fallback to full scan for everything if no best index found for
            // just one query component.
            return fullScan(values, orderings);
        }

        OrderedProperty<S>[] totalOrderings = null;
        ensureTotalOrdering:
        if (analysis.getResults().size() > 1) {
            // Union will be performed, and so a total ordering is required.

            // TODO: The logic in this section needs to be totally reworked. It
            // does a terrible job of finding the best total ordering, often
            // performing full sorts when not needed. Essentially, inefficient
            // query plans can get generated.

            // If all selected indexes are unique and have the same effective ordering, then
            // nothing special needs to be done to ensure total ordering.
            OrderedProperty<S>[] effectiveOrderings = null;
            totalOrderCheck:
            if (orderings == null || orderings.length == 0) {
                for (IndexSelector.IndexFitness<S> result : analysis.getResults()) {
                    StorableIndex<S> index = result.getIndex();
                    if (!index.isUnique()) {
                        break totalOrderCheck;
                    }
                    if (effectiveOrderings == null) {
                        effectiveOrderings = result.getEffectiveOrderings();
                        continue;
                    }
                    if (!Arrays.equals(effectiveOrderings, result.getEffectiveOrderings())) {
                        break totalOrderCheck;
                    }
                }
                // All indexes already define a total ordering.
                totalOrderings = effectiveOrderings;
                break ensureTotalOrdering;
            }

            // Augment the ordering with elements of a unique index.

            // Count how often an index has been used.
            Map<StorableIndex<S>, Integer> counts = new LinkedHashMap<StorableIndex<S>, Integer>();

            for (IndexSelector.IndexFitness<S> result : analysis.getResults()) {
                StorableIndex<S> index = result.getIndex();
                counts.put(index, (counts.containsKey(index)) ? (counts.get(index) + 1) : 1);
            }

            // Find the unique index that has been selected most often.
            StorableIndex<S> unique = mPrimaryKeyIndex;
            int uniqueCount = 0;
            for (Map.Entry<StorableIndex<S>, Integer> entry : counts.entrySet()) {
                if (entry.getKey().isUnique() && entry.getValue() > uniqueCount) {
                    unique = entry.getKey();
                    uniqueCount = entry.getValue();
                }
            }

            if (unique == null) {
                // Select first found unique index.
                for (StorableIndex<S> index : mIndexSet) {
                    if (index.isUnique()) {
                        unique = index;
                        break;
                    }
                }
                if (unique == null) {
                    throw new UnsupportedOperationException
                        ("Cannot perform union; sort requires at least one unique index");
                }
            }

            // To avoid full sorts, choose an index which is already being used
            // for its ordering. It may have a range filter or handled
            // orderings.
            StorableIndex<S> best = null;
            int bestCount = 0;
            for (IndexSelector.IndexFitness<S> result : analysis.getResults()) {
                if ((result.getInclusiveRangeStartFilters().length > 0 ||
                     result.getExclusiveRangeStartFilters().length > 0 ||
                     result.getInclusiveRangeEndFilters().length > 0 ||
                     result.getExclusiveRangeEndFilters().length > 0) &&
                    (result.getHandledOrderings() != null ||
                     result.getRemainderOrderings() == null)) {

                    StorableIndex<S> index = result.getIndex();
                    int count = counts.get(index);

                    if (count > bestCount) {
                        best = index;
                        bestCount = count;
                    }
                }
            }

            {
                int newLength = (orderings == null ? 0 : orderings.length)
                    + (best == null ? 0 : best.getPropertyCount())
                    + unique.getPropertyCount();
                totalOrderings = new OrderedProperty[newLength];

                int j = 0;
                if (orderings != null) {
                    for (int i=0; i<orderings.length; i++) {
                        totalOrderings[j++] = orderings[i];
                    }
                }
                if (best != null) {
                    for (int i=0; i<best.getPropertyCount(); i++) {
                        totalOrderings[j++] = OrderedProperty.get
                            (best.getProperty(i), best.getPropertyDirection(i));
                    }
                }
                for (int i=0; i<unique.getPropertyCount(); i++) {
                    totalOrderings[j++] = OrderedProperty.get
                        (unique.getProperty(i), unique.getPropertyDirection(i));
                }
            }

            // Augmented total orderings may contain redundancies, which are
            // removed by index selector. Running the analysis again may be
            // produce the exact same results as before. No harm done.

            analysis = new IndexAnalysis<S>(mPrimaryKeyIndex, mIndexSet, totalOrderings);
            dnfFilter.accept(analysis, null);

            if (analysis.noBestIndex()) {
                // Fallback to full scan for everything if no best index found for
                // just one query component.
                return fullScan(values, orderings);
            }
        }

        // Attempt to reduce the number of separate cursors need to be opened for union.
        analysis.reduceResults();

        List<CursorFactory<S>> subFactories = new ArrayList<CursorFactory<S>>();

        for (IndexSelector.IndexFitness<S> result : analysis.getResults()) {
            CursorFactory<S> subFactory;

            // Determine if KeyCursorFactory should be used instead.
            boolean isKeyFilter = result.isKeyFilter();
            if (isKeyFilter) {
                subFactory = new KeyCursorFactory<S>
                    (this, result.getIndex(), result.getExactFilter());
            } else {
                subFactory = new IndexCursorFactory<S>
                    (this, result.getIndex(),
                     result.shouldReverseOrder(), result.shouldReverseRange(),
                     result.getExactFilter(),
                     result.getInclusiveRangeStartFilters(),
                     result.getExclusiveRangeStartFilters(),
                     result.getInclusiveRangeEndFilters(),
                     result.getExclusiveRangeEndFilters());
            }

            Filter<S> remainderFilter = result.getRemainderFilter();
            if (remainderFilter != null) {
                subFactory = new FilteredCursorFactory<S>(this, subFactory, remainderFilter);
            }

            if (!isKeyFilter) {
                OrderedProperty<S>[] remainderOrderings = result.getRemainderOrderings();
                if (remainderOrderings != null && remainderOrderings.length > 0) {
                    subFactory = new SortedCursorFactory<S>
                        (this, subFactory, result.getHandledOrderings(), remainderOrderings);
                }
            }

            subFactories.add(subFactory);
        }

        CursorFactory<S> factory = UnionedCursorFactory
            .createUnion(this, subFactories, totalOrderings);

        return CompiledQuery.create(mRepository, mStorage, values, orderings, this, factory);
    }

    private Query<S> fullScan(FilterValues<S> values, OrderedProperty<S>[] orderings)
        throws FetchException
    {
        // Try to select index that has best ordering.
        IndexSelector<S> selector = new IndexSelector<S>(null, orderings);
        StorableIndex<S> best = mPrimaryKeyIndex;

        if (mIndexSet != null) {
            for (StorableIndex<S> candidate : mIndexSet) {
                int cmpResult = selector.compare(best, candidate);
                if (cmpResult > 0) {
                    best = candidate;
                }
            }
        }

        IndexSelector.IndexFitness<S> result = selector.examine(best);

        CursorFactory<S> factory;
        if (result == null || result.isUseless()) {
            factory = new FullScanCursorFactory<S>(this, mPrimaryKeyIndex);
            if (values != null) {
                factory = new FilteredCursorFactory<S>(this, factory, values.getFilter());
            }
            if (orderings != null && orderings.length > 0) {
                factory = new SortedCursorFactory<S>(this, factory, null, orderings);
            }
        } else {
            factory = new IndexCursorFactory<S>
                (this, result.getIndex(),
                 result.shouldReverseOrder(), result.shouldReverseRange(),
                 result.getExactFilter(),
                 result.getInclusiveRangeStartFilters(),
                 result.getExclusiveRangeStartFilters(),
                 result.getInclusiveRangeEndFilters(),
                 result.getExclusiveRangeEndFilters());

            Filter<S> remainderFilter = result.getRemainderFilter();
            if (remainderFilter != null) {
                factory = new FilteredCursorFactory<S>(this, factory, remainderFilter);
            }

            OrderedProperty<S>[] remainderOrderings = result.getRemainderOrderings();
            if (remainderOrderings != null && remainderOrderings.length > 0) {
                factory = new SortedCursorFactory<S>
                    (this, factory, result.getHandledOrderings(), remainderOrderings);
            }
        }

        return CompiledQuery.create(mRepository, mStorage, values, orderings, this, factory);
    }

    /**
     * Returns the primary Storage object in this object.
     */
    protected final Storage<S> getStorage() {
        return mStorage;
    }

    /**
     * Returns the storage object that the given index applies to. By default,
     * this method returns the primary storage. Override if indexes may be
     * defined in multiple storages.
     */
    protected Storage<S> getStorageFor(StorableIndex<S> index) {
        return mStorage;
    }

    /**
     * Return a new Cursor instance constrained by the given parameters. The
     * index values are aligned with the index properties at property index
     * 0. An optional start or end boundary matches up with the index property
     * following the last of the index values.
     *
     * @param index index to open, which may be the primary key index
     * @param exactValues optional list of exactly matching values to apply to index
     * @param rangeStartBoundary start boundary type
     * @param rangeStartValue value to start at if boundary is not open
     * @param rangeEndBoundary end boundary type
     * @param rangeEndValue value to end at if boundary is not open
     * @param reverseRange indicates that range operates on a property that is
     * ordered in reverse (this parameter might also be true simply because
     * reverseOrder is true)
     * @param reverseOrder when true, iteration is reversed
     */
    protected abstract Cursor<S> openCursor(StorableIndex<S> index,
                                            Object[] exactValues,
                                            BoundaryType rangeStartBoundary,
                                            Object rangeStartValue,
                                            BoundaryType rangeEndBoundary,
                                            Object rangeEndValue,
                                            boolean reverseRange,
                                            boolean reverseOrder)
        throws FetchException;

    /**
     * Return a new Cursor instance which is expected to fetch at most one
     * object. The chosen index is unique, and a primary or alternate key is
     * contained within it.
     * <p>
     * Subclasses are encouraged to override this method and provide a more
     * efficient implementation.
     *
     * @param index index to open, which may be the primary key index
     * @param exactValues first values to set for index; length may be smaller
     * than index property count
     */
    protected Cursor<S> openKeyCursor(StorableIndex<S> index,
                                      Object[] exactValues)
        throws FetchException
    {
        return openCursor(index, exactValues,
                          BoundaryType.OPEN, null,
                          BoundaryType.OPEN, null,
                          false,
                          false);
    }

    @SuppressWarnings("unchecked")
    Comparator<S> makeComparator(OrderedProperty<S>[] orderings) {
        if (orderings == null) {
            return null;
        }

        BeanComparator bc = BeanComparator.forClass(getStorableInfo().getStorableType());

        for (OrderedProperty<S> property : orderings) {
            bc = bc.orderBy(property.getChainedProperty().toString());
            bc = bc.caseSensitive();
            if (property.getDirection() == Direction.DESCENDING) {
                bc = bc.reverse();
            }
        }

        return bc;
    }

    private static class CompiledQuery<S extends Storable> extends BaseQuery<S> {
        private final BaseQueryEngine<S> mEngine;
        private final CursorFactory<S> mFactory;

        static <S extends Storable> Query<S> create(Repository repo,
                                                    Storage<S> storage,
                                                    FilterValues<S> values,
                                                    OrderedProperty<S>[] orderings,
                                                    BaseQueryEngine<S> engine,
                                                    CursorFactory<S> factory)
            throws FetchException
        {
            if (factory == null) {
                throw new IllegalArgumentException();
            }
            factory = factory.getActualFactory();
            return new CompiledQuery<S>(repo, storage, values, orderings, engine, factory);
        }

        private CompiledQuery(Repository repo,
                              Storage<S> storage,
                              FilterValues<S> values,
                              OrderedProperty<S>[] orderings,
                              BaseQueryEngine<S> engine,
                              CursorFactory<S> factory)
            throws FetchException
        {
            super(repo, storage, values, orderings);
            mEngine = engine;
            mFactory = factory;
        }

        private CompiledQuery(Repository repo,
                              Storage<S> storage,
                              FilterValues<S> values,
                              String[] orderings,
                              BaseQueryEngine<S> engine,
                              CursorFactory<S> factory)
        {
            super(repo, storage, values, orderings);
            mEngine = engine;
            mFactory = factory;
        }

        public Query<S> orderBy(String property)
            throws FetchException, UnsupportedOperationException
        {
            return mEngine.getOrderedQuery(getFilterValues(), property);
        }

        public Query<S> orderBy(String... properties)
            throws FetchException, UnsupportedOperationException
        {
            return mEngine.getOrderedQuery(getFilterValues(), properties);
        }

        public Cursor<S> fetch() throws FetchException {
            return mFactory.openCursor(getFilterValues());
        }

        public long count() throws FetchException {
            return mFactory.count(getFilterValues());
        }

        public boolean printNative(Appendable app, int indentLevel) throws IOException {
            return mFactory.printNative(app, indentLevel, getFilterValues());
        }

        public boolean printPlan(Appendable app, int indentLevel) throws IOException {
            return mFactory.printPlan(app, indentLevel, getFilterValues());
        }

        protected BaseQuery<S> newInstance(FilterValues<S> values) {
            return new CompiledQuery<S>
                (getRepository(), getStorage(), values, getOrderings(), mEngine, mFactory);
        }
    }

    private static interface CursorFactory<S extends Storable> {
        Cursor<S> openCursor(FilterValues<S> values) throws FetchException;

        long count(FilterValues<S> values) throws FetchException;

        /**
         * Append filter rules to the given filter.
         *
         * @param filter initial filter, might be null.
         */
        Filter<S> buildFilter(Filter<S> filter);

        /**
         * Applies an ordering to the given query in a new query.
         */
        Query<S> applyOrderBy(Query<S> query) throws FetchException;

        /**
         * Returns the storage object that this factory needs to use. Usually,
         * this is the same as the primary. If multiple storages are needed,
         * then null is returned. In either case, if the storage is not the
         * primary, then this factory cannot be used. Use the factory from
         * getActualFactory instead.
         */
        Storage<S> getActualStorage();

        /**
         * Returns another instance of this factory that uses the proper
         * storage.
         */
        CursorFactory<S> getActualFactory() throws FetchException;

        /**
         * @param values optional
         */
        boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException;

        /**
         * @param values optional
         */
        boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException;
    }

    private abstract static class AbstractCursorFactory<S extends Storable>
        implements CursorFactory<S>
    {
        protected final BaseQueryEngine<S> mEngine;

        AbstractCursorFactory(BaseQueryEngine<S> engine) {
            mEngine = engine;
        }

        public long count(FilterValues<S> values) throws FetchException {
            Cursor<S> cursor = openCursor(values);
            try {
                long count = cursor.skipNext(Integer.MAX_VALUE);
                if (count == Integer.MAX_VALUE) {
                    int amt;
                    while ((amt = cursor.skipNext(Integer.MAX_VALUE)) > 0) {
                        count += amt;
                    }
                }
                return count;
            } finally {
                cursor.close();
            }
        }

        public CursorFactory<S> getActualFactory() throws FetchException {
            Storage<S> storage = getActualStorage();
            if (storage == mEngine.getStorage()) {
                return this;
            }
            return new QueryCursorFactory<S>(this, storage);
        }

        public boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            return false;
        }

        void indent(Appendable app, int indentLevel) throws IOException {
            for (int i=0; i<indentLevel; i++) {
                app.append(' ');
            }
        }
    }

    private static class IndexCursorFactory<S extends Storable>
        extends AbstractCursorFactory<S>
    {
        protected final StorableIndex<S> mIndex;

        private final boolean mReverseOrder;
        private final boolean mReverseRange;
        private final Filter<S> mExactFilter;
        private final PropertyFilter<S>[] mInclusiveRangeStartFilters;
        private final PropertyFilter<S>[] mExclusiveRangeStartFilters;
        private final PropertyFilter<S>[] mInclusiveRangeEndFilters;
        private final PropertyFilter<S>[] mExclusiveRangeEndFilters;

        IndexCursorFactory(BaseQueryEngine<S> engine,
                           StorableIndex<S> index,
                           boolean reverseOrder,
                           boolean reverseRange,
                           Filter<S> exactFilter,
                           PropertyFilter<S>[] inclusiveRangeStartFilters,
                           PropertyFilter<S>[] exclusiveRangeStartFilters,
                           PropertyFilter<S>[] inclusiveRangeEndFilters,
                           PropertyFilter<S>[] exclusiveRangeEndFilters)
        {
            super(engine);
            mIndex = index;
            mExactFilter = exactFilter;
            mReverseOrder = reverseOrder;
            mReverseRange = reverseRange;
            mInclusiveRangeStartFilters = inclusiveRangeStartFilters;
            mExclusiveRangeStartFilters = exclusiveRangeStartFilters;
            mInclusiveRangeEndFilters = inclusiveRangeEndFilters;
            mExclusiveRangeEndFilters = exclusiveRangeEndFilters;
        }

        public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
            Object[] exactValues = null;
            Object rangeStartValue = null;
            Object rangeEndValue = null;
            BoundaryType rangeStartBoundary = BoundaryType.OPEN;
            BoundaryType rangeEndBoundary = BoundaryType.OPEN;

            if (values != null) {
                if (mExactFilter != null) {
                    exactValues = values.getValuesFor(mExactFilter);
                }

                // In determining the proper range values and boundary types,
                // the order in which this code runs is important. The exclusive
                // filters must be checked before the inclusive filters.

                for (PropertyFilter<S> p : mExclusiveRangeStartFilters) {
                    Object value = values.getValue(p);
                    if (rangeStartBoundary == BoundaryType.OPEN ||
                        compareWithNullHigh(value, rangeStartValue) > 0)
                    {
                        rangeStartValue = value;
                        rangeStartBoundary = BoundaryType.EXCLUSIVE;
                    }
                }

                for (PropertyFilter<S> p : mInclusiveRangeStartFilters) {
                    Object value = values.getValue(p);
                    if (rangeStartBoundary == BoundaryType.OPEN ||
                        compareWithNullHigh(value, rangeStartValue) > 0)
                    {
                        rangeStartValue = value;
                        rangeStartBoundary = BoundaryType.INCLUSIVE;
                    }
                }

                for (PropertyFilter<S> p : mExclusiveRangeEndFilters) {
                    Object value = values.getValue(p);
                    if (rangeEndBoundary == BoundaryType.OPEN ||
                        compareWithNullHigh(value, rangeEndValue) < 0)
                    {
                        rangeEndValue = value;
                        rangeEndBoundary = BoundaryType.EXCLUSIVE;
                    }
                }

                for (PropertyFilter<S> p : mInclusiveRangeEndFilters) {
                    Object value = values.getValue(p);
                    if (rangeEndBoundary == BoundaryType.OPEN ||
                        compareWithNullHigh(value, rangeEndValue) < 0)
                    {
                        rangeEndValue = value;
                        rangeEndBoundary = BoundaryType.INCLUSIVE;
                    }
                }
            }

            return mEngine.openCursor(mIndex, exactValues,
                                      rangeStartBoundary, rangeStartValue,
                                      rangeEndBoundary, rangeEndValue,
                                      mReverseRange,
                                      mReverseOrder);
        }

        public Filter<S> buildFilter(Filter<S> filter) {
            if (mExactFilter != null) {
                filter = filter == null ? mExactFilter : filter.and(mExactFilter);
            }
            for (PropertyFilter<S> p : mInclusiveRangeStartFilters) {
                filter = filter == null ? p : filter.and(p);
            }
            for (PropertyFilter<S> p : mExclusiveRangeStartFilters) {
                filter = filter == null ? p : filter.and(p);
            }
            for (PropertyFilter<S> p : mInclusiveRangeEndFilters) {
                filter = filter == null ? p : filter.and(p);
            }
            for (PropertyFilter<S> p : mExclusiveRangeEndFilters) {
                filter = filter == null ? p : filter.and(p);
            }
            return filter;
        }

        public Query<S> applyOrderBy(Query<S> query) throws FetchException {
            if (mIndex == null) {
                // Index is null if this is a full scan with no ordering specified.
                return query;
            }

            int count = mIndex.getPropertyCount();
            String[] orderBy = new String[count];

            for (int i=0; i<count; i++) {
                String propName = mIndex.getProperty(i).getName();
                Direction dir = mIndex.getPropertyDirection(i);
                if (mReverseOrder) {
                    dir = dir.reverse();
                }
                if (dir == Direction.ASCENDING) {
                    propName = "+".concat(propName);
                } else if (dir == Direction.DESCENDING) {
                    propName = "-".concat(propName);
                }
                orderBy[i] = propName;
            }

            return query.orderBy(orderBy);
        }

        public Storage<S> getActualStorage() {
            return mEngine.getStorageFor(mIndex);
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            if (mReverseOrder) {
                app.append("reverse ");
            }
            if (mIndex.isClustered()) {
                app.append("clustered ");
            }
            app.append("index scan: ");
            app.append(mEngine.getStorableInfo().getStorableType().getName());
            app.append('\n');
            indent(app, indentLevel);
            app.append("...index: ");
            mIndex.appendTo(app);
            app.append('\n');
            if (mExactFilter != null) {
                indent(app, indentLevel);
                app.append("...exact filter: ");
                mExactFilter.appendTo(app, values);
                app.append('\n');
            }
            if (mInclusiveRangeStartFilters.length > 0 || mExclusiveRangeStartFilters.length > 0 ||
                mInclusiveRangeEndFilters.length > 0 || mExclusiveRangeEndFilters.length > 0)
            {
                indent(app, indentLevel);
                app.append("...range filter: ");
                int count = 0;
                for (PropertyFilter<S> p : mExclusiveRangeStartFilters) {
                    if (count++ > 0) {
                        app.append(" & ");
                    }
                    p.appendTo(app, values);
                }
                for (PropertyFilter<S> p : mInclusiveRangeStartFilters) {
                    if (count++ > 0) {
                        app.append(" & ");
                    }
                    p.appendTo(app, values);
                }
                for (PropertyFilter<S> p : mExclusiveRangeEndFilters) {
                    if (count++ > 0) {
                        app.append(" & ");
                    }
                    p.appendTo(app, values);
                }
                for (PropertyFilter<S> p : mInclusiveRangeEndFilters) {
                    if (count++ > 0) {
                        app.append(" & ");
                    }
                    p.appendTo(app, values);
                }
                app.append('\n');
            }
            return true;
        }
    }

    private static class FullScanCursorFactory<S extends Storable> extends IndexCursorFactory<S> {
        FullScanCursorFactory(BaseQueryEngine<S> engine, StorableIndex<S> index) {
            super(engine, index, false, false,
                  null, NO_FILTERS, NO_FILTERS, NO_FILTERS, NO_FILTERS);
        }

        @Override
        public Filter<S> buildFilter(Filter<S> filter) {
            // Full scan doesn't filter anything.
            return filter;
        }

        @Override
        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            app.append("full scan: ");
            app.append(mEngine.getStorableInfo().getStorableType().getName());
            app.append('\n');
            return true;
        }
    }

    private static class KeyCursorFactory<S extends Storable> extends AbstractCursorFactory<S> {
        private final StorableIndex<S> mIndex;
        private final Filter<S> mExactFilter;

        KeyCursorFactory(BaseQueryEngine<S> engine,
                         StorableIndex<S> index, Filter<S> exactFilter) {
            super(engine);
            mIndex = index;
            mExactFilter = exactFilter;
        }

        public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
            return mEngine.openKeyCursor(mIndex, values.getValuesFor(mExactFilter));
        }

        public Filter<S> buildFilter(Filter<S> filter) {
            if (mExactFilter != null) {
                filter = filter == null ? mExactFilter : filter.and(mExactFilter);
            }
            return filter;
        }

        public Query<S> applyOrderBy(Query<S> query) {
            return query;
        }

        public Storage<S> getActualStorage() {
            return mEngine.getStorageFor(mIndex);
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            app.append("index key: ");
            app.append(mEngine.getStorableInfo().getStorableType().getName());
            app.append('\n');
            indent(app, indentLevel);
            app.append("...index: ");
            mIndex.appendTo(app);
            app.append('\n');
            indent(app, indentLevel);
            app.append("...exact filter: ");
            mExactFilter.appendTo(app, values);
            app.append('\n');
            return true;
        }
    }

    private static class FilteredCursorFactory<S extends Storable>
        extends AbstractCursorFactory<S>
    {
        private final CursorFactory<S> mFactory;
        private final Filter<S> mFilter;

        FilteredCursorFactory(BaseQueryEngine<S> engine,
                              CursorFactory<S> factory, Filter<S> filter) {
            super(engine);
            mFactory = factory;
            mFilter = filter;
        }

        public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
            return FilteredCursor.applyFilter(mFilter,
                                              values,
                                              mFactory.openCursor(values));
        }

        public Filter<S> buildFilter(Filter<S> filter) {
            filter = mFactory.buildFilter(filter);
            filter = filter == null ? mFilter : filter.and(mFilter);
            return filter;
        }

        public Query<S> applyOrderBy(Query<S> query) throws FetchException {
            return mFactory.applyOrderBy(query);
        }

        public Storage<S> getActualStorage() {
            return mFactory.getActualStorage();
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            app.append("filter: ");
            mFilter.appendTo(app, values);
            app.append('\n');
            mFactory.printPlan(app, indentLevel + 2, values);
            return true;
        }
    }

    private static class SortedCursorFactory<S extends Storable> extends AbstractCursorFactory<S> {
        private final CursorFactory<S> mFactory;
        private final OrderedProperty<S>[] mHandledOrderings;
        private final OrderedProperty<S>[] mRemainderOrderings;

        private final Comparator<S> mHandledComparator;
        private final Comparator<S> mFinisherComparator;

        SortedCursorFactory(BaseQueryEngine<S> engine,
                            CursorFactory<S> factory,
                            OrderedProperty<S>[] handledOrderings,
                            OrderedProperty<S>[] remainderOrderings) {
            super(engine);
            mFactory = factory;
            if (handledOrderings != null && handledOrderings.length == 0) {
                handledOrderings = null;
            }
            if (remainderOrderings != null && remainderOrderings.length == 0) {
                remainderOrderings = null;
            }
            if (handledOrderings == null && remainderOrderings == null) {
                throw new IllegalArgumentException();
            }
            mHandledOrderings = handledOrderings;
            mRemainderOrderings = remainderOrderings;

            mHandledComparator = engine.makeComparator(handledOrderings);
            mFinisherComparator = engine.makeComparator(remainderOrderings);
        }

        public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
            Cursor<S> cursor = mFactory.openCursor(values);

            SortBuffer<S> buffer = new MergeSortBuffer<S>
                (getActualStorage(), mEngine.mMergeSortTempDir);

            return new SortedCursor<S>(cursor, buffer, mHandledComparator, mFinisherComparator);
        }

        @Override
        public long count(FilterValues<S> values) throws FetchException {
            return mFactory.count(values);
        }


        public Filter<S> buildFilter(Filter<S> filter) {
            return mFactory.buildFilter(filter);
        }

        public Query<S> applyOrderBy(Query<S> query) throws FetchException {
            int handledLength = mHandledOrderings == null ? 0 : mHandledOrderings.length;
            int remainderLength = mRemainderOrderings == null ? 0 : mRemainderOrderings.length;
            String[] orderBy = new String[handledLength + remainderLength];
            int pos = 0;
            for (int i=0; i<handledLength; i++) {
                orderBy[pos++] = mHandledOrderings[i].toString();
            }
            for (int i=0; i<remainderLength; i++) {
                orderBy[pos++] = mRemainderOrderings[i].toString();
            }
            return query.orderBy(orderBy);
        }

        public Storage<S> getActualStorage() {
            return mFactory.getActualStorage();
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            if (mHandledOrderings == null) {
                app.append("full sort: ");
            } else {
                app.append("finish sort: ");
            }
            app.append(Arrays.toString(mRemainderOrderings));
            app.append('\n');
            mFactory.printPlan(app, indentLevel + 2, values);
            return true;
        }
    }

    private static class UnionedCursorFactory<S extends Storable>
        extends AbstractCursorFactory<S>
    {
        static <S extends Storable> CursorFactory<S> createUnion
                          (BaseQueryEngine<S> engine,
                           List<CursorFactory<S>> factories,
                           OrderedProperty<S>[] totalOrderings)
        {
            Comparator<S> orderComparator = engine.makeComparator(totalOrderings);
            return createUnion(engine, factories, totalOrderings, orderComparator);
        }

        @SuppressWarnings("unchecked")
        static <S extends Storable> CursorFactory<S> createUnion
                          (BaseQueryEngine<S> engine,
                           List<CursorFactory<S>> factories,
                           OrderedProperty<S>[] totalOrderings,
                           Comparator<S> orderComparator)
        {
            if (factories.size() > 1) {
                CursorFactory<S>[] array = new CursorFactory[factories.size()];
                factories.toArray(array);
                return new UnionedCursorFactory<S>(engine, array, totalOrderings, orderComparator);
            }
            return factories.get(0);
        }

        private final CursorFactory<S>[] mFactories;
        private final OrderedProperty<S>[] mTotalOrderings;
        private final Comparator<S> mOrderComparator;

        private UnionedCursorFactory(BaseQueryEngine<S> engine,
                                     CursorFactory<S>[] factories,
                                     OrderedProperty<S>[] totalOrderings,
                                     Comparator<S> orderComparator) {
            super(engine);
            mFactories = factories;
            mTotalOrderings = totalOrderings;
            mOrderComparator = orderComparator;
        }

        public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
            Cursor<S> cursor = null;
            for (CursorFactory<S> factory : mFactories) {
                Cursor<S> subCursor = factory.openCursor(values);
                cursor = (cursor == null) ? subCursor
                    : new UnionCursor<S>(cursor, subCursor, mOrderComparator);
            }
            return cursor;
        }

        public Filter<S> buildFilter(Filter<S> filter) {
            for (CursorFactory<S> factory : mFactories) {
                Filter<S> subFilter = factory.buildFilter(null);
                filter = filter == null ? subFilter : filter.or(subFilter);
            }
            return filter;
        }

        public Query<S> applyOrderBy(Query<S> query) throws FetchException {
            if (mTotalOrderings == null || mTotalOrderings.length == 0) {
                return query;
            }

            String[] orderBy = new String[mTotalOrderings.length];
            for (int i=mTotalOrderings.length; --i>=0; ) {
                orderBy[i] = mTotalOrderings[i].toString();
            }

            return query.orderBy(orderBy);
        }

        public Storage<S> getActualStorage() {
            Storage<S> storage = null;
            for (CursorFactory<S> factory : mFactories) {
                Storage<S> subStorage = factory.getActualStorage();
                if (storage == null) {
                    storage = subStorage;
                } else if (storage != subStorage) {
                    return null;
                }
            }
            return storage;
        }

        @Override
        public CursorFactory<S> getActualFactory() throws FetchException {
            Storage<S> requiredStorage = getActualStorage();
            if (requiredStorage == mEngine.getStorage()) {
                // Alternate not really needed.
                return this;
            }
            if (requiredStorage != null) {
                // All components require same external storage, so let
                // external storage do the union.
                return new QueryCursorFactory<S>(this, requiredStorage);
            }

            // Group factories by required storage instance, and then create a
            // union of unions.

            Comparator<CursorFactory<S>> comparator = new Comparator<CursorFactory<S>>() {
                public int compare(CursorFactory<S> a, CursorFactory<S> b) {
                    Storage<S> aStorage = a.getActualStorage();
                    Storage<S> bStorage = b.getActualStorage();
                    if (aStorage == bStorage) {
                        return 0;
                    }
                    Storage<S> engineStorage = mEngine.getStorage();
                    if (aStorage == engineStorage) {
                        return -1;
                    } else if (bStorage == engineStorage) {
                        return 1;
                    }
                    int aHash = System.identityHashCode(a);
                    int bHash = System.identityHashCode(b);
                    if (aHash < bHash) {
                        return -1;
                    } else if (aHash > bHash) {
                        return 1;
                    }
                    return 0;
                }
            };

            Arrays.sort(mFactories, comparator);

            List<CursorFactory<S>> masterList = new ArrayList<CursorFactory<S>>();

            List<CursorFactory<S>> subList = new ArrayList<CursorFactory<S>>();
            Storage<S> group = null;
            for (CursorFactory<S> factory : mFactories) {
                Storage<S> storage = factory.getActualStorage();
                if (group != storage) {
                    if (subList.size() > 0) {
                        masterList.add(createUnion
                                       (mEngine, subList, mTotalOrderings, mOrderComparator));
                        subList.clear();
                    }
                    group = storage;
                }
                CursorFactory<S> subFactory = new QueryCursorFactory<S>(factory, storage);
                subList.add(subFactory);
            }
            if (subList.size() > 0) {
                masterList.add(createUnion(mEngine, subList, mTotalOrderings, mOrderComparator));
                subList.clear();
            }

            return createUnion(mEngine, masterList, mTotalOrderings, mOrderComparator);
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            app.append("union");
            app.append('\n');
            for (CursorFactory<S> factory : mFactories) {
                factory.printPlan(app, indentLevel + 2, values);
            }
            return true;
        }
    }

    /**
     * CursorFactory implementation that reconstructs and calls an external
     * Query.
     */
    private static class QueryCursorFactory<S extends Storable> implements CursorFactory<S> {
        private final CursorFactory<S> mFactory;
        private final Storage<S> mStorage;
        private final Query<S> mQuery;

        /**
         * @param factory factory to derive this factory from
         * @param storage actual storage to query against
         */
        QueryCursorFactory(CursorFactory<S> factory, Storage<S> storage) throws FetchException {
            mFactory = factory;
            mStorage = storage;

            Filter<S> filter = factory.buildFilter(null);

            Query<S> query;
            if (filter == null) {
                query = storage.query();
            } else {
                query = storage.query(filter);
            }

            mQuery = factory.applyOrderBy(query);
        }

        public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
            return applyFilterValues(values).fetch();
        }

        public long count(FilterValues<S> values) throws FetchException {
            return applyFilterValues(values).count();
        }

        public Filter<S> buildFilter(Filter<S> filter) {
            return mFactory.buildFilter(filter);
        }

        public Query<S> applyOrderBy(Query<S> query) throws FetchException {
            return mFactory.applyOrderBy(query);
        }

        public Storage<S> getActualStorage() {
            return mStorage;
        }

        public CursorFactory<S> getActualFactory() {
            return this;
        }

        public boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            return applyFilterValues(values).printNative(app, indentLevel);
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            Query<S> query;
            try {
                query = applyFilterValues(values);
            } catch (IllegalStateException e) {
                query = mQuery;
            }
            return query.printPlan(app, indentLevel);
        }

        private Query<S> applyFilterValues(FilterValues<S> values) {
            // FIXME: figure out how to transfer values directly to query.

            Query<S> query = mQuery;
            Filter<S> filter = query.getFilter();
            // FIXME: this code can get confused if filter has constants.
            if (values != null && filter != null && query.getBlankParameterCount() != 0) {
                query = query.withValues(values.getValuesFor(filter));
            }
            return query;
        }
    }

    private static class IndexAnalysis<S extends Storable> extends Visitor<S, Object, Object>
        implements Comparable<IndexAnalysis<?>>
    {
        private final StorableIndex<S> mPrimaryKeyIndex;
        private final StorableIndexSet<S> mIndexSet;
        private final OrderedProperty<S>[] mOrderings;

        private List<IndexSelector.IndexFitness<S>> mResults;

        IndexAnalysis(StorableIndex<S> primaryKeyIndex,
                      StorableIndexSet<S> indexSet,
                      OrderedProperty<S>[] orderings)
        {
            mPrimaryKeyIndex = primaryKeyIndex;
            mIndexSet = indexSet;
            mOrderings = orderings;
            mResults = new ArrayList<IndexSelector.IndexFitness<S>>();
        }

        public Object visit(OrFilter<S> filter, Object param) {
            Filter<S> left = filter.getLeftFilter();
            if (!(left instanceof OrFilter)) {
                selectIndex(left);
            } else {
                left.accept(this, param);
            }
            Filter<S> right = filter.getRightFilter();
            if (!(right instanceof OrFilter)) {
                selectIndex(right);
            } else {
                right.accept(this, param);
            }
            return null;
        }

        // This method should only be called if root filter has no 'or' operators.
        public Object visit(AndFilter<S> filter, Object param) {
            selectIndex(filter);
            return null;
        }

        // This method should only be called if root filter has no logical operators.
        public Object visit(PropertyFilter<S> filter, Object param) {
            selectIndex(filter);
            return null;
        }

        /**
         * Compares this analysis to another which belongs to a different
         * Storable type. Filters that reference a joined property may be best
         * served by an index defined in the joined type, and this method aids
         * in that selection.
         *
         * @return &lt;0 if these results are better, 0 if equal, or &gt;0 if other is better
         */
        public int compareTo(IndexAnalysis<?> otherAnalysis) {
            if (noBestIndex()) {
                if (otherAnalysis.noBestIndex()) {
                    return 0;
                }
                return 1;
            } else if (otherAnalysis.noBestIndex()) {
                return -1;
            } else {
                return IndexSelector.listCompare(mResults, otherAnalysis.mResults);
            }
        }

        /**
         * If more than one result returned, then a union must be performed.
         * This is because there exist 'or' operators in the full filter.
         */
        List<IndexSelector.IndexFitness<S>> getResults() {
            return mResults;
        }

        /**
         * If more than one result, then a union must be performed. Attempt to
         * reduce the result list by performing unions at the index layer. This
         * reduces the number of cursors that need to be opened for a query,
         * eliminating duplicate work.
         */
        void reduceResults() {
            if (mResults.size() <= 1) {
                return;
            }

            List<IndexSelector.IndexFitness<S>> reduced =
                new ArrayList<IndexSelector.IndexFitness<S>>(mResults.size());

            gather:
            for (int i=0; i<mResults.size(); i++) {
                IndexSelector.IndexFitness fitness = mResults.get(i);
                for (int j=0; j<reduced.size(); j++) {
                    IndexSelector.IndexFitness unioned = fitness.union(reduced.get(j));
                    if (unioned != null) {
                        reduced.set(j, unioned);
                        continue gather;
                    }
                }
                // Couldn't union with another use of index, so add it to reduced list.
                reduced.add(fitness);
            }

            mResults = reduced;
        }

        boolean noBestIndex() {
            // Must be an index for each property filter. No point in unioning
            // an index scan with a full scan. Just do a full scan.
            for (IndexSelector.IndexFitness<S> result : mResults) {
                if (result.isUseless()) {
                    return true;
                }
            }
            return false;
        }

        private void selectIndex(Filter<S> filter) {
            IndexSelector<S> selector = new IndexSelector<S>(filter, mOrderings);

            StorableIndex<S> best = mPrimaryKeyIndex;
            if (mIndexSet != null) {
                for (StorableIndex<S> candidate : mIndexSet) {
                    int result = selector.compare(best, candidate);
                    if (result > 0) {
                        best = candidate;
                    }
                }
            }

            mResults.add(selector.examine(best));
        }
    }
}
