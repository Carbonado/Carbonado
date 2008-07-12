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

package com.amazon.carbonado.qe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Analyzes a simple query specification and determines which index is best
 * suited for its execution. Query filters passed to this analyzer cannot
 * contain any 'or' operations.
 *
 * <p>IndexedQueryAnalyzer is sharable and thread-safe. An instance for a
 * particular Storable type can be cached, avoiding repeated construction
 * cost. In addition, the analyzer caches learned foreign indexes.
 *
 * @author Brian S O'Neill
 * @see UnionQueryAnalyzer
 */
public class IndexedQueryAnalyzer<S extends Storable> {
    final Class<S> mType;
    final RepositoryAccess mRepoAccess;

    // Growable cache which maps join properties to lists of usable foreign indexes.
    private Map<ChainedProperty<S>, ForeignIndexes<S>> mForeignIndexCache;

    /**
     * @param type type of storable being queried
     * @param access repository access for examing available indexes
     * @throws IllegalArgumentException if type or indexProvider is null
     */
    public IndexedQueryAnalyzer(Class<S> type, RepositoryAccess access) {
        if (type == null || access == null) {
            throw new IllegalArgumentException();
        }
        mType = type;
        mRepoAccess = access;
    }

    public Class<S> getStorableType() {
        return mType;
    }

    /**
     * @param filter optional filter which which must be {@link Filter#isBound
     * bound} and cannot contain any logical 'or' operations.
     * @param ordering optional properties which define desired ordering
     * @param hints optional query hints
     * @throws IllegalArgumentException if filter is not supported
     */
    public Result analyze(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws SupportException, RepositoryException
    {
        if (filter != null && !filter.isBound()) {
            throw new IllegalArgumentException("Filter must be bound");
        }

        // First find best local index.
        CompositeScore<S> bestLocalScore = null;
        StorableIndex<S> bestLocalIndex = null;

        final Comparator<CompositeScore<?>> fullComparator = CompositeScore.fullComparator(hints);

        Collection<StorableIndex<S>> localIndexes = indexesFor(getStorableType());
        if (localIndexes != null) {
            for (StorableIndex<S> index : localIndexes) {
                CompositeScore<S> candidateScore =
                    CompositeScore.evaluate(index, filter, ordering);

                if (bestLocalScore == null
                    || fullComparator.compare(candidateScore, bestLocalScore) < 0)
                {
                    bestLocalScore = candidateScore;
                    bestLocalIndex = index;
                }
            }
        }

        // Now try to find best foreign index.

        if (bestLocalScore != null && bestLocalScore.getFilteringScore().isKeyMatch()) {
            // Don't bother checking foreign indexes. The local one is perfect.
            return new Result(filter, bestLocalScore, bestLocalIndex, null, null, hints);
        }

        CompositeScore<?> bestForeignScore = null;
        StorableIndex<?> bestForeignIndex = null;
        ChainedProperty<S> bestForeignProperty = null;

        for (PropertyFilter<S> propFilter : PropertyFilterList.get(filter)) {
            ChainedProperty<S> chainedProp = propFilter.getChainedProperty();

            if (chainedProp.getChainCount() == 0) {
                // Cannot possibly be a join, so move on.
                continue;
            }

            ForeignIndexes<S> foreignIndexes = getForeignIndexes(chainedProp);
            if (foreignIndexes == null) {
                continue;
            }

            for (StorableIndex<?> index : foreignIndexes.mIndexes) {
                CompositeScore<S> candidateScore = CompositeScore.evaluate
                    (foreignIndexes.getVirtualIndex(index),
                     index.isUnique(),
                     index.isClustered(),
                     filter,
                     ordering);

                if (bestForeignScore == null
                    || fullComparator.compare(candidateScore, bestForeignScore) < 0)
                {
                    bestForeignScore = candidateScore;
                    bestForeignIndex = index;
                    bestForeignProperty = foreignIndexes.mProperty;
                }
            }
        }

        // Check if foreign index is better than local index.

        if (bestLocalScore != null && bestForeignScore != null) {
            // When comparing local index to foreign index, use a slightly less
            // discriminating comparator, to prevent foreign indexes from
            // looking too good.

            Comparator<CompositeScore<?>> comp = CompositeScore.localForeignComparator();

            if (comp.compare(bestForeignScore, bestLocalScore) < 0) {
                // Foreign is better.
                bestLocalScore = null;
            } else {
                // Local is better.
                bestForeignScore = null;
            }
        }

        CompositeScore bestScore;

        if (bestLocalScore != null) {
            bestScore = bestLocalScore;
            bestForeignIndex = null;
            bestForeignProperty = null;
        } else {
            bestScore = bestForeignScore;
            bestLocalIndex = null;
        }

        return new Result
            (filter, bestScore, bestLocalIndex, bestForeignIndex, bestForeignProperty, hints);
    }

    /**
     * @return null if no foreign indexes for property
     */
    private synchronized ForeignIndexes<S> getForeignIndexes(ChainedProperty<S> chainedProp)
        throws SupportException, RepositoryException
    {
        // Remove the last property as it is expected to be a simple storable
        // property instead of a joined Storable.
        chainedProp = chainedProp.trim();

        ForeignIndexes<S> foreignIndexes = null;
        if (mForeignIndexCache != null) {
            foreignIndexes = mForeignIndexCache.get(chainedProp);
            if (foreignIndexes != null || mForeignIndexCache.containsKey(chainedProp)) {
                return foreignIndexes;
            }
        }

        // Check if property chain is properly joined and indexed along the way.
        evaluate: {
            if (!isProperJoin(chainedProp.getPrimeProperty())) {
                break evaluate;
            }
            if (chainedProp.isOuterJoin(0)) {
                // Outer joins cannot be optimized via foreign indexes.
                break evaluate;
            }

            int count = chainedProp.getChainCount();
            for (int i=0; i<count; i++) {
                if (!isProperJoin(chainedProp.getChainedProperty(i))) {
                    break evaluate;
                }
                if (chainedProp.isOuterJoin(i + 1)) {
                    // Outer joins cannot be optimized via foreign indexes.
                    break evaluate;
                }
            }

            // All foreign indexes are available for use.
            Class foreignType = chainedProp.getLastProperty().getType();
            Collection<StorableIndex<?>> indexes = indexesFor(foreignType);

            foreignIndexes = new ForeignIndexes<S>(chainedProp, indexes);
        }

        if (mForeignIndexCache == null) {
            mForeignIndexCache = Collections.synchronizedMap
                (new HashMap<ChainedProperty<S>, ForeignIndexes<S>>());
        }

        mForeignIndexCache.put(chainedProp, foreignIndexes);

        return foreignIndexes;
    }

    /**
     * Checks if the property is a join and its internal properties are fully
     * indexed.
     */
    private boolean isProperJoin(StorableProperty<?> property)
        throws SupportException, RepositoryException
    {
        if (!property.isJoin() || property.isQuery()) {
            return false;
        }

        // Make up a filter over the join's internal properties and then search
        // for an index that filters with no remainder.
        Filter<?> filter = Filter.getOpenFilter(property.getEnclosingType());
        int count = property.getJoinElementCount();
        for (int i=0; i<count; i++) {
            filter = filter.and(property.getInternalJoinElement(i).getName(), RelOp.EQ);
        }

        // Java generics are letting me down. I cannot use proper specification
        // because compiler gets confused with all the wildcards.
        Collection indexes = indexesFor(filter.getStorableType());

        if (indexes != null) {
            for (Object index : indexes) {
                FilteringScore score = FilteringScore.evaluate((StorableIndex) index, filter);
                if (score.getRemainderCount() == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    private <T extends Storable> Collection<StorableIndex<T>> indexesFor(Class<T> type)
        throws SupportException, RepositoryException
    {
        return mRepoAccess.storageAccessFor(type).getAllIndexes();
    }

    public class Result {
        private final Filter<S> mFilter;

        private final CompositeScore<S> mScore;
        private final StorableIndex<S> mLocalIndex;
        private final StorableIndex<?> mForeignIndex;
        private final ChainedProperty<S> mForeignProperty;
        private final QueryHints mHints;

        Result(Filter<S> filter,
               CompositeScore<S> score,
               StorableIndex<S> localIndex,
               StorableIndex<?> foreignIndex,
               ChainedProperty<S> foreignProperty,
               QueryHints hints)
        {
            mFilter = filter;
            mScore = score;
            mLocalIndex = localIndex;
            mForeignIndex = foreignIndex;
            mForeignProperty = foreignProperty;
            mHints = hints;
        }

        /**
         * Returns true if the selected index does anything at all to filter
         * results or to order them. If not, a filtered and sorted full scan
         * makes more sense.
         */
        public boolean handlesAnything() {
            return mScore.getFilteringScore().hasAnyMatches() == true
                || mScore.getOrderingScore().getHandledCount() > 0;
        }

        /**
         * Returns combined handled and remainder filter for this result.
         */
        public Filter<S> getFilter() {
            return mFilter;
        }

        /**
         * Returns combined handled and remainder orderings for this result.
         */
        public OrderingList<S> getOrdering() {
            return mScore.getOrderingScore().getHandledOrdering().concat(getRemainderOrdering());
        }

        /**
         * Returns the score on how well the selected index performs the
         * desired filtering and ordering.
         */
        public CompositeScore<S> getCompositeScore() {
            return mScore;
        }

        /**
         * Remainder filter which overrides that in composite score.
         */
        public Filter<S> getRemainderFilter() {
            return mScore.getFilteringScore().getRemainderFilter();
        }

        /**
         * Remainder orderings which override that in composite score.
         */
        public OrderingList<S> getRemainderOrdering() {
            return mScore.getOrderingScore().getRemainderOrdering();
        }

        /**
         * Returns the local index that was selected, or null if a foreign
         * index was selected.
         */
        public StorableIndex<S> getLocalIndex() {
            return mLocalIndex;
        }

        /**
         * Returns the foreign index that was selected, or null if a local
         * index was selected. If a foreign index has been selected, then a
         * {@link JoinedQueryExecutor} is needed.
         */
        public StorableIndex<?> getForeignIndex() {
            return mForeignIndex;
        }

        /**
         * Returns the simple or chained property that maps to the selected
         * foreign index. Returns null if foreign index was not selected. This
         * property corresponds to the "targetToSourceProperty" of {@link
         * JoinedQueryExecutor}.
         */
        public ChainedProperty<S> getForeignProperty() {
            return mForeignProperty;
        }

        /**
         * Returns true if local or foreign index is clustered. Scans of
         * clustered indexes are generally faster.
         */
        public boolean isIndexClustered() {
            return (mLocalIndex != null && mLocalIndex.isClustered())
                || (mForeignIndex != null && mForeignIndex.isClustered());
        }

        /**
         * Returns true if the given result uses the same index as this, and in
         * the same way. The only allowed differences are in the remainder
         * filter and orderings.
         */
        public boolean canMergeRemainder(Result other) {
            if (this == other || (!handlesAnything() && !other.handlesAnything())) {
                return true;
            }

            if (equals(getLocalIndex(), other.getLocalIndex())
                && equals(getForeignIndex(), other.getForeignIndex())
                && equals(getForeignProperty(), other.getForeignProperty()))
            {
                return getCompositeScore().canMergeRemainder(other.getCompositeScore());
            }

            return false;
        }

        /**
         * Merges the remainder filter and orderings of this result with the
         * one given, returning a new result. Call canMergeRemainder first to
         * verify if the merge makes any sense.
         */
        public Result mergeRemainder(Result other) {
            if (this == other) {
                return this;
            }

            // Assuming canMergeRemainder returned true, each handled filter
            // and the combined filter are all identical. This is just a safeguard.
            Filter<S> handledFilter =
                orFilters(getCompositeScore().getFilteringScore().getHandledFilter(),
                          other.getCompositeScore().getFilteringScore().getHandledFilter());

            Filter<S> remainderFilter =
                orFilters(getRemainderFilter(), other.getRemainderFilter());

            OrderingList<S> remainderOrdering =
                getRemainderOrdering().concat(other.getRemainderOrdering()).reduce();

            Filter<S> filter = andFilters(handledFilter, remainderFilter);

            CompositeScore<S> score = mScore
                .withRemainderFilter(remainderFilter)
                .withRemainderOrdering(remainderOrdering);

            return new Result(filter, score, mLocalIndex, mForeignIndex, mForeignProperty, mHints);
        }

        /**
         * Merges the remainder filter of this result with the given filter,
         * returning a new result. If handlesAnything return true, then it
         * doesn't usually make sense to call this method.
         */
        public Result mergeRemainderFilter(Filter<S> filter) {
            return withRemainderFilter(orFilters(getRemainderFilter(), filter));
        }

        private Filter<S> andFilters(Filter<S> a, Filter<S> b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return a.and(b).reduce();
        }

        private Filter<S> orFilters(Filter<S> a, Filter<S> b) {
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return a.or(b).reduce();
        }

        /**
         * Returns a new result with the remainder filter replaced.
         */
        public Result withRemainderFilter(Filter<S> remainderFilter) {
            Filter<S> handledFilter = getCompositeScore().getFilteringScore().getHandledFilter();

            Filter<S> filter = andFilters(handledFilter, remainderFilter);

            CompositeScore<S> score = mScore.withRemainderFilter(remainderFilter);

            return new Result(filter, score, mLocalIndex, mForeignIndex, mForeignProperty, mHints);
        }

        /**
         * Returns a new result with the remainder ordering replaced.
         */
        public Result withRemainderOrdering(OrderingList<S> remainderOrdering) {
            CompositeScore<S> score = mScore.withRemainderOrdering(remainderOrdering);
            return new Result(mFilter, score, mLocalIndex,
                              mForeignIndex, mForeignProperty, mHints);
        }

        /**
         * Creates a QueryExecutor based on this result.
         */
        public QueryExecutor<S> createExecutor()
            throws SupportException, FetchException, RepositoryException
        {
            StorableIndex<S> localIndex = getLocalIndex();
            StorageAccess<S> localAccess = mRepoAccess.storageAccessFor(getStorableType());

            if (localIndex != null) {
                Storage<S> delegate = localAccess.storageDelegate(localIndex);
                if (delegate != null) {
                    return new DelegatedQueryExecutor<S>(delegate, getFilter(), getOrdering());
                }
            }

            Filter<S> remainderFilter = getRemainderFilter();

            QueryExecutor<S> executor;
            if (!handlesAnything()) {
                executor = new FullScanQueryExecutor<S>(localAccess);
            } else if (localIndex == null) {
                // Use foreign executor.
                return JoinedQueryExecutor.build
                    (mRepoAccess, getForeignProperty(), getFilter(), getOrdering(), mHints);
            } else {
                CompositeScore<S> score = getCompositeScore();
                FilteringScore<S> fScore = score.getFilteringScore();
                if (fScore.isKeyMatch()) {
                    executor = new KeyQueryExecutor<S>(localAccess, localIndex, fScore);
                } else {
                    IndexedQueryExecutor ixExecutor =
                        new IndexedQueryExecutor<S>(localAccess, localIndex, score);
                    executor = ixExecutor;
                    if (ixExecutor.getCoveringFilter() != null) {
                        remainderFilter = fScore.getCoveringRemainderFilter();
                    }
                }
            }

            if (remainderFilter != null) {
                executor = new FilteredQueryExecutor<S>(executor, remainderFilter);
            }

            OrderingList<S> remainderOrdering = getRemainderOrdering();
            if (remainderOrdering.size() > 0) {
                executor = new SortedQueryExecutor<S>
                    (localAccess,
                     executor,
                     getCompositeScore().getOrderingScore().getHandledOrdering(),
                     remainderOrdering);
            }

            return executor;
        }

        @Override
        public String toString() {
            return "IndexedQueryAnalyzer.Result {score="
                + getCompositeScore() + ", localIndex="
                + getLocalIndex() + ", foreignIndex="
                + getForeignIndex() + ", foreignProperty="
                + getForeignProperty() + ", remainderFilter="
                + getRemainderFilter() + ", remainderOrdering="
                + getRemainderOrdering() + '}';
        }

        private boolean equals(Object a, Object b) {
            return a == null ? (b == null) : (a.equals(b));
        }
    }

    private static class ForeignIndexes<S extends Storable> {
        final ChainedProperty<S> mProperty;

        final List<StorableIndex<?>> mIndexes;

        // Cache of virtual indexes.
        private final Map<StorableIndex<?>, OrderedProperty<S>[]> mVirtualIndexMap;

        /**
         * @param property type of property must be a joined Storable
         */
        ForeignIndexes(ChainedProperty<S> property, Collection<StorableIndex<?>> indexes) {
            mProperty = property;
            if (indexes == null || indexes.size() == 0) {
                mIndexes = Collections.emptyList();
            } else {
                mIndexes = new ArrayList<StorableIndex<?>>(indexes);
            }
            mVirtualIndexMap = new HashMap<StorableIndex<?>, OrderedProperty<S>[]>();
        }

        /**
         * Prepends local chained property with names of index elements,
         * producing a virtual index on local storable. This allows
         * CompositeScore to evaluate it.
         */
        synchronized OrderedProperty<S>[] getVirtualIndex(StorableIndex<?> index) {
            OrderedProperty<S>[] virtualProps = mVirtualIndexMap.get(index);
            if (virtualProps != null) {
                return virtualProps;
            }

            OrderedProperty<?>[] realProps = index.getOrderedProperties();
            virtualProps = new OrderedProperty[realProps.length];

            for (int i=realProps.length; --i>=0; ) {
                OrderedProperty<?> realProp = realProps[i];
                ChainedProperty<S> virtualChained =
                    mProperty.append(realProp.getChainedProperty());
                virtualProps[i] = OrderedProperty.get(virtualChained, realProp.getDirection());
            }

            mVirtualIndexMap.put(index, virtualProps);

            return virtualProps;
        }
    }
}
