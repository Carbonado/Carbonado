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

import com.amazon.carbonado.Storable;

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
    private final Class<S> mType;
    private final IndexProvider mIndexProvider;

    // Growable cache which maps join properties to lists of usable foreign indexes.
    private Map<ChainedProperty<S>, ForeignIndexes<S>> mForeignIndexCache;

    /**
     * @param type type of storable being queried
     * @param indexProvider
     * @throws IllegalArgumentException if type or indexProvider is null
     */
    public IndexedQueryAnalyzer(Class<S> type, IndexProvider indexProvider) {
        if (type == null || indexProvider == null) {
            throw new IllegalArgumentException();
        }
        mType = type;
        mIndexProvider = indexProvider;
    }

    public Class<S> getStorableType() {
        return mType;
    }

    /**
     * @param filter optional filter which which must be {@link Filter#isBound
     * bound} and cannot contain any logical 'or' operations.
     * @param orderings optional properties which define desired ordering
     * @throws IllegalArgumentException if filter is not supported
     */
    public Result analyze(Filter<S> filter, List<OrderedProperty<S>> orderings) {
        if (!filter.isBound()) {
            // Strictly speaking, this is not required, but it detects the
            // mistake of not properly calling initialFilterValues.
            throw new IllegalArgumentException("Filter must be bound");
        }

        final Comparator<CompositeScore<?>> comparator = CompositeScore.fullComparator();

        // First find best local index.
        CompositeScore<S> bestScore = null;
        StorableIndex<S> bestLocalIndex = null;

        Collection<StorableIndex<S>> localIndexes = mIndexProvider.indexesFor(mType);
        if (localIndexes != null) {
            for (StorableIndex<S> index : localIndexes) {
                CompositeScore<S> candidateScore =
                    CompositeScore.evaluate(index, filter, orderings);

                if (bestScore == null || comparator.compare(candidateScore, bestScore) < 0) {
                    bestScore = candidateScore;
                    bestLocalIndex = index;
                }
            }
        }

        // Now try to find better foreign index.
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
                     orderings);

                if (bestScore == null || comparator.compare(candidateScore, bestScore) < 0) {
                    bestScore = candidateScore;
                    bestLocalIndex = null;
                    bestForeignIndex = index;
                    bestForeignProperty = foreignIndexes.mProperty;
                }
            }
        }

        return new Result(filter,
                          bestScore,
                          bestLocalIndex,
                          bestForeignIndex,
                          bestForeignProperty);
    }

    /**
     * @return null if no foreign indexes for property
     */
    private synchronized ForeignIndexes<S> getForeignIndexes(ChainedProperty<S> chainedProp) {
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

            int count = chainedProp.getChainCount();
            for (int i=0; i<count; i++) {
                if (!isProperJoin(chainedProp.getChainedProperty(i))) {
                    break evaluate;
                }
            }

            // All foreign indexes are available for use.
            Class foreignType = chainedProp.getLastProperty().getType();
            Collection<StorableIndex<?>> indexes = mIndexProvider.indexesFor(foreignType);

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
    private boolean isProperJoin(StorableProperty<?> property) {
        if (!property.isJoin() || property.isQuery()) {
            return false;
        }

        // Make up a filter over the join's internal properties and then search
        // for an index that filters with no remainder.
        Filter<?> filter = Filter.getOpenFilter((Class<? extends Storable>) property.getType());
        int count = property.getJoinElementCount();
        for (int i=0; i<count; i++) {
            filter = filter.and(property.getInternalJoinElement(i).getName(), RelOp.EQ);
        }

        // Java generics are letting me down. I cannot use proper specification
        // because compiler gets confused with all the wildcards.
        Collection indexes = mIndexProvider.indexesFor(filter.getStorableType());

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

    private <F extends Storable> boolean simpleAnalyze(Filter<F> filter) {
        Collection<StorableIndex<F>> indexes = mIndexProvider.indexesFor(filter.getStorableType());

        if (indexes != null) {
            for (StorableIndex<F> index : indexes) {
                FilteringScore<F> score = FilteringScore.evaluate(index, filter);
                if (score.getRemainderCount() == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    public class Result {
        private final Filter<S> mFilter;

        private final CompositeScore<S> mScore;
        private final StorableIndex<S> mLocalIndex;
        private final StorableIndex<?> mForeignIndex;
        private final ChainedProperty<S> mForeignProperty;

        private final Filter<S> mRemainderFilter;
        private final List<OrderedProperty<S>> mRemainderOrderings;

        Result(Filter<S> filter,
               CompositeScore<S> score,
               StorableIndex<S> localIndex,
               StorableIndex<?> foreignIndex,
               ChainedProperty<S> foreignProperty)
        {
            mFilter = filter;
            mScore = score;
            mLocalIndex = localIndex;
            mForeignIndex = foreignIndex;
            mForeignProperty = foreignProperty;
            mRemainderFilter = score.getFilteringScore().getRemainderFilter();
            mRemainderOrderings = score.getOrderingScore().getRemainderOrderings();
        }

        // Called by mergeRemainder.
        private Result(Result result,
                       Filter<S> remainderFilter,
                       List<OrderedProperty<S>> remainderOrderings)
        {
            mFilter = result.mFilter == null ? remainderFilter
                : (remainderFilter == null ? result.mFilter : result.mFilter.or(remainderFilter));

            mScore = result.mScore;
            mLocalIndex = result.mLocalIndex;
            mForeignIndex = result.mForeignIndex;
            mForeignProperty = result.mForeignProperty;
            mRemainderFilter = remainderFilter;
            mRemainderOrderings = remainderOrderings;
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
        public List<OrderedProperty<S>> getOrderings() {
            List<OrderedProperty<S>> handled = mScore.getOrderingScore().getHandledOrderings();
            List<OrderedProperty<S>> remainder = getRemainderOrderings();

            if (handled.size() == 0) {
                return remainder;
            }
            if (remainder.size() == 0) {
                return handled;
            }

            List<OrderedProperty<S>> combined =
                new ArrayList<OrderedProperty<S>>(handled.size() + remainder.size());

            combined.addAll(handled);
            combined.addAll(remainder);

            return combined;
        }

        /**
         * Returns the score on how well the selected index performs the
         * desired filtering and ordering. When building a query executor, do
         * not use the remainder filter and orderings available in the
         * composite score. Instead, get them directly from this result object.
         */
        public CompositeScore<S> getCompositeScore() {
            return mScore;
        }

        /**
         * Remainder filter which overrides that in composite score.
         */
        public Filter<S> getRemainderFilter() {
            return mRemainderFilter;
        }

        /**
         * Remainder orderings which override that in composite score.
         */
        public List<OrderedProperty<S>> getRemainderOrderings() {
            return mRemainderOrderings;
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
         * property corresponds to the "bToAProperty" of {@link
         * JoinedQueryExecutor}.
         */
        public ChainedProperty<S> getForeignProperty() {
            return mForeignProperty;
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

            return new Result
                (this,
                 getCompositeScore().mergeRemainderFilter(other.getCompositeScore()),
                 getCompositeScore().mergeRemainderOrderings(other.getCompositeScore()));
        }

        /**
         * Merges the remainder filter of this result with the given filter,
         * returning a new result. If handlesAnything return true, then it
         * doesn't make sense to call this method.
         */
        public Result mergeRemainder(Filter<S> filter) {
            Filter<S> remainderFilter = getRemainderFilter();
            if (remainderFilter == null) {
                remainderFilter = filter;
            } else if (filter != null) {
                remainderFilter = remainderFilter.or(filter);
            }

            return new Result(this, remainderFilter, getRemainderOrderings());
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
