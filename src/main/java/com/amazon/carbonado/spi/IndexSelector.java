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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Tries to find the best index to use for a query. When used to sort a list of
 * indexes, the first in the list (the lowest) is the best index.
 *
 * @author Brian S O'Neill
 */
public class IndexSelector<S extends Storable> implements Comparator<StorableIndex<S>> {
    static int intCompare(int a, int b) {
        if (a < b) {
            return -1;
        }
        if (a > b) {
            return 1;
        }
        return 0;
    }

    // Also called by BaseQueryEngine.
    @SuppressWarnings("unchecked")
    static <E extends Comparable> int listCompare(List<? extends E> a,
                                                  List<? extends E> b) {
        int size = Math.min(a.size(), b.size());
        for (int i=0; i<size; i++) {
            int result = a.get(i).compareTo(b.get(i));
            if (result != 0) {
                return result;
            }
        }
        if (a.size() < size) {
            return -1;
        }
        if (a.size() > size) {
            return 1;
        }
        return 0;
    }

    // Original filter passed into constructor
    private final Filter<S> mFilter;

    // Elements of original filter, which are combined by logical 'and's. Filters
    // which are likely to remove more results are ordered first in the array.
    final PropertyFilter<S>[] mFilters;

    final OrderedProperty<S>[] mOrderings;

    /**
     * @param filter filter which cannot contain any logical 'or' operations.
     * @throws IllegalArgumentException if filter not supported
     */
    public IndexSelector(Filter<S> filter) {
        this(filter, (OrderedProperty<S>[]) null);
    }

    /**
     * @param filter optional filter which cannot contain any logical 'or' operations.
     * @param orderings optional orderings
     * @throws IllegalArgumentException if filter not supported
     */
    @SuppressWarnings("unchecked")
    public IndexSelector(Filter<S> filter, OrderedProperty<S>... orderings) {
        mFilter = filter;

        // Copy property filters.
        final List<PropertyFilter<S>> filterList = new ArrayList<PropertyFilter<S>>();

        if (filter != null) {
            filter.accept(new Visitor<S, Object, Object>() {
                public Object visit(OrFilter<S> filter, Object param) {
                    throw new IllegalArgumentException("Logical 'or' not allowed");
                }

                public Object visit(PropertyFilter<S> filter, Object param) {
                    filterList.add(filter);
                    return null;
                }
            }, null);
        }

        mFilters = filterList.toArray(new PropertyFilter[filterList.size()]);
        // Ensure all '=' operators are first, and all '!=' operators are last.
        Arrays.sort(mFilters, new PropertyFilterComparator<S>());

        if (orderings == null || orderings.length == 0) {
            mOrderings = null;
        } else {
            // Copy ordering properties, but don't duplicate properties.
            int length = orderings.length;
            Map<ChainedProperty<S>, OrderedProperty<S>> orderingMap =
                    new LinkedHashMap<ChainedProperty<S>, OrderedProperty<S>>(length);
            for (int i=0; i<length; i++) {
                OrderedProperty<S> ordering = orderings[i];
                if (ordering != null) {
                    ChainedProperty<S> prop = ordering.getChainedProperty();
                    if (!orderingMap.containsKey(prop)) {
                        orderingMap.put(prop, ordering);
                    }
                }
            }

            // Drop orderings against exact matches in filter since they aren't needed.
            for (PropertyFilter<S> propFilter : filterList) {
                if (propFilter.getOperator() == RelOp.EQ) {
                    orderingMap.remove(propFilter.getChainedProperty());
                }
            }

            mOrderings = orderingMap.values().toArray(new OrderedProperty[orderingMap.size()]);
        }
    }

    /**
     * Returns &lt;0 if the current index is better than the candidate index, 0
     * if they are equally good, or &gt;0 if the candidate index is
     * better.
     * <p>
     * Note: the best index may sort results totally reversed. The cursor that
     * uses this index must iterate in reverse to compensate.
     *
     * @param currentIndex current "best" index
     * @param candidateIndex index to test against
     */
    public int compare(StorableIndex<S> currentIndex, StorableIndex<S> candidateIndex) {
        if (currentIndex == null) {
            if (candidateIndex == null) {
                return 0;
            } else {
                return 1;
            }
        } else if (candidateIndex == null) {
            return -1;
        }

        IndexScore<S> currentScore = new IndexScore<S>(this, currentIndex);
        IndexScore<S> candidateScore = new IndexScore<S>(this, candidateIndex);

        return currentScore.compareTo(candidateScore);
    }

    /**
     * Examines the given index for overall fitness.
     */
    public IndexFitness<S> examine(StorableIndex<S> index) {
        return new IndexFitness<S>(this, index, mFilter, mFilters, mOrderings);
    }

    /**
     * Provides information regarding the overall fitness of an index for use
     * in a query, and gives us information about how we can properly apply it.  That is,
     * if the index provides 3 out of 7 properties, we'll have to scan the output and apply the
     * remaining four by hand.  If an index does not sort the property for which we're doing an
     * inexact match, we'll have to subsort -- and so on.
     */
    public static class IndexFitness<S extends Storable> implements Comparable<IndexFitness<?>> {
        private final StorableIndex<S> mIndex;
        private final IndexScore<S> mIndexScore;

        private final Filter<S> mExactFilter;
        private final PropertyFilter<S>[] mInclusiveRangeStartFilters;
        private final PropertyFilter<S>[] mExclusiveRangeStartFilters;
        private final PropertyFilter<S>[] mInclusiveRangeEndFilters;
        private final PropertyFilter<S>[] mExclusiveRangeEndFilters;
        private final Filter<S> mRemainderFilter;

        private final OrderedProperty<S>[] mHandledOrderings;
        private final OrderedProperty<S>[] mRemainderOrderings;

        private final boolean mShouldReverseOrder;
        private final boolean mShouldReverseRange;

        @SuppressWarnings("unchecked")
        IndexFitness(IndexSelector<S> selector, StorableIndex<S> index,
                     Filter<S> fullFilter, PropertyFilter<S>[] fullFilters,
                     OrderedProperty<S>[] fullOrderings)
        {
            mIndex = index;
            mIndexScore = new IndexScore<S>(selector, index);

            FilterScore filterScore = mIndexScore.getFilterScore();

            Filter<S> exactFilter;
            List<PropertyFilter<S>> inclusiveRangeStartFilters =
                new ArrayList<PropertyFilter<S>>();
            List<PropertyFilter<S>> exclusiveRangeStartFilters =
                new ArrayList<PropertyFilter<S>>();
            List<PropertyFilter<S>> inclusiveRangeEndFilters = new ArrayList<PropertyFilter<S>>();
            List<PropertyFilter<S>> exclusiveRangeEndFilters = new ArrayList<PropertyFilter<S>>();
            Filter<S> remainderFilter;

            Direction rangeDirection = null;
            buildFilters: {
                if (fullFilter == null) {
                    exactFilter = null;
                    remainderFilter = fullFilter;
                    break buildFilters;
                }

                int exactMatches = filterScore.exactMatches();
                int indexPos = 0;

                LinkedList<PropertyFilter<S>> filterList =
                    new LinkedList<PropertyFilter<S>>(Arrays.asList(fullFilters));

                if (exactMatches <= 0) {
                    exactFilter = null;
                } else {
                    exactFilter = null;
                    // Build filter whose left-to-right property order matches
                    // the order of the index.
                    for (int i=0; i<exactMatches; i++) {
                        StorableProperty<S> indexProp = index.getProperty(indexPos++);
                        Filter<S> next = removeIndexProp(filterList, indexProp, RelOp.EQ);
                        if (next != null) {
                            exactFilter = (exactFilter == null) ? next : exactFilter.and(next);
                        }
                    }
                }

                if (filterScore.hasInexactMatch()) {
                    // All matches must be consecutive, so first inexact match
                    // is index property after all exact matches.
                    StorableProperty<S> indexProp = index.getProperty(indexPos);
                    rangeDirection = index.getPropertyDirection(indexPos);

                    while (true) {
                        PropertyFilter<S> p = removeIndexProp(filterList, indexProp, RelOp.GE);
                        if (p == null) {
                            break;
                        }
                        inclusiveRangeStartFilters.add(p);
                    }

                    while (true) {
                        PropertyFilter<S> p = removeIndexProp(filterList, indexProp, RelOp.GT);
                        if (p == null) {
                            break;
                        }
                        exclusiveRangeStartFilters.add(p);
                    }

                    while (true) {
                        PropertyFilter<S> p = removeIndexProp(filterList, indexProp, RelOp.LE);
                        if (p == null) {
                            break;
                        }
                        inclusiveRangeEndFilters.add(p);
                    }

                    while (true) {
                        PropertyFilter<S> p = removeIndexProp(filterList, indexProp, RelOp.LT);
                        if (p == null) {
                            break;
                        }
                        exclusiveRangeEndFilters.add(p);
                    }
                }

                remainderFilter = null;
                while (filterList.size() > 0) {
                    Filter<S> next = filterList.removeFirst();
                    remainderFilter = (remainderFilter == null) ? next : remainderFilter.and(next);
                }
            }

            mExactFilter = exactFilter;
            mInclusiveRangeStartFilters =
                inclusiveRangeStartFilters.toArray(new PropertyFilter[0]);
            mExclusiveRangeStartFilters =
                exclusiveRangeStartFilters.toArray(new PropertyFilter[0]);
            mInclusiveRangeEndFilters = inclusiveRangeEndFilters.toArray(new PropertyFilter[0]);
            mExclusiveRangeEndFilters = exclusiveRangeEndFilters.toArray(new PropertyFilter[0]);
            mRemainderFilter = remainderFilter;

            OrderingScore orderingScore = mIndexScore.getOrderingScore();

            OrderedProperty<S>[] handledOrderings;
            OrderedProperty<S>[] remainderOrderings;
            boolean shouldReverseOrder;

            buildOrderings: {
                int totalMatches = orderingScore.totalMatches();
                if (fullOrderings == null || fullOrderings.length == 0 || totalMatches == 0) {
                    handledOrderings = null;
                    remainderOrderings = fullOrderings;
                    shouldReverseOrder = false;
                    break buildOrderings;
                }

                shouldReverseOrder = totalMatches < 0;
                totalMatches = Math.abs(totalMatches);

                if (totalMatches >= fullOrderings.length) {
                    handledOrderings = fullOrderings;
                    remainderOrderings = null;
                    break buildOrderings;
                }

                final int pos = orderingScore.startPosition();

                if (index.isUnique() && (pos + totalMatches) >= index.getPropertyCount()) {
                    // Since all properties of unique index have been used, additional
                    // remainder ordering is superfluous, and so it is handled.
                    handledOrderings = fullOrderings;
                    remainderOrderings = null;
                    break buildOrderings;
                }

                Set<OrderedProperty<S>> handledSet = new LinkedHashSet<OrderedProperty<S>>();
                Set<OrderedProperty<S>> remainderSet =
                        new LinkedHashSet<OrderedProperty<S>>(Arrays.asList(fullOrderings));

                for (int i=0; i<totalMatches; i++) {
                    ChainedProperty<S> chainedProp =
                        ChainedProperty.get(index.getProperty(pos + i));
                    OrderedProperty<S> op;
                    op = OrderedProperty.get(chainedProp, Direction.ASCENDING);
                    if (remainderSet.remove(op)) {
                        handledSet.add(op);
                    }
                    op = OrderedProperty.get(chainedProp, Direction.DESCENDING);
                    if (remainderSet.remove(op)) {
                        handledSet.add(op);
                    }
                    op = OrderedProperty.get(chainedProp, Direction.UNSPECIFIED);
                    if (remainderSet.remove(op)) {
                        handledSet.add(op);
                    }
                }

                if (remainderSet.size() == 0) {
                    handledOrderings = fullOrderings;
                    remainderOrderings = null;
                    break buildOrderings;
                }

                if (handledSet.size() == 0) {
                    handledOrderings = null;
                    remainderOrderings = fullOrderings;
                    break buildOrderings;
                }

                handledOrderings = handledSet.toArray
                    (new OrderedProperty[handledSet.size()]);
                remainderOrderings = remainderSet.toArray
                    (new OrderedProperty[remainderSet.size()]);
            }

            // If using range match, but index direction is backwards. Flipping
            // "shouldReverseOrder" doesn't fix the problem. Instead, swap the
            // ranges around.
            boolean shouldReverseRange = rangeDirection == Direction.DESCENDING;

            mHandledOrderings = handledOrderings;
            mRemainderOrderings = remainderOrderings;
            mShouldReverseOrder = shouldReverseOrder;
            mShouldReverseRange = shouldReverseRange;
        }

        private IndexFitness(StorableIndex<S> index, IndexScore<S> indexScore,
                             Filter<S> exactFilter,
                             PropertyFilter<S>[] inclusiveRangeStartFilters,
                             PropertyFilter<S>[] exclusiveRangeStartFilters,
                             PropertyFilter<S>[] inclusiveRangeEndFilters,
                             PropertyFilter<S>[] exclusiveRangeEndFilters,
                             Filter<S> remainderFilter,
                             OrderedProperty<S>[] handledOrderings,
                             OrderedProperty<S>[] remainderOrderings,
                             boolean shouldReverseOrder,
                             boolean shouldReverseRange)
        {
            mIndex = index;
            mIndexScore = indexScore;

            mExactFilter = exactFilter;
            mInclusiveRangeStartFilters = inclusiveRangeStartFilters;
            mExclusiveRangeStartFilters = exclusiveRangeStartFilters;
            mInclusiveRangeEndFilters = inclusiveRangeEndFilters;
            mExclusiveRangeEndFilters = exclusiveRangeEndFilters;
            mRemainderFilter = remainderFilter;

            mHandledOrderings = handledOrderings;
            mRemainderOrderings = remainderOrderings;

            mShouldReverseOrder = shouldReverseOrder;
            mShouldReverseRange = shouldReverseRange;
        }

        private PropertyFilter<S> removeIndexProp(List<PropertyFilter<S>> filterList,
                                                  StorableProperty<S> indexProp,
                                                  RelOp operator)
        {
            Iterator<PropertyFilter<S>> it = filterList.iterator();
            while (it.hasNext()) {
                PropertyFilter<S> filter = it.next();

                if (operator != filter.getOperator()) {
                    continue;
                }

                ChainedProperty<S> chainedProp = filter.getChainedProperty();
                if (chainedProp.getChainCount() == 0) {
                    StorableProperty<S> prime = chainedProp.getPrimeProperty();
                    if (indexProp.equals(prime)) {
                        it.remove();
                        return filter;
                    }
                }
            }
            return null;
        }

        /**
         * Returns the index that this fitness object applies to.
         */
        public StorableIndex<S> getIndex() {
            return mIndex;
        }

        /**
         * Returns true if the index doesn't actually do anything to filter
         * results or to order them.
         */
        public boolean isUseless() {
            return mExactFilter == null
                && mInclusiveRangeStartFilters.length == 0
                && mExclusiveRangeStartFilters.length == 0
                && mInclusiveRangeEndFilters.length == 0
                && mExclusiveRangeEndFilters.length == 0
                && (mHandledOrderings == null || mHandledOrderings.length == 0);
        }

        /**
         * Returns true if the index results should be iterated in reverse.
         */
        public boolean shouldReverseOrder() {
            return mShouldReverseOrder;
        }

        /**
         * Returns true if start and end ranges should be reversed.
         */
        public boolean shouldReverseRange() {
            return mShouldReverseRange;
        }

        /**
         * Returns the filter handled by the applicable index for exact
         * matches. Is null if none.
         */
        public Filter<S> getExactFilter() {
            return mExactFilter;
        }

        /**
         * Returns true if index is unique and exact filter matches each index
         * property. Using this index guarantees one fetch result.
         */
        public boolean isKeyFilter() {
            if (mExactFilter == null || !mIndex.isUnique()) {
                return false;
            }

            final Set<StorableProperty<S>> properties;
            {
                int propertyCount = mIndex.getPropertyCount();
                properties = new HashSet<StorableProperty<S>>(propertyCount);
                for (int i=0; i<propertyCount; i++) {
                    properties.add(mIndex.getProperty(i));
                }
            }

            mExactFilter.accept(new Visitor<S, Object, Object>() {
                public Object visit(PropertyFilter<S> filter, Object param) {
                    ChainedProperty<S> chained = filter.getChainedProperty();
                    if (chained.getChainCount() == 0) {
                        properties.remove(chained.getPrimeProperty());
                    }
                    return null;
                }
            }, null);

            return properties.size() == 0;
        }

        /**
         * Returns the filters handled by the applicable index for range
         * matches. All property names are the same and operator is GE.
         */
        public PropertyFilter<S>[] getInclusiveRangeStartFilters() {
            return mInclusiveRangeStartFilters;
        }

        /**
         * Returns the filters handled by the applicable index for range
         * matches. All property names are the same and operator is GT.
         */
        public PropertyFilter<S>[] getExclusiveRangeStartFilters() {
            return mExclusiveRangeStartFilters;
        }

        /**
         * Returns the filters handled by the applicable index for range
         * matches. All property names are the same and operator is LE.
         */
        public PropertyFilter<S>[] getInclusiveRangeEndFilters() {
            return mInclusiveRangeEndFilters;
        }

        /**
         * Returns the filters handled by the applicable index for range
         * matches. All property names are the same and operator is LT.
         */
        public PropertyFilter<S>[] getExclusiveRangeEndFilters() {
            return mExclusiveRangeEndFilters;
        }

        /**
         * Returns a filter which contains terms not handled by the applicable
         * index. If the selector has no filter or if the index is complete,
         * null is returned. If the index filters nothing required by the
         * selector, the complete filter is returned.
         */
        public Filter<S> getRemainderFilter() {
            return mRemainderFilter;
        }

        /**
         * Returns the desired orderings handled by the applicable index,
         * possibly when reversed.
         */
        public OrderedProperty<S>[] getHandledOrderings() {
            return (mHandledOrderings == null) ? null : mHandledOrderings.clone();
        }

        /**
         * Returns desired orderings not handled by the applicable index,
         * possibly when reversed. If the selector has no ordering or the index
         * is complete, null is returned. If the index orders nothing required
         * by the selector, the complete reduced ordering is returned.
         */
        public OrderedProperty<S>[] getRemainderOrderings() {
            return (mRemainderOrderings == null) ? null : mRemainderOrderings.clone();
        }

        /**
         * Returns the orderings actually provided by the applicable index,
         * possibly when reversed. Natural order is not a total order unless
         * index is unique.
         */
        public OrderedProperty<S>[] getNaturalOrderings() {
            return getActualOrderings(false);
        }

        /**
         * Returns the orderings actually provided by the applicable index,
         * excluding exact filter matches, possibly when reversed. Effective
         * order is not a total order unless index is unique.
         */
        public OrderedProperty<S>[] getEffectiveOrderings() {
            return getActualOrderings(true);
        }

        @SuppressWarnings("unchecked")
        private OrderedProperty<S>[] getActualOrderings(boolean excludeExactMatches) {
            int exactMatches = 0;
            if (excludeExactMatches) {
                exactMatches = mIndexScore.getFilterScore().exactMatches();
            }

            int count = mIndex.getPropertyCount();
            OrderedProperty<S>[] orderings = new OrderedProperty[count - exactMatches];
            for (int i=exactMatches; i<count; i++) {
                StorableProperty<S> property = mIndex.getProperty(i);
                Direction direction = mIndex.getPropertyDirection(i);
                if (mShouldReverseOrder) {
                    direction = direction.reverse();
                }
                orderings[i - exactMatches] = OrderedProperty.get(property, direction);
            }
            return orderings;
        }

        /**
         * Compares this fitness to another which belongs to a different
         * Storable type. Filters that reference a joined property may be best
         * served by an index defined in the joined type, and this method aids
         * in that selection.
         *
         * @return &lt;0 if this score is better, 0 if equal, or &gt;0 if other is better
         */
        public int compareTo(IndexFitness<?> otherFitness) {
            return mIndexScore.compareTo(otherFitness.mIndexScore);
        }

        /**
         * Returns true if given fitness result uses the same index, and in the
         * same way.
         */
        public boolean canUnion(IndexFitness fitness) {
            if (this == fitness) {
                return true;
            }

            return mIndex.equals(fitness.mIndex) &&
                (mExactFilter == null ?
                 fitness.mExactFilter == null :
                 (mExactFilter.equals(fitness.mExactFilter))) &&
                Arrays.equals(mInclusiveRangeStartFilters,
                              fitness.mInclusiveRangeStartFilters) &&
                Arrays.equals(mExclusiveRangeStartFilters,
                              fitness.mExclusiveRangeStartFilters) &&
                Arrays.equals(mInclusiveRangeEndFilters,
                              fitness.mInclusiveRangeEndFilters) &&
                Arrays.equals(mExclusiveRangeEndFilters,
                              fitness.mExclusiveRangeEndFilters) &&
                mShouldReverseOrder == fitness.mShouldReverseOrder &&
                mShouldReverseRange == fitness.mShouldReverseRange &&
                (mHandledOrderings == null ?
                 fitness.mHandledOrderings == null :
                 (Arrays.equals(mHandledOrderings, fitness.mHandledOrderings)));
        }

        /**
         * If the given fitness can union with this one, return a new unioned
         * one. If union not possible, null is returned.
         */
        public IndexFitness union(IndexFitness fitness) {
            if (this == fitness) {
                return this;
            }

            if (!canUnion(fitness)) {
                return null;
            }

            // Union the remainder filter and orderings.

            Filter<S> remainderFilter;
            if (mRemainderFilter == null) {
                if (fitness.mRemainderFilter == null) {
                    remainderFilter = null;
                } else {
                    remainderFilter = fitness.mRemainderFilter;
                }
            } else {
                if (fitness.mRemainderFilter == null) {
                    remainderFilter = mRemainderFilter;
                } else if (mRemainderFilter.equals(fitness.mRemainderFilter)) {
                    remainderFilter = mRemainderFilter;
                } else {
                    remainderFilter = mRemainderFilter.or(fitness.mRemainderFilter);
                }
            }

            OrderedProperty<S>[] remainderOrderings;
            if (mRemainderOrderings == null) {
                if (fitness.mRemainderOrderings == null) {
                    remainderOrderings = null;
                } else {
                    remainderOrderings = fitness.mRemainderOrderings;
                }
            } else {
                if (fitness.mRemainderOrderings == null) {
                    remainderOrderings = mRemainderOrderings;
                } else if (mRemainderOrderings.length >= fitness.mRemainderOrderings.length) {
                    remainderOrderings = mRemainderOrderings;
                } else {
                    remainderOrderings = fitness.mRemainderOrderings;
                }
            }

            return new IndexFitness<S>(mIndex, mIndexScore,
                                       mExactFilter,
                                       mInclusiveRangeStartFilters,
                                       mExclusiveRangeStartFilters,
                                       mInclusiveRangeEndFilters,
                                       mExclusiveRangeEndFilters,
                                       remainderFilter,
                                       mHandledOrderings,
                                       remainderOrderings,
                                       mShouldReverseOrder,
                                       mShouldReverseRange);
        }

        public String toString() {
            return "IndexFitness [index=" + mIndex
                + ", filterScore=" + mIndexScore.getFilterScore()
                + ", orderingScore=" + mIndexScore.getOrderingScore()
                + ", exactFilter=" + quoteNonNull(mExactFilter)
                + ", inclusiveRangeStartFilters=" + mInclusiveRangeStartFilters
                + ", exclusiveRangeStartFilters=" + mExclusiveRangeStartFilters
                + ", inclusiveRangeEndFilters=" + mInclusiveRangeEndFilters
                + ", exclusiveRangeEndFilters=" + mExclusiveRangeEndFilters
                + ", remainderFilter=" + quoteNonNull(mRemainderFilter)
                + ", handledOrderings=" + Arrays.toString(mHandledOrderings)
                + ", remainderOrderings=" + Arrays.toString(mRemainderOrderings)
                + ", shouldReverse=" + mShouldReverseOrder
                + ']';
        }

        private static String quoteNonNull(Filter value) {
            return value == null ? null : ('"' + String.valueOf(value) + '"');
        }
    }

    /**
     * Composite of filter score and ordering score. The filter score measures
     * how well an index performs the desired level of filtering. Likewise, the
     * ordering score measures how well an index performs the desired ordering.
     */
    private static class IndexScore<S extends Storable> implements Comparable<IndexScore<?>> {
        private final IndexSelector<S> mSelector;
        private final StorableIndex<S> mIndex;

        private FilterScore<S> mFilterScore;
        private OrderingScore mOrderingScore;

        IndexScore(IndexSelector<S> selector, StorableIndex<S> index) {
            mSelector = selector;
            mIndex = index;
        }

        @SuppressWarnings("unchecked")
        public int compareTo(IndexScore<?> candidateScore) {
            final FilterScore thisFilterScore = this.getFilterScore();
            final FilterScore candidateFilterScore = candidateScore.getFilterScore();

            // Compare total count of exact matching properties.
            {
                int result = thisFilterScore.compareExact(candidateFilterScore);
                if (result != 0) {
                    return result;
                }
            }

            // Exact matches same, choose clustered index if more than one match.
            if (thisFilterScore.exactMatches() > 1) {
                if (mIndex.isClustered()) {
                    if (!candidateScore.mIndex.isClustered()) {
                        return -1;
                    }
                } else if (candidateScore.mIndex.isClustered()) {
                    return 1;
                }
            }

            // Compare range match. (index can have at most one range match)
            if (thisFilterScore.hasRangeMatch()) {
                if (candidateFilterScore.hasRangeMatch()) {
                    // Both have range match, choose clustered index.
                    if (mIndex.isClustered()) {
                        if (!candidateScore.mIndex.isClustered()) {
                            return -1;
                        }
                    } else if (candidateScore.mIndex.isClustered()) {
                        return 1;
                    }
                } else {
                    return -1;
                }
            } else if (candidateFilterScore.hasRangeMatch()) {
                return 1;
            }

            final OrderingScore thisOrderingScore = this.getOrderingScore();
            final OrderingScore candidateOrderingScore = candidateScore.getOrderingScore();

            int finalResult = 0;

            // Compare orderings, but only if candidate filters anything. It is
            // generally slower to scan an index just for correct ordering,
            // than it is to sort the results of a full scan. Why? Because an
            // index scan results in a lot of random file accesses, and disk is
            // so slow.  There is an exception to this rule if the candidate is
            // a clustered index, in which case there are no random file
            // accesses.
            if (candidateFilterScore.anyMatches() || candidateScore.mIndex.isClustered()) {
                int currentMatches = thisOrderingScore.totalMatches();
                int candidateMatches = candidateOrderingScore.totalMatches();
                if (currentMatches != candidateMatches) {
                    if (Math.abs(currentMatches) > Math.abs(candidateMatches)) {
                        // Only select current filter if it filters anything.
                        if (thisFilterScore.anyMatches()) {
                            return -1;
                        }
                        // Potentially use this result later.
                        finalResult = -1;
                    } else if (Math.abs(currentMatches) < Math.abs(candidateMatches)) {
                        return 1;
                    } else {
                        // Magnitudes are equal, but sign differs. Choose positive,
                        // but not yet.
                        finalResult = (currentMatches > 0) ? -1 : 1;
                    }
                }
            }

            // Compare total count of inexact matching properties.
            {
                int result = thisFilterScore.compareInexact(candidateFilterScore);
                if (result != 0) {
                    return result;
                }
            }

            // Compare positioning of matching properties (favor index that best
            // matches specified property sequence of filter)
            {
                int result = thisFilterScore.compareExactPositions(candidateFilterScore);
                if (result != 0) {
                    return result;
                }
                result = thisFilterScore.compareInexactPosition(candidateFilterScore);
                if (result != 0) {
                    return result;
                }
            }

            // If both indexes have a non-zero score (that is, either index would
            // actually be useful), choose the one that has the least number of
            // properties in it. The theory being that smaller index keys mean more
            // nodes will fit into the memory cache during an index scan. This
            // extra test doesn't try to estimate the average size of properties,
            // so it may choose wrong.
            {
                if ((thisFilterScore.anyMatches() && candidateFilterScore.anyMatches()) ||
                    (thisOrderingScore.anyMatches() && candidateOrderingScore.anyMatches()))
                {
                    if (mIndex.getPropertyCount() < candidateScore.mIndex.getPropertyCount()) {
                        return -1;
                    }
                    if (mIndex.getPropertyCount() > candidateScore.mIndex.getPropertyCount()) {
                        return 1;
                    }
                }
            }

            // Final result derived from ordering comparison earlier.
            return finalResult;
        }

        /**
         * Total matches on score indicates how many consecutive index
         * properties (from the start) match the filter requirements.
         */
        public FilterScore<S> getFilterScore() {
            if (mFilterScore != null) {
                return mFilterScore;
            }

            mFilterScore = new FilterScore<S>();

            int indexPropCount = mIndex.getPropertyCount();
            PropertyFilter<S>[] filters = mSelector.mFilters;
            int filterCount = filters.length;

            for (int i=0; i<indexPropCount; i++) {
                StorableProperty<S> prop = mIndex.getProperty(i);
                int matchesBefore = mFilterScore.totalMatches();
                for (int pos=0; pos<filterCount; pos++) {
                    mFilterScore.tally(prop, filters[pos], pos);
                }
                if (mFilterScore.totalMatches() <= matchesBefore) {
                    // Missed an index property and cannot have holes in index.
                    break;
                }
            }

            return mFilterScore;
        }

        public OrderingScore getOrderingScore() {
            if (mOrderingScore != null) {
                return mOrderingScore;
            }

            OrderedProperty<S>[] orderings = mSelector.mOrderings;

            if (orderings == null || orderings.length == 0) {
                return mOrderingScore = new OrderingScore(0, 0);
            }

            int indexPropCount = mIndex.getPropertyCount();
            if (indexPropCount <= 0) {
                return mOrderingScore = new OrderingScore(0, 0);
            }

            // Make sure first ordering property follows exact matches.

            if (orderings[0].getChainedProperty().getChainCount() > 0) {
                // Indexes don't currently support chained properties.
                return mOrderingScore = new OrderingScore(0, 0);
            }

            final StorableProperty<S> first =
                orderings[0].getChainedProperty().getPrimeProperty();

            // Start pos after all exact matching filter properties
            int pos = getFilterScore().exactMatches();

            if (pos >= indexPropCount || !mIndex.getProperty(pos).equals(first)) {
                return mOrderingScore = new OrderingScore(0, 0);
            }

            boolean reverse = false;
            switch (mIndex.getPropertyDirection(pos)) {
            case ASCENDING:
                reverse = (orderings[0].getDirection() == Direction.DESCENDING);
                break;
            case DESCENDING:
                reverse = (orderings[0].getDirection() == Direction.ASCENDING);
                break;
            }

            // Match count is the run length of matching properties.
            int matches = 1;
            final int startPos = pos;

            calcMatches:
            for (int i=1; i<orderings.length && ++pos<indexPropCount; i++) {
                if (orderings[i].getChainedProperty().getChainCount() > 0) {
                    // Indexes don't currently support chained properties.
                    break;
                }

                if (mIndex.getProperty(pos).equals
                        (orderings[i].getChainedProperty().getPrimeProperty())) {
                    if (orderings[i].getDirection() != Direction.UNSPECIFIED) {
                        Direction expected = mIndex.getPropertyDirection(pos);
                        if (reverse) {
                            expected = expected.reverse();
                        }
                        if (orderings[i].getDirection() != expected) {
                            break calcMatches;
                        }
                    }
                    matches++;
                }
            }

            return mOrderingScore = new OrderingScore(startPos, reverse ? -matches : matches);
        }
    }

    /**
     * One of the scores that evaluates an index's fitness for a given filter.
     * <P>A filter mentions properties, either as exact ("=") or inexact (">" "<", et al)
     * <P>An index contains properties, in order.
     * <P>An index property matches a filter if the filter uses that property, and if all of the
     * properties in the index to the left of the property are also in the filter (since holes
     * in the index make the index useless for subsequent properties)
     * <P>Then the index filter score is a function of the number of matches it contains,
     * and how early in the filter they appear.
     * <P>Any exact filter match beats an inexact filter.
     * <P>More exact filter matches beats fewer.
     * <P>Inexact will be selected if there are no exact matches
     *
     * <P>Note that there will be only one inexact match, since once we're in an inexact range we
     * have to scan the entire range (and a later inexact match will be arbitrarily ordered within
     * that range).
     *
     * <P>For example:
     * <pre>
     * user query: "a>? & b=? & c = ?"
     * will be presorted to
     * "b=? & c=? & a>?
     * a, b, c     == a[inexact]->2, b->0, c->1
     * d, a, b, c  == useless
     * c, d, b     == c->1
     * </pre>
     * ...so the "c,d,b" index will win.
     */
    private static class FilterScore<S extends Storable> {
        // Positions of exact matches
        private List<Integer> mExactMatches = new ArrayList<Integer>();

        // Properties which have been used for exact matching -- these should
        // show up only once per filter set
        private Set<StorableProperty> mExactMatchProps = new HashSet<StorableProperty>();

        // Position of inexact match
        private int mInexactMatchPos;

        // Property used for inexact match
        private StorableProperty<S> mInexactMatch;

        private boolean mHasRangeStart;
        private boolean mHasRangeEnd;

        FilterScore() {
        }

        /**
         * Tally up filter score.
         *
         * @param prop property of candidate index
         * @param filter property filter to check for index fitness
         * @param pos position of filter in filter list
         */
        void tally(StorableProperty<S> prop, PropertyFilter<S> filter, int pos) {
            ChainedProperty<S> chained = filter.getChainedProperty();

            if (chained.getChainCount() == 0 && chained.getPrimeProperty().equals(prop)) {
                switch (filter.getOperator()) {
                case EQ:
                    // Exact match
                    if (mInexactMatch == null) {
                        mExactMatches.add(pos);
                        mExactMatchProps.add(prop);
                    }

                    break;

                case LT: case GE: case GT: case LE:
                    // Inexact match

                    if (mInexactMatch == null) {
                        // If for some reason the query contains an exact and
                        // an inexact match on the same property (a>1 & a=14)
                        // we'll never care about the inexact match.
                        if (!mExactMatchProps.contains(prop)) {
                            mInexactMatchPos = pos;
                            mInexactMatch = prop;
                        }
                    }

                    // Check for range match
                    if (prop.equals(mInexactMatch)) {
                        switch (filter.getOperator()) {
                        case LT: case LE:
                            mHasRangeStart = true;
                            break;
                        case GT: case GE:
                            mHasRangeEnd = true;
                            break;
                        }
                    }

                    break;
                }
            }
        }

        int compareExact(FilterScore<S> candidate) {
            return -intCompare(mExactMatches.size(), candidate.mExactMatches.size());
        }

        int compareInexact(FilterScore<S> candidate) {
            if (mInexactMatch == null && candidate.mInexactMatch != null) {
                return 1;
            } else if (mInexactMatch != null && candidate.mInexactMatch == null) {
                return -1;
            }
            return 0;
        }

        int compareExactPositions(FilterScore<S> candidate) {
            return listCompare(mExactMatches, candidate.mExactMatches);
        }

        int compareInexactPosition(FilterScore<S> candidate) {
            return intCompare(mInexactMatchPos, candidate.mInexactMatchPos);
        }

        boolean anyMatches() {
            return mExactMatches.size() > 0 || mInexactMatch != null;
        }

        int exactMatches() {
            return mExactMatches.size();
        }

        boolean hasRangeMatch() {
            return mHasRangeStart && mHasRangeEnd;
        }

        boolean hasInexactMatch() {
            return mInexactMatch != null;
        }

        int totalMatches() {
            return mExactMatches.size() + (mInexactMatch == null ? 0 : 1);
        }

        public String toString() {
            return "FilterScore [exactMatches=" + mExactMatches
                + ", exactMatchProps=" + mExactMatchProps
                + ", inexactMatch=" + (mInexactMatch == null ? null : mInexactMatch.getName())
                + ", rangeMatch=" + hasRangeMatch()
                + ']';
        }
    }

    /**
     * How well does this index help me sort things
     */
    private static class OrderingScore {
        private final int mStartPos;
        private final int mTotalMatches;

        OrderingScore(int startPos, int totalMatches) {
            mStartPos = startPos;
            mTotalMatches = totalMatches;
        }

        /**
         * Returns start position of index.
         */
        int startPosition() {
            return mStartPos;
        }

        boolean anyMatches() {
            return mTotalMatches > 0;
        }

        /**
         * Magnitude represents count of matching orderings. If negative
         * result, index produces reversed ordering.
         */
        int totalMatches() {
            return mTotalMatches;
        }

        public String toString() {
            return "OrderingScore [startPos=" + mStartPos
                + ", totalMatches=" + mTotalMatches
                + ']';
        }
    }

    /**
     * Sorts property filters such that '==' operations come before '!='
     * operations. Assuming a stable sort is used, all other property filters
     * are left in place
     */
    private static class PropertyFilterComparator<S extends Storable>
        implements Comparator<PropertyFilter<S>>
    {
        public int compare(PropertyFilter<S> a, PropertyFilter<S> b) {
            if (a.getOperator() != b.getOperator()) {
                if (a.getOperator() == RelOp.EQ) {
                    return -1;
                }
                if (a.getOperator() == RelOp.NE) {
                    return 1;
                }
                if (b.getOperator() == RelOp.EQ) {
                    return 1;
                }
                if (b.getOperator() == RelOp.NE) {
                    return -1;
                }
            }
            return 0;
        }
    }
}
