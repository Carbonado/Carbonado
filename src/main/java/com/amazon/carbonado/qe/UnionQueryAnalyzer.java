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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.ExistsFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableKey;

/**
 * Analyzes a query specification and determines how it can be executed as a
 * union of smaller queries. If necessary, the UnionQueryAnalyzer will alter
 * the query slightly, imposing a total ordering. Internally, an {@link
 * IndexedQueryAnalyzer} is used for selecting the best indexes.
 *
 * <p>UnionQueryAnalyzer is sharable and thread-safe. An instance for a
 * particular Storable type can be cached, avoiding repeated construction
 * cost. In addition, the analyzer caches learned foreign indexes.
 *
 * @author Brian S O'Neill
 */
public class UnionQueryAnalyzer<S extends Storable> implements QueryExecutorFactory<S> {
    final IndexedQueryAnalyzer<S> mIndexAnalyzer;
    final RepositoryAccess mRepoAccess;

    /**
     * @param type type of storable being queried
     * @param access repository access for examing available indexes
     * @throws IllegalArgumentException if type or indexProvider is null
     */
    public UnionQueryAnalyzer(Class<S> type, RepositoryAccess access) {
        mIndexAnalyzer = new IndexedQueryAnalyzer<S>(type, access);
        mRepoAccess = access;
    }

    public Class<S> getStorableType() {
        return mIndexAnalyzer.getStorableType();
    }

    /**
     * @param filter optional filter which must be {@link Filter#isBound bound}
     * @param ordering optional properties which define desired ordering
     * @param hints optional query hints
     */
    public Result analyze(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws SupportException, RepositoryException
    {
        if (filter != null && !filter.isBound()) {
            throw new IllegalArgumentException("Filter must be bound");
        }

        if (ordering == null) {
            ordering = OrderingList.emptyList();
        }

        return buildResult(filter, ordering, hints);
    }

    /**
     * Returns an executor that handles the given query specification.
     *
     * @param filter optional filter which must be {@link Filter#isBound bound}
     * @param ordering optional properties which define desired ordering
     * @param hints optional query hints
     */
    public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws RepositoryException
    {
        return analyze(filter, ordering, hints).createExecutor();
    }

    /**
     * Splits the filter into sub-results, merges sub-results, and possibly
     * imposes a total ordering.
     */
    private Result buildResult(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws SupportException, RepositoryException
    {
        List<IndexedQueryAnalyzer<S>.Result> subResults;
        if (filter == null) {
            subResults = Collections
                .singletonList(mIndexAnalyzer.analyze(filter, ordering, hints));
        } else {
            subResults = splitIntoSubResults(filter, ordering, hints);
        }

        if (subResults.size() <= 1) {
            // Total ordering not required.
            return new Result(subResults);
        }

        // If any orderings have an unspecified direction, switch to ASCENDING
        // or DESCENDING, depending on which is more popular. Then build new
        // sub-results.
        for (int pos = 0; pos < ordering.size(); pos++) {
            OrderedProperty<S> op = ordering.get(pos);
            if (op.getDirection() != Direction.UNSPECIFIED) {
                continue;
            }

            // Find out which direction is most popular for this property.
            Tally tally = new Tally(op.getChainedProperty());
            for (IndexedQueryAnalyzer<S>.Result result : subResults) {
                tally.increment(findHandledDirection(result, op));
            }

            ordering = ordering.replace(pos, op.direction(tally.getBestDirection()));

            // Re-calc with specified direction. Only do one property at a time
            // since one simple change might alter the query plan.
            subResults = splitIntoSubResults(filter, ordering, hints);

            if (subResults.size() <= 1) {
                // Total ordering no longer required.
                return new Result(subResults);
            }
        }

        // Gather all the keys available. As ordering properties touch key
        // properties, they are removed from all key sets. When a key set size
        // reaches zero, total ordering has been achieved.
        List<Set<ChainedProperty<S>>> keys = getKeys();

        // Check if current ordering is total.
        for (OrderedProperty<S> op : ordering) {
            ChainedProperty<S> property = op.getChainedProperty();
            if (pruneKeys(keys, property)) {
                // Found a key which is fully covered, indicating total ordering.
                return new Result(subResults, ordering);
            }
        }

        // Create a super key which contains all the properties required for
        // total ordering. The goal here is to append these properties to the
        // ordering in a fashion that takes advantage of each index's natural
        // ordering. This in turn should cause any sort operation to operate
        // over smaller groups. Smaller groups means smaller sort buffers.
        // Smaller sort buffers makes a merge sort happy.

        // Super key could be stored simply in a set, but a map makes it
        // convenient for tracking tallies.
        Map<ChainedProperty<S>, Tally> superKey = new LinkedHashMap<ChainedProperty<S>, Tally>();
        for (Set<ChainedProperty<S>> key : keys) {
            for (ChainedProperty<S> property : key) {
                if (!superKey.containsKey(property)) {
                    superKey.put(property, new Tally(property));
                }
            }
        }

        // Keep looping until total ordering achieved.
        while (true) {
            // For each ordering score, iterate over the entire unused ordering
            // properties and select the next free property. If property is in
            // the super key increment a tally associated with property
            // direction. Choose the property with the best tally and augment
            // the orderings with it and create new sub-results. Remove the
            // property from the super key and the key set. If any key is now
            // fully covered, a total ordering has been achieved.

            for (IndexedQueryAnalyzer<S>.Result result : subResults) {
                OrderingScore<S> score = result.getCompositeScore().getOrderingScore();

                OrderingList<S> unused = score.getUnusedOrdering();
                if (unused.size() > 0) {
                    for (OrderedProperty<S> prop : unused) {
                        ChainedProperty<S> chainedProp = prop.getChainedProperty();
                        Tally tally = superKey.get(chainedProp);
                        if (tally != null) {
                            tally.increment(prop.getDirection());
                        }
                    }
                }

                OrderingList<S> free = score.getFreeOrdering();
                if (free.size() > 0) {
                    OrderedProperty<S> prop = free.get(0);
                    ChainedProperty<S> chainedProp = prop.getChainedProperty();
                    Tally tally = superKey.get(chainedProp);
                    if (tally != null) {
                        tally.increment(prop.getDirection());
                    }
                }
            }

            Tally best = bestTally(superKey.values());
            ChainedProperty<S> bestProperty = best.getProperty();

            // Now augment the orderings and create new sub-results.
            ordering = ordering.concat(OrderedProperty.get(bestProperty, best.getBestDirection()));
            subResults = splitIntoSubResults(filter, ordering, hints);

            if (subResults.size() <= 1) {
                // Total ordering no longer required.
                break;
            }

            // Remove property from super key and key set...
            superKey.remove(bestProperty);
            if (superKey.size() == 0) {
                break;
            }
            if (pruneKeys(keys, bestProperty)) {
                break;
            }

            // Clear the tallies for the next run.
            for (Tally tally : superKey.values()) {
                tally.clear();
            }
        }

        return new Result(subResults, ordering);
    }

    /**
     * Returns a list of all primary and alternate keys, stripped of ordering.
     */
    private List<Set<ChainedProperty<S>>> getKeys()
        throws SupportException, RepositoryException
    {
        StorableInfo<S> info = StorableIntrospector.examine(mIndexAnalyzer.getStorableType());
        List<Set<ChainedProperty<S>>> keys = new ArrayList<Set<ChainedProperty<S>>>();

        keys.add(stripOrdering(info.getPrimaryKey().getProperties()));

        for (StorableKey<S> altKey : info.getAlternateKeys()) {
            keys.add(stripOrdering(altKey.getProperties()));
        }

        // Also fold in all unique indexes, just in case they weren't reported
        // as actual keys.
        Collection<StorableIndex<S>> indexes =
            mRepoAccess.storageAccessFor(getStorableType()).getAllIndexes();

        for (StorableIndex<S> index : indexes) {
            if (!index.isUnique()) {
                continue;
            }

            int propCount = index.getPropertyCount();
            Set<ChainedProperty<S>> props = new LinkedHashSet<ChainedProperty<S>>(propCount);

            for (int i=0; i<propCount; i++) {
                props.add(index.getOrderedProperty(i).getChainedProperty());
            }

            keys.add(props);
        }

        return keys;
    }

    private Set<ChainedProperty<S>> stripOrdering(Set<? extends OrderedProperty<S>> orderedProps) {
        Set<ChainedProperty<S>> props = new LinkedHashSet<ChainedProperty<S>>(orderedProps.size());
        for (OrderedProperty<S> ordering : orderedProps) {
            props.add(ordering.getChainedProperty());
        }
        return props;
    }

    /**
     * Removes the given property from all keys, returning true if any key has
     * zero properties as a result.
     */
    private boolean pruneKeys(List<Set<ChainedProperty<S>>> keys, ChainedProperty<S> property) {
        boolean result = false;

        for (Set<ChainedProperty<S>> key : keys) {
            key.remove(property);
            if (key.size() == 0) {
                result = true;
                continue;
            }
        }

        return result;
    }

    private Tally bestTally(Iterable<Tally> tallies) {
        Tally best = null;
        for (Tally tally : tallies) {
            if (best == null || tally.compareTo(best) > 0) {
                best = tally;
            }
        }
        return best;
    }

    private Direction findHandledDirection(IndexedQueryAnalyzer<S>.Result result,
                                           OrderedProperty<S> unspecified)
    {
        ChainedProperty<S> chained = unspecified.getChainedProperty();
        OrderingScore<S> score = result.getCompositeScore().getOrderingScore();
        OrderingList<S> handled = score.getHandledOrdering();
        for (OrderedProperty<S> property : handled) {
            if (chained.equals(property.getChainedProperty())) {
                return property.getDirection();
            }
        }
        return Direction.UNSPECIFIED;
    }

    /**
     * Splits the filter into sub-results and possibly merges them.
     */
    private List<IndexedQueryAnalyzer<S>.Result>
        splitIntoSubResults(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws SupportException, RepositoryException
    {
        // Required for split to work.
        Filter<S> dnfFilter = filter.disjunctiveNormalForm();

        Splitter splitter = new Splitter(ordering, hints);
        RepositoryException e = dnfFilter.accept(splitter, null);
        if (e != null) {
            throw e;
        }

        List<IndexedQueryAnalyzer<S>.Result> subResults = splitter.mSubResults;

        // Check if any sub-result handles nothing. If so, a full scan is the
        // best option for the entire query and all sub-results merge into a
        // single sub-result. Any sub-results which filter anything and contain
        // a join property in the filter are exempt from the merge. This is
        // because fewer joins are read than if a full scan is performed for
        // the entire query. The resulting union has both a full scan and an
        // index scan.

        IndexedQueryAnalyzer<S>.Result full = null;
        for (IndexedQueryAnalyzer<S>.Result result : subResults) {
            if (!result.handlesAnything()) {
                full = result;
                break;
            }
            if (!result.getCompositeScore().getFilteringScore().hasAnyMatches()) {
                if (full == null) {
                    // This index is used only for its ordering, and it will be
                    // tentatively selected as the "full scan". If a result is
                    // found doesn't use an index for anything, then it becomes
                    // the "full scan" index.
                    full = result;
                }
            }
        }

        if (full == null) {
            // Okay, no full scan needed.
            return subResults;
        }

        List<IndexedQueryAnalyzer<S>.Result> mergedResults =
            new ArrayList<IndexedQueryAnalyzer<S>.Result>();

        for (IndexedQueryAnalyzer<S>.Result result : subResults) {
            if (result == full) {
                // Add after everything has been merged into it.
                continue;
            }

            boolean exempt = result.getCompositeScore().getFilteringScore().hasAnyMatches();

            if (exempt) {
                // Must also have a join in the filter to be exempt.
                List<PropertyFilter<S>> subFilters = PropertyFilterList.get(result.getFilter());

                joinCheck: {
                    for (PropertyFilter<S> subFilter : subFilters) {
                        if (subFilter.getChainedProperty().getChainCount() > 0) {
                            // A chain implies a join was followed, so result is exempt.
                            break joinCheck;
                        }
                    }
                    // No joins found, result is not exempt from merging into full scan.
                    exempt = false;
                }
            }

            if (exempt) {
                mergedResults.add(result);
            } else {
                full = full.mergeRemainderFilter(result.getFilter());
            }
        }

        if (mergedResults.size() == 0) {
            // Nothing was exempt. Rather than return a result with a dnf
            // filter, return full scan with a simpler reduced filter.
            full = full.withRemainderFilter(filter.reduce());
        }

        mergedResults.add(full);

        return mergedResults;
    }

    public class Result {
        private final List<IndexedQueryAnalyzer<S>.Result> mSubResults;
        private final OrderingList<S> mTotalOrdering;

        Result(List<IndexedQueryAnalyzer<S>.Result> subResults) {
            this(subResults, null);
        }

        Result(List<IndexedQueryAnalyzer<S>.Result> subResults, OrderingList<S> totalOrdering) {
            if (subResults.size() < 1) {
                throw new IllegalArgumentException();
            }
            mSubResults = Collections.unmodifiableList(subResults);
            mTotalOrdering = totalOrdering;
        }

        /**
         * Returns results for each sub-query to be executed in the union. If
         * only one result is returned, then no union needs to be performed.
         */
        public List<IndexedQueryAnalyzer<S>.Result> getSubResults() {
            return mSubResults;
        }

        /**
         * Returns a total ordering, if one was imposed. Otherwise, null is returned.
         */
        public OrderingList<S> getTotalOrdering() {
            return mTotalOrdering;
        }

        /**
         * Creates a QueryExecutor based on this result.
         */
        public QueryExecutor<S> createExecutor()
            throws SupportException, FetchException, RepositoryException
        {
            List<IndexedQueryAnalyzer<S>.Result> subResults = getSubResults();
            int size = subResults.size();

            if (size == 1) {
                return subResults.get(0).createExecutor();
            }

            List<QueryExecutor<S>> executors = new ArrayList<QueryExecutor<S>>(size);
            for (int i=0; i<size; i++) {
                executors.add(subResults.get(i).createExecutor());
            }

            return new UnionQueryExecutor<S>(executors, mTotalOrdering);
        }
    }

    /**
     * Used to track which property direction is most popular.
     */    
    private class Tally implements Comparable<Tally> {
        private final ChainedProperty<S> mProperty;

        private int mAscendingCount;
        private int mDescendingCount;

        Tally(ChainedProperty<S> property) {
            mProperty = property;
        }

        ChainedProperty<S> getProperty() {
            return mProperty;
        }

        void increment(Direction dir) {
            switch (dir) {
            case UNSPECIFIED:
                mAscendingCount++;
                mDescendingCount++;
                break;

            case ASCENDING:
                mAscendingCount++;
                break;

            case DESCENDING:
                mDescendingCount++;
                break;
            }
        }

        /**
         * Only returns ASCENDING or DESCENDING.
         */
        Direction getBestDirection() {
            if (mAscendingCount >= mDescendingCount) {
                return Direction.ASCENDING;
            }
            return Direction.DESCENDING;
        }

        int getBestCount() {
            if (mAscendingCount >= mDescendingCount) {
                return mAscendingCount;
            }
            return mDescendingCount;
        }

        void clear() {
            mAscendingCount = 0;
            mDescendingCount = 0;
        }

        /**
         * Returns -1 if this tally is worse.
         */
        public int compareTo(Tally other) {
            int thisBest = getBestCount();
            int otherBest = other.getBestCount();
            if (thisBest < otherBest) {
                return -1;
            }
            if (thisBest > otherBest) {
                return 1;
            }
            return 0;
        }

        @Override
        public String toString() {
            return "Tally: {property=" + mProperty +
                ", asc=" + mAscendingCount +
                ", desc=" + mDescendingCount +
                '}';
        }
    }

    /**
     * Analyzes a disjunctive normal filter into sub-results over filters that
     * only contain 'and' operations.
     */
    private class Splitter extends Visitor<S, RepositoryException, Object> {
        private final OrderingList<S> mOrdering;
        private final QueryHints mHints;

        final List<IndexedQueryAnalyzer<S>.Result> mSubResults;

        Splitter(OrderingList<S> ordering, QueryHints hints) {
            mOrdering = ordering;
            mHints = hints;
            mSubResults = new ArrayList<IndexedQueryAnalyzer<S>.Result>();
        }

        @Override
        public RepositoryException visit(OrFilter<S> filter, Object param) {
            try {
                Filter<S> left = filter.getLeftFilter();
                if (!(left instanceof OrFilter)) {
                    subAnalyze(left);
                } else {
                    RepositoryException e = left.accept(this, param);
                    if (e != null) {
                        return e;
                    }
                }
                Filter<S> right = filter.getRightFilter();
                if (!(right instanceof OrFilter)) {
                    subAnalyze(right);
                } else {
                    RepositoryException e = right.accept(this, param);
                    if (e != null) {
                        return e;
                    }
                }
                return null;
            } catch (RepositoryException e) {
                return e;
            }
        }

        // This method should only be called if root filter has no 'or' operators.
        @Override
        public RepositoryException visit(AndFilter<S> filter, Object param) {
            try {
                subAnalyze(filter);
                return null;
            } catch (RepositoryException e) {
                return e;
            }
        }

        // This method should only be called if root filter has no logical operators.
        @Override
        public RepositoryException visit(PropertyFilter<S> filter, Object param) {
            try {
                subAnalyze(filter);
                return null;
            } catch (RepositoryException e) {
                return e;
            }
        }

        // This method should only be called if root filter has no logical operators.
        @Override
        public RepositoryException visit(ExistsFilter<S> filter, Object param) {
            try {
                subAnalyze(filter);
                return null;
            } catch (RepositoryException e) {
                return e;
            }
        }

        private void subAnalyze(Filter<S> subFilter) throws SupportException, RepositoryException {
            IndexedQueryAnalyzer<S>.Result subResult =
                mIndexAnalyzer.analyze(subFilter, mOrdering, mHints);

            // Rather than blindly add to mSubResults, try to merge with
            // another result. This in turn reduces the number of cursors
            // needed by the union.

            int size = mSubResults.size();
            for (int i=0; i<size; i++) {
                IndexedQueryAnalyzer<S>.Result existing = mSubResults.get(i);
                if (existing.canMergeRemainder(subResult)) {
                    mSubResults.set(i, existing.mergeRemainder(subResult));
                    return;
                }
            }

            // Couldn't merge, so add a new entry.
            mSubResults.add(subResult);
        }
    }
}
