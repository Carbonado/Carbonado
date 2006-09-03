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
import java.util.Collections;
import java.util.List;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;

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
public class UnionQueryAnalyzer<S extends Storable> {
    final IndexedQueryAnalyzer<S> mIndexAnalyzer;

    /**
     * @param type type of storable being queried
     * @param indexProvider
     * @throws IllegalArgumentException if type or indexProvider is null
     */
    public UnionQueryAnalyzer(Class<S> type, IndexProvider indexProvider) {
        mIndexAnalyzer = new IndexedQueryAnalyzer<S>(type, indexProvider);
    }

    /**
     * @param filter optional filter which must be {@link Filter#isBound bound}
     */
    public Result analyze(Filter<S> filter) {
        return analyze(filter, null);
    }

    /**
     * @param filter optional filter which must be {@link Filter#isBound bound}
     * @param orderings optional properties which define desired ordering
     */
    public Result analyze(Filter<S> filter, List<OrderedProperty<S>> orderings) {
        if (!filter.isBound()) {
            // Strictly speaking, this is not required, but it detects the
            // mistake of not properly calling initialFilterValues.
            throw new IllegalArgumentException("Filter must be bound");
        }

        // Required for split to work.
        filter = filter.disjunctiveNormalForm();

        List<IndexedQueryAnalyzer<S>.Result> subResults = splitIntoSubResults(filter, orderings);

        if (subResults.size() > 1) {
            // If more than one, then a total ordering is required.

            // FIXME
        }

        return new Result(subResults);
    }

    private List<OrderedProperty<S>> isTotalOrdering() {
        // FIXME
        return null;
    }

    private List<IndexedQueryAnalyzer<S>.Result>
        splitIntoSubResults(Filter<S> filter, List<OrderedProperty<S>> orderings)
    {
        Splitter splitter = new Splitter(orderings);
        filter.accept(splitter, null);

        List<IndexedQueryAnalyzer<S>.Result> subResults = splitter.mSubResults;

        // Check if any sub-result handles nothing. If so, a full scan is the
        // best option for the entire query and all sub-results merge into a
        // single sub-result. Any sub-results which filter anything and contain
        // a join property in the filter are exempt from the merge. This is
        // because fewer joins are read then if a full scan is performed for
        // the entire query. The resulting union has both a full scan and an
        // index scan.

        IndexedQueryAnalyzer<S>.Result full = null;
        for (IndexedQueryAnalyzer<S>.Result result : subResults) {
            if (!result.handlesAnything()) {
                full = result;
                break;
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
            // FIXME: check for joins

            if (exempt) {
                subResults.add(result);
            } else {
                full = full.mergeRemainder(result.getFilter());
            }
        }

        mergedResults.add(full);

        return mergedResults;
    }

    public class Result {
        // FIXME: User of QueryAnalyzer results needs to identify what actual
        // storage is used by an index. It is also responsible for grouping
        // unions together if storage differs. If foreign index is selected,
        // then join is needed.

        private final List<IndexedQueryAnalyzer<S>.Result> mSubResults;

        Result(List<IndexedQueryAnalyzer<S>.Result> subResults) {
            mSubResults = subResults;
        }

        /**
         * Returns results for each sub-query to be executed in the union. If
         * only one result is returned, then no union needs to be performed.
         */
        public List<IndexedQueryAnalyzer<S>.Result> getSubResults() {
            return mSubResults;
        }

        /**
         * If more than one sub-result, then a total ordering is
         * imposed. Otherwise, null is returned.
         */
        public List<OrderedProperty<S>> getTotalOrdering() {
            // FIXME
            return null;
        }
    }

    /**
     * Analyzes a disjunctive normal filter into sub-results over filters that
     * only contain 'and' operations.
     */
    private class Splitter extends Visitor<S, Object, Object> {
        private final List<OrderedProperty<S>> mOrderings;

        final List<IndexedQueryAnalyzer<S>.Result> mSubResults;

        Splitter(List<OrderedProperty<S>> orderings) {
            mOrderings = orderings;
            mSubResults = new ArrayList<IndexedQueryAnalyzer<S>.Result>();
        }

        @Override
        public Object visit(OrFilter<S> filter, Object param) {
            Filter<S> left = filter.getLeftFilter();
            if (!(left instanceof OrFilter)) {
                subAnalyze(left);
            } else {
                left.accept(this, param);
            }
            Filter<S> right = filter.getRightFilter();
            if (!(right instanceof OrFilter)) {
                subAnalyze(right);
            } else {
                right.accept(this, param);
            }
            return null;
        }

        // This method should only be called if root filter has no 'or' operators.
        @Override
        public Object visit(AndFilter<S> filter, Object param) {
            subAnalyze(filter);
            return null;
        }

        // This method should only be called if root filter has no logical operators.
        @Override
        public Object visit(PropertyFilter<S> filter, Object param) {
            subAnalyze(filter);
            return null;
        }

        private void subAnalyze(Filter<S> subFilter) {
            IndexedQueryAnalyzer<S>.Result subResult =
                mIndexAnalyzer.analyze(subFilter, mOrderings);

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
