/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.cursor;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.ClosedFilter;
import com.amazon.carbonado.filter.ExistsFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.OpenFilter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.Visitor;

/**
 * Optimizes a Filter such that simpler tests are performed before more complex
 * tests which may need to follow joins. This takes advantage of the
 * short-circuit logic used by FilteredCursorGenerator.
 *
 * @author Brian S O'Neill
 */
class ShortCircuitOptimizer {

    public static <S extends Storable> Filter<S> optimize(Filter<S> filter) {
        return filter.accept(new Walker<S>(), null).mNewFilter;
    }

    private static class FilterAndCost<S extends Storable> {
        final Filter<S> mNewFilter;
        final ChainedProperty<S> mExpensiveProperty;

        FilterAndCost(Filter<S> filter, ChainedProperty<S> expensive) {
            mNewFilter = filter;
            mExpensiveProperty = expensive;
        }

        /**
         * Returns -1 if this is cheaper, 0 if same, 1 if this is more expensive
         */
        int compareCost(FilterAndCost<S> other) {
            if (mExpensiveProperty == null) {
                if (other.mExpensiveProperty == null) {
                    return 0;
                }
                return -1;
            } else if (other.mExpensiveProperty == null) {
                return 1;
            }

            if (mExpensiveProperty.equals(other.mExpensiveProperty)) {
                return 0;
            }

            int result = joinCompare(mExpensiveProperty.getPrimeProperty(),
                                     other.mExpensiveProperty.getPrimeProperty());

            if (result != 0) {
                return result;
            }

            if (mExpensiveProperty.getChainCount() == 0) {
                if (other.mExpensiveProperty.getChainCount() == 0) {
                    return 0;
                }
            } else if (other.mExpensiveProperty.getChainCount() == 0) {
                return 1;
            }

            int length = Math.min(mExpensiveProperty.getChainCount(),
                                  other.mExpensiveProperty.getChainCount());

            for (int i=0; i<length; i++) {
                result = joinCompare(mExpensiveProperty.getChainedProperty(i),
                                     other.mExpensiveProperty.getChainedProperty(i));

                if (result != 0) {
                    return result;
                }
            }

            if (mExpensiveProperty.getChainCount() < other.mExpensiveProperty.getChainCount()) {
                return -1;
            }
            if (mExpensiveProperty.getChainCount() > other.mExpensiveProperty.getChainCount()) {
                return 1;
            }

            return 0;
        }

        /**
         * Returns -1 if "a" has cheaper join, 0 if same, 1 if "a" has more expensive join
         */
        private int joinCompare(StorableProperty<?> a, StorableProperty<?> b) {
            if (a.isQuery()) {
                if (b.isQuery()) {
                    return 0;
                }
                return 1;
            } else if (b.isQuery()) {
                return -1;
            }
            if (a.isJoin()) {
                if (b.isJoin()) {
                    return 0;
                }
                return 1;
            } else if (b.isJoin()) {
                return -1;
            }
            return 0;
        }
    }

    private static class Walker<S extends Storable> extends Visitor<S, FilterAndCost<S>, Object> {
        @Override
        public FilterAndCost<S> visit(OrFilter<S> filter, Object param) {
            FilterAndCost<S> leftCost = filter.getLeftFilter().accept(this, param);
            FilterAndCost<S> rightCost = filter.getRightFilter().accept(this, param);

            int compare = leftCost.compareCost(rightCost);

            Filter<S> newFilter;
            ChainedProperty<S> expensiveProperty;

            if (compare <= 0) {
                // Current arrangement is fine.
                newFilter = leftCost.mNewFilter.or(rightCost.mNewFilter);
                expensiveProperty = rightCost.mExpensiveProperty;
            } else {
                // Swap nodes such that the expensive one is checked later.
                newFilter = rightCost.mNewFilter.or(leftCost.mNewFilter);
                expensiveProperty = leftCost.mExpensiveProperty;
            }

            return new FilterAndCost<S>(newFilter, expensiveProperty);
        }

        @Override
        public FilterAndCost<S> visit(AndFilter<S> filter, Object param) {
            FilterAndCost<S> leftCost = filter.getLeftFilter().accept(this, param);
            FilterAndCost<S> rightCost = filter.getRightFilter().accept(this, param);

            int compare = leftCost.compareCost(rightCost);

            Filter<S> newFilter;
            ChainedProperty<S> expensiveProperty;

            if (compare <= 0) {
                // Current arrangement is fine.
                newFilter = leftCost.mNewFilter.and(rightCost.mNewFilter);
                expensiveProperty = rightCost.mExpensiveProperty;
            } else {
                // Swap nodes such that the expensive one is checked later.
                newFilter = rightCost.mNewFilter.and(leftCost.mNewFilter);
                expensiveProperty = leftCost.mExpensiveProperty;
            }

            return new FilterAndCost<S>(newFilter, expensiveProperty);
        }

        @Override
        public FilterAndCost<S> visit(PropertyFilter<S> filter, Object param) {
            return new FilterAndCost<S>(filter, filter.getChainedProperty());
        }

        @Override
        public FilterAndCost<S> visit(ExistsFilter<S> filter, Object param) {
            return new FilterAndCost<S>(filter, filter.getChainedProperty());
        }

        @Override
        public FilterAndCost<S> visit(OpenFilter<S> filter, Object param) {
            return new FilterAndCost<S>(filter, null);
        }

        @Override
        public FilterAndCost<S> visit(ClosedFilter<S> filter, Object param) {
            return new FilterAndCost<S>(filter, null);
        }
    }
}
