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

import java.util.Comparator;
import java.util.Collections;
import java.util.List;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;

/**
 * Evaluates an index for how well it matches a query's desired filtering and
 * ordering. A composite score is not a single absolute value \u2013 instead it
 * has a relative weight when compared to other scores.
 *
 * @author Brian S O'Neill
 * @see FilteringScore
 * @see OrderingScore
 */
public class CompositeScore<S extends Storable> {
    /**
     * Evaluates the given index for its filtering and ordering capabilities
     * against the given filter and order-by properties.
     *
     * @param index index to evaluate
     * @param filter optional filter which cannot contain any logical 'or' operations.
     * @param ordering optional properties which define desired ordering
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> CompositeScore<S> evaluate
        (StorableIndex<S> index,
         Filter<S> filter,
         OrderingList<S> ordering)
    {
        if (index == null) {
            throw new IllegalArgumentException("Index required");
        }

        return evaluate(index.getOrderedProperties(),
                        index.isUnique(),
                        index.isClustered(),
                        filter,
                        ordering);
    }

    /**
     * Evaluates the given index properties for its filtering and ordering
     * capabilities against the given filter and order-by properties.
     *
     * @param indexProperties index properties to evaluate
     * @param unique true if index is unique
     * @param clustered true if index is clustered
     * @param filter optional filter which cannot contain any logical 'or' operations.
     * @param ordering optional properties which define desired ordering
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> CompositeScore<S> evaluate
        (OrderedProperty<S>[] indexProperties,
         boolean unique,
         boolean clustered,
         Filter<S> filter,
         OrderingList<S> ordering)
    {
        FilteringScore<S> filteringScore = FilteringScore
            .evaluate(indexProperties, unique, clustered, filter);

        OrderingScore<S> orderingScore = OrderingScore
            .evaluate(indexProperties, unique, clustered, filter, ordering);

        return new CompositeScore<S>(filteringScore, orderingScore);
    }

    /**
     * Returns a comparator which determines which CompositeScores are
     * better. It compares identity matches, range matches, ordering, open
     * range matches, property arrangement and index cost estimate. It does not
     * matter if the scores were evaluated for different indexes or storable
     * types. The comparator returns {@code <0} if first score is better,
     * {@code 0} if equal, or {@code >0} if second is better.
     */
    public static Comparator<CompositeScore<?>> fullComparator() {
        return Full.INSTANCE;
    }

    private final FilteringScore<S> mFilteringScore;
    private final OrderingScore<S> mOrderingScore;

    private CompositeScore(FilteringScore<S> filteringScore, OrderingScore<S> orderingScore) {
        mFilteringScore = filteringScore;
        mOrderingScore = orderingScore;
    }

    /**
     * Returns the score on how well the evaluated index performs the desired
     * filtering.
     */
    public FilteringScore<S> getFilteringScore() {
        return mFilteringScore;
    }

    /**
     * Returns the score on how well the evaluated index performs the desired
     * ordering.
     */
    public OrderingScore<S> getOrderingScore() {
        return mOrderingScore;
    }

    /**
     * Returns true if the filtering score can merge its remainder filter and
     * the ordering score can merge its remainder orderings.
     */
    public boolean canMergeRemainder(CompositeScore<S> other) {
        return getFilteringScore().canMergeRemainderFilter(other.getFilteringScore())
            && getOrderingScore().canMergeRemainderOrdering(other.getOrderingScore());
    }

    /**
     * Merges the remainder filter of this score with the one given using an
     * 'or' operation. Call canMergeRemainder first to verify if the merge
     * makes any sense.
     */
    public Filter<S> mergeRemainderFilter(CompositeScore<S> other) {
        return getFilteringScore().mergeRemainderFilter(other.getFilteringScore());
    }

    /**
     * Merges the remainder orderings of this score with the one given. Call
     * canMergeRemainder first to verify if the merge makes any sense.
     */
    public OrderingList<S> mergeRemainderOrdering(CompositeScore<S> other) {
        return getOrderingScore().mergeRemainderOrdering(other.getOrderingScore());
    }

    public String toString() {
        return "CompositeScore {" + getFilteringScore() + ", " + getOrderingScore() + '}';
    }

    private static class Full implements Comparator<CompositeScore<?>> {
        static final Comparator<CompositeScore<?>> INSTANCE = new Full();

        public int compare(CompositeScore<?> first, CompositeScore<?> second) {
            int result = FilteringScore.nullCompare(first, second);
            if (result != 0) {
                return result;
            }

            result = FilteringScore.rangeComparator()
                .compare(first.getFilteringScore(), second.getFilteringScore());

            if (result != 0) {
                return result;
            }

            result = OrderingScore.fullComparator()
                .compare(first.getOrderingScore(), second.getOrderingScore());

            if (result != 0) {
                return result;
            }

            result = FilteringScore.fullComparator()
                .compare(first.getFilteringScore(), second.getFilteringScore());

            return result;
        }
    }
}
