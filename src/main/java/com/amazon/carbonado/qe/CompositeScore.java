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
     * Returns a partial comparator suited for comparing local indexes to
     * foreign indexes. It determines which CompositeScores are better by
     * examining identity matches, range matches and ordering. It does not
     * matter if the scores were evaluated for different indexes or storable
     * types. The comparator returns {@code <0} if first score is better,
     * {@code 0} if equal, or {@code >0} if second is better.
     */
    public static Comparator<CompositeScore<?>> localForeignComparator() {
        return localForeignComparator(null);
    }

    /**
     * Returns a partial comparator suited for comparing local indexes to
     * foreign indexes. It determines which CompositeScores are better by
     * examining identity matches, range matches and ordering. It does not
     * matter if the scores were evaluated for different indexes or storable
     * types. The comparator returns {@code <0} if first score is better,
     * {@code 0} if equal, or {@code >0} if second is better.
     *
     * @param hints optional hints
     */
    public static Comparator<CompositeScore<?>> localForeignComparator(QueryHints hints) {
        if (hints != null && hints.contains(QueryHint.CONSUME_SLICE)) {
            return Comp.LOCAL_FOREIGN_SLICE;
        }
        return Comp.LOCAL_FOREIGN;
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
        return fullComparator(null);
    }

    /**
     * Returns a comparator which determines which CompositeScores are
     * better. It compares identity matches, range matches, ordering, open
     * range matches, property arrangement and index cost estimate. It does not
     * matter if the scores were evaluated for different indexes or storable
     * types. The comparator returns {@code <0} if first score is better,
     * {@code 0} if equal, or {@code >0} if second is better.
     *
     * @param hints optional hints
     */
    public static Comparator<CompositeScore<?>> fullComparator(QueryHints hints) {
        if (hints != null && hints.contains(QueryHint.CONSUME_SLICE)) {
            return Comp.SLICE;
        }
        return Comp.FULL;
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

    /**
     * Returns a new CompositeScore with the filtering remainder replaced and
     * covering matches recalculated. Other matches are not recalculated.
     *
     * @since 1.2
     */
    public CompositeScore<S> withRemainderFilter(Filter<S> filter) {
        return new CompositeScore<S>(mFilteringScore.withRemainderFilter(filter),
                                     mOrderingScore);
    }

    /**
     * Returns a new CompositeScore with the ordering remainder
     * replaced. Handled count is not recalculated.
     *
     * @since 1.2
     */
    public CompositeScore<S> withRemainderOrdering(OrderingList<S> ordering) {
        return new CompositeScore<S>(mFilteringScore,
                                     mOrderingScore.withRemainderOrdering(ordering));
    }

    @Override
    public String toString() {
        return "CompositeScore {" + getFilteringScore() + ", " + getOrderingScore() + '}';
    }

    private static class Comp implements Comparator<CompositeScore<?>> {
        static final Comparator<CompositeScore<?>> LOCAL_FOREIGN = new Comp(false, false);
        static final Comparator<CompositeScore<?>> SLICE = new Comp(true, true);
        static final Comparator<CompositeScore<?>> LOCAL_FOREIGN_SLICE = new Comp(false, true);
        static final Comparator<CompositeScore<?>> FULL = new Comp(true, false);

        private final boolean mFull;
        private final boolean mSlice;

        private Comp(boolean full, boolean slice) {
            mFull = full;
            mSlice = slice;
        }

        public int compare(CompositeScore<?> first, CompositeScore<?> second) {
            int result = FilteringScore.nullCompare(first, second);
            if (result != 0) {
                return result;
            }

            FilteringScore<?> firstScore = first.getFilteringScore();
            FilteringScore<?> secondScore = second.getFilteringScore();

            result = FilteringScore.rangeComparator().compare(firstScore, secondScore);

            OrderingScore<?> firstOrderingScore = first.getOrderingScore();
            OrderingScore<?> secondOrderingScore = second.getOrderingScore();
                
            if (result != 0) {
                if (!firstScore.hasAnyMatches() || !secondScore.hasAnyMatches()) {
                    // Return result if either index filters nothing.
                    return result;
                }

                // negative: first is better, zero: same, positive: second is better
                int handledScore =
                    secondOrderingScore.getHandledCount() - firstOrderingScore.getHandledCount();

                if (handledScore == 0) {
                    // Neither index handles ordering any better, so don't
                    // bother examining that.
                    return result;
                }

                if (Integer.signum(result) == Integer.signum(handledScore)) {
                    // Index is better at both filtering and ordering. A double win.
                    return result;
                }

                // This is a tough call. Both indexes perform some filtering,
                // but one index is clearly better at it. The other index is
                // clearly better for ordering, however. Without knowing how
                // many results can be filtered out, it isn't possible to
                // decide which index is better. Let the user decide in this
                // case, by examing the preference order of filter properties.

                int preferenceResult =
                    secondScore.getPreferenceScore().compareTo(firstScore.getPreferenceScore());
                if (preferenceResult != 0) {
                    return preferenceResult;
                }

                // Okay, preference is not helping. If handled filter count is
                // the same, choose the better ordering. Why? Most likely a nearly
                // identical index was created specifically for ordering. One index
                // might look better for filtering just because it is clustered.

                if (firstScore.getHandledCount() == secondScore.getHandledCount()) {
                    if (handledScore != 0) {
                        return handledScore;
                    }
                }

                // Just return the result for the better filtering index.
                return result;
            }

            // If this point is reached, the filtering score has not been
            // terribly helpful in deciding an index. Check the ordering score.

            boolean comparedOrdering = false;
            if (considerOrdering(firstScore) && considerOrdering(secondScore)) {
                // Only consider ordering if slice mode, or if index is fast
                // (clustered) or if index is used for any significant
                // filtering. A full scan of an index just to get desired
                // ordering can be very expensive due to random access I/O. A
                // sort operation is often faster.

                result = OrderingScore.fullComparator()
                    .compare(first.getOrderingScore(), second.getOrderingScore());
                comparedOrdering = true;

                if (result != 0) {
                    return result;
                }
            }

            if (!mFull) {
                // Favor index that has any matches.
                if (firstScore.hasAnyMatches()) {
                    if (!secondScore.hasAnyMatches()) {
                        return -1;
                    }
                } else if (secondScore.hasAnyMatches()) {
                    return 1;
                }
                return 0;
            }

            // Additional tests for full comparator.

            result = FilteringScore.fullComparator().compare(firstScore, secondScore);

            if (result != 0) {
                return result;
            }

            // Still no idea which is best. Compare ordering if not already
            // done so.

            if (!comparedOrdering) {
                result = OrderingScore.fullComparator()
                    .compare(first.getOrderingScore(), second.getOrderingScore());
                comparedOrdering = true;

                if (result != 0) {
                    return result;
                }
            }

            // Finally, just favor index with fewer properties, under the
            // assumption that fewer properties means smaller sized records
            // that need to be read in.
            if (firstScore.getIndexPropertyCount() < secondScore.getIndexPropertyCount()) {
                return -1;
            } else if (firstScore.getIndexPropertyCount() > secondScore.getIndexPropertyCount()) {
                return 1;
            }

            return result;
        }

        private boolean considerOrdering(FilteringScore<?> score) {
            return mSlice
                || score.isIndexClustered()
                || score.getIdentityCount() > 0
                || score.hasRangeMatch();
        }
    }
}
