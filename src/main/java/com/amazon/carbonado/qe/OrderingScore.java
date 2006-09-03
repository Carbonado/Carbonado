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
import java.util.Comparator;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import static com.amazon.carbonado.info.Direction.*;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;

/**
 * Evaluates an index for how well it matches a query's desired ordering. An
 * ordering score is not a single absolute value \u2013 instead it has a relative
 * weight when compared to other scores.
 *
 * <p>An index matches a desired ordering if the arrangement of properties
 * matches. Not all properties of the index need to be used, however. Also,
 * gaps in the arrangement are allowed if a property identity filter
 * matches. An property identity filter is of the form {@code "a = ?"}.
 *
 * <p>An OrderingScore measures the number of ordering properties that are
 * matched and the number that are remaining. If there are remainder
 * properties, then the user of the evaluated index will need to perform a
 * post-sort operation to achieve the desired results.
 *
 * <p>In general, an OrderingScore is better than another if it has more
 * matched properties and fewer remainder properties. Index clustering,
 * property count, and natural order is also considered.
 *
 * @author Brian S O'Neill
 * @see FilteringScore
 * @see CompositeScore
 */
public class OrderingScore<S extends Storable> {
    /**
     * Evaluates the given index for its ordering capabilities against the
     * given filter and order-by properties.
     *
     * @param index index to evaluate
     * @param filter optional filter which cannot contain any logical 'or' operations.
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> OrderingScore<S> evaluate
        (StorableIndex<S> index,
         Filter<S> filter)
    {
        return evaluate(index, filter, null);
    }

    /**
     * Evaluates the given index for its ordering capabilities against the
     * given filter and order-by properties.
     *
     * @param index index to evaluate
     * @param filter optional filter which cannot contain any logical 'or' operations.
     * @param orderings properties which define desired ordering
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> OrderingScore<S> evaluate
        (StorableIndex<S> index,
         Filter<S> filter,
         List<OrderedProperty<S>> orderings)
    {
        if (index == null) {
            throw new IllegalArgumentException("Index required");
        }

        return evaluate(index.getOrderedProperties(),
                        index.isUnique(),
                        index.isClustered(),
                        filter,
                        orderings);
    }

    /**
     * Evaluates the given index properties for its ordering capabilities
     * against the given filter and order-by properties.
     *
     * @param indexProperties index properties to evaluate
     * @param unique true if index is unique
     * @param clustered true if index is clustered
     * @param filter optional filter which cannot contain any logical 'or' operations.
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> OrderingScore<S> evaluate
        (OrderedProperty<S>[] indexProperties,
         boolean unique,
         boolean clustered,
         Filter<S> filter)
    {
        return evaluate(indexProperties, unique, clustered, filter, null);
    }

    /**
     * Evaluates the given index properties for its ordering capabilities
     * against the given filter and order-by properties.
     *
     * @param indexProperties index properties to evaluate
     * @param unique true if index is unique
     * @param clustered true if index is clustered
     * @param filter optional filter which cannot contain any logical 'or' operations.
     * @param orderings properties which define desired ordering
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> OrderingScore<S> evaluate
        (OrderedProperty<S>[] indexProperties,
         boolean unique,
         boolean clustered,
         Filter<S> filter,
         List<OrderedProperty<S>> orderings)
    {
        if (indexProperties == null) {
            throw new IllegalArgumentException("Index properties required");
        }

        // Get filter list early to detect errors.
        List<PropertyFilter<S>> filterList = PropertyFilterList.get(filter);

        if (orderings == null || orderings.size() == 0) {
            return new OrderingScore<S>(clustered, indexProperties.length, null, null, false);
        }

        // Ordering properties which match identity filters don't affect order
        // results. Build up this set to find them quickly.
        Set<ChainedProperty<S>> identityPropSet =
            new HashSet<ChainedProperty<S>>(filterList.size());

        for (PropertyFilter<S> propFilter : filterList) {
            if (propFilter.getOperator() == RelOp.EQ) {
                identityPropSet.add(propFilter.getChainedProperty());
            }
        }

        // If index is unique and every property is matched by an identity
        // filter, then there won't be any handled or remainder properties.
        uniquelyCheck:
        if (unique) {
            for (int i=0; i<indexProperties.length; i++) {
                ChainedProperty<S> indexProp = indexProperties[i].getChainedProperty();
                if (!identityPropSet.contains(indexProp)) {
                    // Missed a property, so ordering is still relevent.
                    break uniquelyCheck;
                }
            }

            return new OrderingScore<S>(clustered,
                                        indexProperties.length,
                                        null,   // no handled properties
                                        null,   // no remainder properties
                                        false); // no need to reverse order
        }

        List<OrderedProperty<S>> handledProperties = new ArrayList<OrderedProperty<S>>();
        List<OrderedProperty<S>> remainderProperties = new ArrayList<OrderedProperty<S>>();

        Boolean shouldReverseOrder = null;

        Set<ChainedProperty<S>> seen = new HashSet<ChainedProperty<S>>();

        int indexPos = 0;
        calcScore:
        for (int i=0; i<orderings.size(); i++) {
            OrderedProperty<S> property = orderings.get(i);
            ChainedProperty<S> chained = property.getChainedProperty();

            if (seen.contains(chained)) {
                // Redundant property doesn't affect ordering.
                continue calcScore;
            }

            seen.add(chained);

            if (identityPropSet.contains(chained)) {
                // Doesn't affect ordering.
                continue calcScore;
            }

            indexPosMatch:
            while (indexPos < indexProperties.length) {
                OrderedProperty<S> indexProp = indexProperties[indexPos];
                ChainedProperty<S> indexChained = indexProp.getChainedProperty();

                if (chained.equals(indexChained)) {
                    if (property.getDirection() != UNSPECIFIED) {
                        Direction indexDir = indexProp.getDirection();
                        if (indexDir == UNSPECIFIED) {
                            indexDir = ASCENDING;
                        }

                        if (shouldReverseOrder == null) {
                            shouldReverseOrder = indexDir != property.getDirection();
                        } else if ((indexDir != property.getDirection()) ^ shouldReverseOrder) {
                            // Direction mismatch, so cannot be handled.
                            break indexPosMatch;
                        }
                    }

                    handledProperties.add(property);

                    indexPos++;
                    continue calcScore;
                }

                if (identityPropSet.contains(indexChained)) {
                    // Even though ordering did not match index at current
                    // position, the search for handled propertes can continue if
                    // index gap matches an identity filter.
                    indexPos++;
                    continue indexPosMatch;
                }

                // Index gap, so cannot be handled.
                break indexPosMatch;
            }

            // Property not handled and not an identity filter.
            remainderProperties.add(property);
            indexPos = Integer.MAX_VALUE;
        }

        if (shouldReverseOrder == null) {
            shouldReverseOrder = false;
        }

        return new OrderingScore<S>(clustered,
                                    indexProperties.length,
                                    handledProperties,
                                    remainderProperties,
                                    shouldReverseOrder);
    }

    /**
     * Returns a comparator which determines which OrderingScores are
     * better. It does not matter if the scores were evaluated for different
     * indexes or storable types. The comparator returns {@code <0} if first
     * score is better, {@code 0} if equal, or {@code >0} if second is better.
     */
    public static Comparator<OrderingScore<?>> fullComparator() {
        return Full.INSTANCE;
    }

    private final boolean mIndexClustered;
    private final int mIndexPropertyCount;

    private final List<OrderedProperty<S>> mHandledOrderings;
    private final List<OrderedProperty<S>> mRemainderOrderings;

    private final boolean mShouldReverseOrder;

    private OrderingScore(boolean indexClustered,
                          int indexPropertyCount,
                          List<OrderedProperty<S>> handledOrderings,
                          List<OrderedProperty<S>> remainderOrderings,
                          boolean shouldReverseOrder)
    {
        mIndexClustered = indexClustered;
        mIndexPropertyCount = indexPropertyCount;
        mHandledOrderings = FilteringScore.prepareList(handledOrderings);
        mRemainderOrderings = FilteringScore.prepareList(remainderOrderings);
        mShouldReverseOrder = shouldReverseOrder;
    }

    /**
     * Returns true if evaluated index is clustered. Scans of clustered indexes
     * are generally faster.
     */
    public boolean isIndexClustered() {
        return mIndexClustered;
    }

    /**
     * Returns the amount of properties in the evaluated index.
     */
    public int getIndexPropertyCount() {
        return mIndexPropertyCount;
    }

    /**
     * Returns the number of desired orderings the evaluated index supports.
     */
    public int getHandledCount() {
        return mHandledOrderings.size();
    }

    /**
     * Returns the ordering properties that the evaluated index supports.
     *
     * @return handled orderings, never null
     */
    public List<OrderedProperty<S>> getHandledOrderings() {
        return mHandledOrderings;
    }

    /**
     * Returns the number of desired orderings the evaluated index does not
     * support. When non-zero, a query plan which uses the evaluated index must
     * perform a sort.
     */
    public int getRemainderCount() {
        return mRemainderOrderings.size();
    }

    /**
     * Returns the ordering properties that the evaluated index does not
     * support.
     *
     * @return remainder orderings, never null
     */
    public List<OrderedProperty<S>> getRemainderOrderings() {
        return mRemainderOrderings;
    }

    /**
     * Returns true if evaluated index must be iterated in reverse to achieve
     * the desired ordering.
     */
    public boolean shouldReverseOrder() {
        return mShouldReverseOrder;
    }

    /**
     * Returns true if the given score uses an index exactly the same as this
     * one. The only allowed differences are in the count of remainder
     * orderings.
     */
    public boolean canMergeRemainderOrderings(OrderingScore<S> other) {
        if (this == other || (getHandledCount() == 0 && other.getHandledCount() == 0)) {
            return true;
        }

        if (isIndexClustered() == other.isIndexClustered()
            && getIndexPropertyCount() == other.getIndexPropertyCount()
            && shouldReverseOrder() == other.shouldReverseOrder()
            && getHandledOrderings().equals(other.getHandledOrderings()))
        {
            // The remainder orderings cannot conflict.
            List<OrderedProperty<S>> thisRemainderOrderings = getRemainderOrderings();
            List<OrderedProperty<S>> otherRemainderOrderings = other.getRemainderOrderings();

            int size = Math.min(thisRemainderOrderings.size(), otherRemainderOrderings.size());
            for (int i=0; i<size; i++) {
                if (!thisRemainderOrderings.get(i).equals(otherRemainderOrderings.get(i))) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    /**
     * Merges the remainder orderings of this score with the one given. Call
     * canMergeRemainderOrderings first to verify if the merge makes any sense.
     */
    public List<OrderedProperty<S>> mergeRemainderOrderings(OrderingScore<S> other) {
        List<OrderedProperty<S>> thisRemainderOrderings = getRemainderOrderings();

        if (this == other) {
            return thisRemainderOrderings;
        }

        List<OrderedProperty<S>> otherRemainderOrderings = other.getRemainderOrderings();

        // Choose the longer list.

        if (thisRemainderOrderings.size() == 0) {
            return otherRemainderOrderings;
        } else {
            if (otherRemainderOrderings.size() == 0) {
                return thisRemainderOrderings;
            } else if (thisRemainderOrderings.size() >= otherRemainderOrderings.size()) {
                return thisRemainderOrderings;
            } else {
                return otherRemainderOrderings;
            }
        }
    }

    public String toString() {
        return "OrderingScore {handledCount=" + getHandledCount() +
            ", remainderCount=" + getRemainderCount() +
            ", shouldReverseOrder=" + shouldReverseOrder() +
            '}';
    }

    private static class Full implements Comparator<OrderingScore<?>> {
        static final Comparator<OrderingScore<?>> INSTANCE = new Full();

        public int compare(OrderingScore<?> first, OrderingScore<?> second) {
            if (first == second) {
                return 0;
            }

            int result = FilteringScore.nullCompare(first, second);
            if (result != 0) {
                return result;
            }

            double firstRatio, otherRatio;

            {
                int total = first.getHandledCount() + first.getRemainderCount();
                firstRatio = ((double) first.getHandledCount()) / total;
            }

            {
                int total = second.getHandledCount() + second.getRemainderCount();
                otherRatio = ((double) second.getHandledCount()) / total;
            }

            // Choose index with more handled properties.
            if (firstRatio > otherRatio) {
                return -1;
            } else if (firstRatio < otherRatio) {
                return 1;
            }

            // Choose index with any handled properties over the one with
            // neither handled nor remainder properties.
            if (Double.isNaN(firstRatio)) {
                if (!Double.isNaN(otherRatio)) {
                    return 1;
                }
            } else if (Double.isNaN(otherRatio)) {
                return -1;
            }

            // Choose clustered index, under the assumption than it can be
            // scanned more quickly.
            if (first.isIndexClustered()) {
                if (!second.isIndexClustered()) {
                    return -1;
                }
            } else if (second.isIndexClustered()) {
                return 1;
            }

            // Choose index with fewer properties, under the assumption that fewer
            // properties means smaller sized records that need to be read in.
            if (first.getIndexPropertyCount() < second.getIndexPropertyCount()) {
                return -1;
            } else if (first.getIndexPropertyCount() > second.getIndexPropertyCount()) {
                return 1;
            }

            // Choose index whose natural order matches desired order.
            if (first.shouldReverseOrder()) {
                if (!second.shouldReverseOrder()) {
                    return 1;
                }
            } else if (second.shouldReverseOrder()) {
                return -1;
            }

            return 0;
        }
    }
}
