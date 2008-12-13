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

import java.math.BigInteger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.List;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;

/**
 * Evaluates an index for how well it matches a query's desired filtering. A
 * filtering score is not a single absolute value \u2013 instead it has a
 * relative weight when compared to other scores.
 *
 * <p>An index matches a desired filtering if the arrangement of properties and
 * its relational operator matches. A matching {@link RelOp#EQ =} operator is
 * an identity match. A range match is determined by a matching operator of
 * {@link RelOp#GT >}, {@link RelOp#GE >=}, {@link RelOp#LT <}, or {@link
 * RelOp#LE <=}. Filters with a {@link RelOp#NE !=} operator are
 * ignored. Although not all index properties need to be used, the first must
 * be and no gaps are allowed.
 *
 * <p>A FilteringScore measures the number of filter properties that are
 * matched and the number that are remaining. If there are remainder
 * properties, then the user of the evaluated index will need to perform an
 * additional filtering operation to achieve the desired results.
 *
 * <p>In general, a FilteringScore is better than another if it has more
 * matched properties and fewer remainder properties. Matching more identity
 * properties is given preference over matching range properties. Index
 * clustering is also considered for score comparison.
 *
 * @author Brian S O'Neill
 * @see OrderingScore
 * @see CompositeScore
 */
public class FilteringScore<S extends Storable> {
    /**
     * Evaluates the given index for its filtering capabilities against the
     * given filter.
     *
     * @param index index to evaluate
     * @param filter filter which cannot contain any logical 'or' operations.
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> FilteringScore<S> evaluate(StorableIndex<S> index,
                                                                  Filter<S> filter)
    {
        if (index == null) {
            throw new IllegalArgumentException("Index required");
        }

        return evaluate(index.getOrderedProperties(),
                        index.isUnique(),
                        index.isClustered(),
                        filter);
    }

    /**
     * Evaluates the given index properties for its filtering capabilities
     * against the given filter.
     *
     * @param indexProperties index properties to evaluate
     * @param unique true if index is unique
     * @param clustered true if index is clustered
     * @param filter filter which cannot contain any logical 'or' operations.
     * @throws IllegalArgumentException if index is null or filter is not supported
     */
    public static <S extends Storable> FilteringScore<S> evaluate
        (OrderedProperty<S>[] indexProperties,
         boolean unique,
         boolean clustered,
         Filter<S> filter)
    {
        if (indexProperties == null) {
            throw new IllegalArgumentException("Index properties required");
        }

        // List is ordered such that '=' operations are first and '!='
        // operations are last.
        PropertyFilterList<S> originalFilterList = PropertyFilterList.get(filter);

        // Copy it so that matching elements can be removed.
        List<PropertyFilter<S>> filterList = new ArrayList<PropertyFilter<S>>(originalFilterList);

        // First find the identity matches.

        List<PropertyFilter<S>> identityFilters = new ArrayList<PropertyFilter<S>>();
        int arrangementScore = 0;
        BigInteger preferenceScore = BigInteger.ZERO;

        int indexPropPos;
        int lastFilterPos = 0;

        identityMatches:
        for (indexPropPos = 0; indexPropPos < indexProperties.length; indexPropPos++) {
            ChainedProperty<S> indexProp = indexProperties[indexPropPos].getChainedProperty();

            for (int pos = 0; pos < filterList.size(); pos++) {
                PropertyFilter<S> subFilter = filterList.get(pos);
                if (subFilter.getOperator() != RelOp.EQ) {
                    // No more '=' operators will be seen so short-circuit.
                    break identityMatches;
                }
                if (subFilter.getChainedProperty().equals(indexProp)) {
                    identityFilters.add(subFilter);
                    int shift = originalFilterList.size()
                        - originalFilterList.getOriginalPosition(subFilter) - 1;
                    preferenceScore = preferenceScore.or(BigInteger.ONE.shiftLeft(shift));
                    if (pos >= lastFilterPos) {
                        arrangementScore++;
                    }
                    filterList.remove(pos);
                    lastFilterPos = pos;
                    continue identityMatches;
                }
            }

            // Consecutive index property not used, so stop searching for identity matches.
            break identityMatches;
        }

        // Index property following identity matches can only be used for
        // supporting range matches. Multiple filters may match, but their
        // property must be the same as the index property.

        List<PropertyFilter<S>> rangeStartFilters;
        List<PropertyFilter<S>> rangeEndFilters;

        boolean shouldReverseRange;

        if (indexPropPos >= indexProperties.length) {
            rangeStartFilters = Collections.emptyList();
            rangeEndFilters = Collections.emptyList();
            shouldReverseRange = false;
        } else {
            ChainedProperty<S> indexProp = indexProperties[indexPropPos].getChainedProperty();

            rangeStartFilters = new ArrayList<PropertyFilter<S>>();
            rangeEndFilters = new ArrayList<PropertyFilter<S>>();

            rangeMatches:
            for (int pos = 0; pos < filterList.size(); pos++) {
                PropertyFilter<S> subFilter = filterList.get(pos);
                RelOp op = subFilter.getOperator();

                switch (op) {
                case NE:
                    // No more range operators will be seen so short-circuit.
                    break rangeMatches;

                case GT: case GE: case LT: case LE:
                    if (subFilter.getChainedProperty().equals(indexProp)) {
                        switch (op) {
                        case GT: case GE:
                            rangeStartFilters.add(subFilter);
                            break;
                        default:
                            rangeEndFilters.add(subFilter);
                            break;
                        }

                        filterList.remove(pos);

                        int shift = originalFilterList.size()
                            - originalFilterList.getOriginalPosition(subFilter) - 1;
                        preferenceScore = preferenceScore.or(BigInteger.ONE.shiftLeft(shift));

                        // Loop correction after removing element.
                        pos--;
                    }
                    break;
                }
            }

            shouldReverseRange = (rangeStartFilters.size() > 0 || rangeEndFilters.size() > 0) &&
                indexProperties[indexPropPos].getDirection() == Direction.DESCENDING;
        }

        List<? extends Filter<S>> remainderFilters;
        if (originalFilterList.getExistsFilters().size() == 0) {
            remainderFilters = filterList;
        } else {
            remainderFilters = new ArrayList<Filter<S>>(filterList);
            // Java "generics" suck. Note the stupid cast to make this work.
            ((List) remainderFilters).addAll(originalFilterList.getExistsFilters());
        }

        return new FilteringScore<S>(indexProperties,
                                     clustered,
                                     unique,
                                     identityFilters,
                                     rangeStartFilters,
                                     rangeEndFilters,
                                     arrangementScore,
                                     preferenceScore,
                                     remainderFilters,
                                     shouldReverseRange);
    }

    /**
     * Returns a partial comparator which determines which FilteringScores are
     * better by examining only identity and range matches. It does not matter
     * if the scores were evaluated for different indexes or storable
     * types. The comparator returns {@code <0} if first score is better,
     * {@code 0} if equal, or {@code >0} if second is better.
     */
    public static Comparator<FilteringScore<?>> rangeComparator() {
        return Range.INSTANCE;
    }

    /**
     * Returns a comparator which determines which FilteringScores are
     * better. It compares identity matches, range matches, open range matches
     * and property arrangement. It does not matter if the scores were
     * evaluated for different indexes or storable types. The comparator
     * returns {@code <0} if first score is better, {@code 0} if equal, or
     * {@code >0} if second is better.
     */
    public static Comparator<FilteringScore<?>> fullComparator() {
        return Full.INSTANCE;
    }

    static <E> List<E> prepareList(List<E> list) {
        if (list == null || list.size() == 0) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * Comparison orders null high.
     */
    static int nullCompare(Object first, Object second) {
        if (first == null) {
            if (second != null) {
                return 1;
            }
        } else if (second == null) {
            return -1;
        }
        return 0;
    }

    /**
     * Splits the filter from its conjunctive normal form. And'ng the filters
     * together produces the original filter.
     */
    static <S extends Storable> List<Filter<S>> split(Filter<S> filter) {
        return filter == null ? null : filter.conjunctiveNormalFormSplit();
    }

    private final OrderedProperty<S>[] mIndexProperties;
    private final boolean mIndexClustered;
    private final boolean mIndexUnique;

    private final List<PropertyFilter<S>> mIdentityFilters;

    private final List<PropertyFilter<S>> mRangeStartFilters;
    private final List<PropertyFilter<S>> mRangeEndFilters;

    private final int mArrangementScore;
    private final BigInteger mPreferenceScore;

    private final List<? extends Filter<S>> mRemainderFilters;
    private final List<? extends Filter<S>> mCoveringFilters;

    private final boolean mShouldReverseRange;

    private transient Filter<S> mIdentityFilter;
    private transient Filter<S> mRemainderFilter;
    private transient Filter<S> mCoveringFilter;
    private transient Filter<S> mCoveringRemainderFilter;

    private FilteringScore(OrderedProperty<S>[] indexProperties,
                           boolean indexClustered,
                           boolean indexUnique,
                           List<PropertyFilter<S>> identityFilters,
                           List<PropertyFilter<S>> rangeStartFilters,
                           List<PropertyFilter<S>> rangeEndFilters,
                           int arrangementScore,
                           BigInteger preferenceScore,
                           List<? extends Filter<S>> remainderFilters,
                           boolean shouldReverseRange)
    {
        mIndexProperties = indexProperties;
        mIndexClustered = indexClustered;
        mIndexUnique = indexUnique;
        mIdentityFilters = prepareList(identityFilters);
        mRangeStartFilters = prepareList(rangeStartFilters);
        mRangeEndFilters = prepareList(rangeEndFilters);
        mArrangementScore = arrangementScore;
        mPreferenceScore = preferenceScore;
        mRemainderFilters = prepareList(remainderFilters);
        mShouldReverseRange = shouldReverseRange;

        mCoveringFilters = findCoveringMatches();
    }

    private FilteringScore(FilteringScore<S> score, Filter<S> remainderFilter) {
        mIndexProperties = score.mIndexProperties;
        mIndexClustered = score.mIndexClustered;
        mIndexUnique = score.mIndexUnique;
        mIdentityFilters = score.mIdentityFilters;
        mRangeStartFilters = score.mRangeStartFilters;
        mRangeEndFilters = score.mRangeEndFilters;
        mArrangementScore = score.mArrangementScore;
        mPreferenceScore = score.mPreferenceScore;
        mRemainderFilters = prepareList(split(remainderFilter));
        mShouldReverseRange = score.mShouldReverseRange;

        mCoveringFilters = findCoveringMatches();

        mIdentityFilter = score.mIdentityFilter;
        mRemainderFilter = remainderFilter;
    }

    /**
     * Returns true if evaluated index is clustered. Scans of clustered indexes
     * are generally faster.
     */
    public boolean isIndexClustered() {
        return mIndexClustered;
    }

    /**
     * Returns true if evaluated index is unique.
     */
    public boolean isIndexUnique() {
        return mIndexUnique;
    }

    /**
     * Returns the amount of properties in the evaluated index.
     */
    public int getIndexPropertyCount() {
        return mIndexProperties.length;
    }

    /**
     * Returns number of consecutive left-aligned index properties which match
     * property filters with an operator of {@link RelOp#EQ}.
     */
    public int getIdentityCount() {
        return mIdentityFilters.size();
    }

    /**
     * Returns the identity property filters supported by the evaluated
     * index. The order of the list matches the order in which the properties
     * appear in the index. The operator of each filter is {@link RelOp#EQ}.
     */
    public List<PropertyFilter<S>> getIdentityFilters() {
        return mIdentityFilters;
    }

    /**
     * Returns the composite identity filter, or null if no identity property
     * filters.
     */
    public Filter<S> getIdentityFilter() {
        if (mIdentityFilter == null) {
            mIdentityFilter = buildCompositeFilter(getIdentityFilters());
        }
        return mIdentityFilter;
    }

    /**
     * Returns true if any property filter with an operator of {@link RelOp#GT}
     * or {@link RelOp#GE} matches an index property. The index property used
     * for the range is the first one following the identity count.
     */
    public boolean hasRangeStart() {
        return mRangeStartFilters.size() > 0;
    }

    /**
     * Returns the range start property filters supported by the evaluated
     * index. The operator of each filter is either {@link RelOp#GT} or {@link
     * RelOp#GE}. The property of each filter is identical, and the properties
     * are also identical to any range end filters.
     */
    public List<PropertyFilter<S>> getRangeStartFilters() {
        return mRangeStartFilters;
    }

    /**
     * Returns the range start property filters supported by the evaluated
     * index whose operator is only {@link RelOp#GT}. This list is a subset of
     * those returned by {@link #getRangeStartFilters}.
     */
    public List<PropertyFilter<S>> getExclusiveRangeStartFilters() {
        return reduce(getRangeStartFilters(), RelOp.GT);
    }

    /**
     * Returns the range start property filters supported by the evaluated
     * index whose operator is only {@link RelOp#GE}. This list is a subset of
     * those returned by {@link #getRangeStartFilters}.
     */
    public List<PropertyFilter<S>> getInclusiveRangeStartFilters() {
        return reduce(getRangeStartFilters(), RelOp.GE);
    }

    /**
     * Returns true if any property filter with an operator of {@link RelOp#LT}
     * or {@link RelOp#LE} matches an index property. The index property used
     * for the range is the first one following the identity count.
     */
    public boolean hasRangeEnd() {
        return mRangeEndFilters.size() > 0;
    }

    /**
     * Returns the range end property filters supported by the evaluated
     * index. The operator of each filter is either {@link RelOp#LT} or {@link
     * RelOp#LE}. The property of each filter is identical, and the properties
     * are also identical to any range start filters.
     */
    public List<PropertyFilter<S>> getRangeEndFilters() {
        return mRangeEndFilters;
    }

    /**
     * Returns the range end property filters supported by the evaluated
     * index whose operator is only {@link RelOp#LT}. This list is a subset of
     * those returned by {@link #getRangeEndFilters}.
     */
    public List<PropertyFilter<S>> getExclusiveRangeEndFilters() {
        return reduce(getRangeEndFilters(), RelOp.LT);
    }

    /**
     * Returns the range end property filters supported by the evaluated
     * index whose operator is only {@link RelOp#LE}. This list is a subset of
     * those returned by {@link #getRangeEndFilters}.
     */
    public List<PropertyFilter<S>> getInclusiveRangeEndFilters() {
        return reduce(getRangeEndFilters(), RelOp.LE);
    }

    /**
     * Returns the count of all handled property filters.
     */
    public int getHandledCount() {
        return getIdentityCount() + mRangeStartFilters.size() + mRangeEndFilters.size();
    }

    /**
     * Returns the composite handled filter, or null if no matches at all.
     */
    public Filter<S> getHandledFilter() {
        Filter<S> identity = getIdentityFilter();
        Filter<S> rangeStart = buildCompositeFilter(getRangeStartFilters());
        Filter<S> rangeEnd = buildCompositeFilter(getRangeEndFilters());

        return and(and(identity, rangeStart), rangeEnd);
    }

    private Filter<S> and(Filter<S> a, Filter<S> b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.and(b);
    }

    /**
     * Returns true if there is both a range start and range end.
     */
    public boolean hasRangeMatch() {
        return hasRangeStart() && hasRangeEnd();
    }

    /**
     * Returns true if the identity count is greater than zero or if there is a
     * range match.
     */
    public boolean hasAnyMatches() {
        return getIdentityCount() > 0 || hasRangeStart() || hasRangeEnd();
    }

    /**
     * Returns a value which indicates how well the index property order
     * matches the property filter specification order. A higher value
     * can indicate that the index is a slightly better match.
     *
     * @return arrangement value, never negative
     */
    public int getArrangementScore() {
        return mArrangementScore;
    }

    /**
     * Returns a value which indicates user index preference, based on the
     * original ordering of elements in the filter. A higher value can
     * indicate that the index is a slightly better match.
     *
     * @return preference value which can be compared to another one
     */
    public Comparable getPreferenceScore() {
        return mPreferenceScore;
    }

    /**
     * Returns number of property filters not supported by the evaluated index.
     */
    public int getRemainderCount() {
        return mRemainderFilters.size();
    }

    /**
     * Returns the filters not supported by the evaluated index.
     */
    public List<? extends Filter<S>> getRemainderFilters() {
        return mRemainderFilters;
    }

    /**
     * Returns the composite remainder filter not supported by the evaluated
     * index, or null if no remainder.
     */
    public Filter<S> getRemainderFilter() {
        if (mRemainderFilter == null) {
            mRemainderFilter = buildCompositeFilter(getRemainderFilters());
        }
        return mRemainderFilter;
    }

    /**
     * Returns number of covering property filters which are supported by the
     * evaluated index. This count is no more than the remainder count. If
     * hasAnyMatches returns false, then the covering count is zero.
     *
     * @since 1.2
     */
    public int getCoveringCount() {
        return mCoveringFilters.size();
    }

    /**
     * Returns the covering filters which are supported by the evaluated index,
     * which is a subset of the remainder filters.
     *
     * @since 1.2
     */
    public List<? extends Filter<S>> getCoveringFilters() {
        return mCoveringFilters;
    }

    /**
     * Returns the composite covering filter supported by the evaluated index,
     * or null if the covering count is zero.
     *
     * @since 1.2
     */
    public Filter<S> getCoveringFilter() {
        if (mCoveringFilter == null) {
            mCoveringFilter = buildCompositeFilter(getCoveringFilters());
        }
        return mCoveringFilter;
    }

    /**
     * Returns the composite remainder filter without including the covering
     * filter. Returns null if no remainder.
     *
     * @since 1.2
     */
    public Filter<S> getCoveringRemainderFilter() {
        if (mCoveringRemainderFilter == null) {
            List<? extends Filter<S>> remainderFilters = mRemainderFilters;
            List<? extends Filter<S>> coveringFilters = mCoveringFilters;
            if (coveringFilters.size() < remainderFilters.size()) {
                Filter<S> composite = null;
                for (int i=0; i<remainderFilters.size(); i++) {
                    Filter<S> subFilter = remainderFilters.get(i);
                    if (!coveringFilters.contains(subFilter)) {
                        if (composite == null) {
                            composite = subFilter;
                        } else {
                            composite = composite.and(subFilter);
                        }
                    }
                }
                mCoveringRemainderFilter = composite;
            }
        }
        return mCoveringRemainderFilter;
    }

    /**
     * Returns true if evaluated index is unique and each of its properties has
     * an identity match. When index and filter are used in a query, expect at
     * most one result.
     */
    public boolean isKeyMatch() {
        return isIndexUnique() && getIndexPropertyCount() == getIdentityCount();
    }

    /**
     * Returns true if there is a range start or end match, but natural order
     * of matching property is descending.
     */
    public boolean shouldReverseRange() {
        return mShouldReverseRange;
    }

    /**
     * Returns true if the given score uses an index exactly the same as this
     * one. The only allowed differences are in the remainder filter.
     */
    public boolean canMergeRemainderFilter(FilteringScore<S> other) {
        if (this == other || (!hasAnyMatches() && !other.hasAnyMatches())) {
            return true;
        }

        return isIndexClustered() == other.isIndexClustered()
            && isIndexUnique() == other.isIndexUnique()
            && getIndexPropertyCount() == other.getIndexPropertyCount()
            && getArrangementScore() == other.getArrangementScore()
            && getPreferenceScore().equals(other.getPreferenceScore())
            && shouldReverseRange() == other.shouldReverseRange()
            // List comparisons assume identical ordering, but this is
            // not strictly required. Since the different scores likely
            // originated from the same complex filter, the ordering
            // likely matches. A set equality test is not needed.
            && getIdentityFilters().equals(other.getIdentityFilters())
            && getRangeStartFilters().equals(other.getRangeStartFilters())
            && getRangeEndFilters().equals(other.getRangeEndFilters());
    }

    /**
     * Merges the remainder filter of this score with the one given using an
     * 'or' operation. Call canMergeRemainderFilter first to verify if the
     * merge makes any sense. Returns null if no remainder filter at all.
     */
    public Filter<S> mergeRemainderFilter(FilteringScore<S> other) {
        Filter<S> thisRemainderFilter = getRemainderFilter();

        if (this == other) {
            return thisRemainderFilter;
        }

        Filter<S> otherRemainderFilter = other.getRemainderFilter();

        if (thisRemainderFilter == null) {
            return otherRemainderFilter;
        } else if (otherRemainderFilter == null) {
            return thisRemainderFilter;
        } else if (thisRemainderFilter.equals(otherRemainderFilter)) {
            return thisRemainderFilter;
        } else {
            return thisRemainderFilter.or(otherRemainderFilter);
        }
    }

    /**
     * Returns a new FilteringScore with the remainder replaced and covering
     * matches recalculated. Other matches are not recalculated.
     *
     * @since 1.2
     */
    public FilteringScore<S> withRemainderFilter(Filter<S> filter) {
        return new FilteringScore<S>(this, filter);
    }

    @Override
    public String toString() {
        return "FilteringScore {identityCount=" + getIdentityCount() +
            ", hasRangeStart=" + hasRangeStart() +
            ", hasRangeEnd=" + hasRangeEnd() +
            ", remainderCount=" + getRemainderCount() +
            ", coveringCount=" + getCoveringCount() +
            '}';
    }

    private List<PropertyFilter<S>> reduce(List<PropertyFilter<S>> filters, RelOp op) {
        List<PropertyFilter<S>> reduced = null;

        for (int i=0; i<filters.size(); i++) {
            PropertyFilter<S> filter = filters.get(i);
            if (filter.getOperator() != op) {
                if (reduced == null) {
                    reduced = new ArrayList<PropertyFilter<S>>(filters.size());
                    for (int j=0; j<i; j++) {
                        reduced.add(filters.get(j));
                    }
                }
            } else if (reduced != null) {
                reduced.add(filter);
            }
        }

        return reduced == null ? filters : reduced;
    }

    private Filter<S> buildCompositeFilter(List<? extends Filter<S>> filterList) {
        if (filterList.size() == 0) {
            return null;
        }
        Filter<S> composite = filterList.get(0);
        for (int i=1; i<filterList.size(); i++) {
            composite = composite.and(filterList.get(i));
        }
        return composite;
    }

    /**
     * Finds covering matches from the remainder.
     */
    private List<Filter<S>> findCoveringMatches() {
        List<Filter<S>> coveringFilters = null;

        boolean check = !mRemainderFilters.isEmpty()
            && (mIdentityFilters.size() > 0
                || mRangeStartFilters.size() > 0 
                || mRangeEndFilters.size() > 0);

        if (check) {
            // Any remainder property which is provided by the index is a covering match.
            for (Filter<S> subFilter : mRemainderFilters) {
                if (isProvidedByIndex(subFilter)) {
                    if (coveringFilters == null) {
                        coveringFilters = new ArrayList<Filter<S>>();
                    }
                    coveringFilters.add(subFilter);
                }
            }
        }

        return prepareList(coveringFilters);
    }

    private boolean isProvidedByIndex(Filter<S> filter) {
        Boolean result = filter.accept(new Visitor<S, Boolean, Object>() {
            @Override
            public Boolean visit(OrFilter<S> filter, Object param) {
                return filter.getLeftFilter().accept(this, param)
                    && filter.getRightFilter().accept(this, param);
            }

            @Override
            public Boolean visit(AndFilter<S> filter, Object param) {
                return filter.getLeftFilter().accept(this, param)
                    && filter.getRightFilter().accept(this, param);
            }

            @Override
            public Boolean visit(PropertyFilter<S> filter, Object param) {
                ChainedProperty<S> filterProp = filter.getChainedProperty();
                for (OrderedProperty<S> indexProp : mIndexProperties) {
                    if (indexProp.getChainedProperty().equals(filterProp)) {
                        return true;
                    }
                }
                return false;
            }
        }, null);

        return result == null ? false : result;
    }

    private static class Range implements Comparator<FilteringScore<?>> {
        static final Comparator<FilteringScore<?>> INSTANCE = new Range();

        public int compare(FilteringScore<?> first, FilteringScore<?> second) {
            if (first == second) {
                return 0;
            }

            int result = nullCompare(first, second);
            if (result != 0) {
                return result;
            }

            // Compare identity matches.
            if (first.getIdentityCount() > second.getIdentityCount()) {
                return -1;
            }
            if (first.getIdentityCount() < second.getIdentityCount()) {
                return 1;
            }

            // Compare range match. (index can have at most one range match)
            if (first.hasRangeMatch()) {
                if (second.hasRangeMatch()) {
                    // Both have range match, favor clustered index.
                    if (first.isIndexClustered()) {
                        if (!second.isIndexClustered()) {
                            return -1;
                        }
                    } else if (second.isIndexClustered()) {
                        return 1;
                    }
                } else {
                    return -1;
                }
            } else if (second.hasRangeMatch()) {
                return 1;
            }

            // If any identity matches, favor clustered index.
            if (first.getIdentityCount() > 0) {
                if (first.isIndexClustered()) {
                    if (!second.isIndexClustered()) {
                        return -1;
                    }
                } else if (second.isIndexClustered()) {
                    return 1;
                }
            }

            return 0;
        }
    }

    private static class Full implements Comparator<FilteringScore<?>> {
        static final Comparator<FilteringScore<?>> INSTANCE = new Full();

        public int compare(FilteringScore<?> first, FilteringScore<?> second) {
            int result = Range.INSTANCE.compare(first, second);
            if (result != 0) {
                return result;
            }

            // Favor index that has any matches.
            if (first.hasAnyMatches()) {
                if (!second.hasAnyMatches()) {
                    return -1;
                }
            } else if (second.hasAnyMatches()) {
                return 1;
            }

            // Favor index that best matches specified property arrangement of filter.
            if (first.getArrangementScore() > second.getArrangementScore()) {
                return -1;
            }
            if (first.getArrangementScore() < second.getArrangementScore()) {
                return 1;
            }

            // Favor clustered index.
            if (first.isIndexClustered()) {
                if (!second.isIndexClustered()) {
                    return -1;
                }
            } else if (second.isIndexClustered()) {
                return 1;
            }

            // Favor index which contains fewer remainders.
            if (first.getRemainderCount() < second.getRemainderCount()) {
                return -1;
            }
            if (first.getRemainderCount() > second.getRemainderCount()) {
                return 1;
            }

            // Favor index which contains more covering matches.
            if (first.getCoveringCount() > second.getCoveringCount()) {
                return -1;
            }
            if (first.getCoveringCount() < second.getCoveringCount()) {
                return 1;
            }

            return 0;
        }
    }
}
