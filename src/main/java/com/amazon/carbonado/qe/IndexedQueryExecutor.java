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

import java.io.IOException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.PropertyFilter;

import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.OrderedProperty;

/**
 * QueryExecutor which utilizes an index.
 *
 * @author Brian S O'Neill
 */
public class IndexedQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    /**
     * Compares two objects which are assumed to be Comparable. If one value is
     * null, it is treated as being higher. This consistent with all other
     * property value comparisons in carbonado.
     */
    static int compareWithNullHigh(Object a, Object b) {
        return a == null ? (b == null ? 0 : -1) : (b == null ? 1 : ((Comparable) a).compareTo(b));
    }

    private final Support<S> mSupport;
    private final StorableIndex<S> mIndex;
    private final int mHandledCount;
    private final int mIdentityCount;
    private final Filter<S> mIdentityFilter;
    private final List<PropertyFilter<S>> mExclusiveRangeStartFilters;
    private final List<PropertyFilter<S>> mInclusiveRangeStartFilters;
    private final List<PropertyFilter<S>> mExclusiveRangeEndFilters;
    private final List<PropertyFilter<S>> mInclusiveRangeEndFilters;
    private final boolean mReverseOrder;
    private final boolean mReverseRange;

    /**
     * @param index index to use, which may be a primary key index
     * @param score score determines how best to utilize the index
     * @throws IllegalArgumentException if any parameter is null
     */
    public IndexedQueryExecutor(Support<S> support,
                                StorableIndex<S> index,
                                CompositeScore<S> score)
    {
        if (support == null && this instanceof Support) {
            support = (Support<S>) this;
        }
        if (support == null || index == null || score == null) {
            throw new IllegalArgumentException();
        }

        mSupport = support;
        mIndex = index;

        FilteringScore<S> fScore = score.getFilteringScore();
        OrderingScore<S> oScore = score.getOrderingScore();

        mHandledCount = oScore.getHandledCount();

        mIdentityCount = fScore.getIdentityCount();
        mIdentityFilter = fScore.getIdentityFilter();
        mExclusiveRangeStartFilters = fScore.getExclusiveRangeStartFilters();
        mInclusiveRangeStartFilters = fScore.getInclusiveRangeStartFilters();
        mExclusiveRangeEndFilters = fScore.getExclusiveRangeEndFilters();
        mInclusiveRangeEndFilters = fScore.getInclusiveRangeEndFilters();

        mReverseOrder = oScore.shouldReverseOrder();
        mReverseRange = fScore.shouldReverseRange();
    }

    @Override
    public Class<S> getStorableType() {
        // Storable type of filter may differ if index is used along with a
        // join. The type of the index is the correct storable type.
        return mIndex.getStorableType();
    }

    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        Object[] identityValues = null;
        Object rangeStartValue = null;
        Object rangeEndValue = null;
        BoundaryType rangeStartBoundary = BoundaryType.OPEN;
        BoundaryType rangeEndBoundary = BoundaryType.OPEN;

        if (values != null) {
            if (mIdentityFilter != null) {
                identityValues = values.getValuesFor(mIdentityFilter);
            }

            // In determining the proper range values and boundary types, the
            // order in which this code runs is important. The exclusive
            // filters must be checked before the inclusive filters.

            for (int i=mExclusiveRangeStartFilters.size(); --i>=0; ) {
                Object value = values.getValue(mExclusiveRangeStartFilters.get(i));
                if (rangeStartBoundary == BoundaryType.OPEN ||
                    compareWithNullHigh(value, rangeStartValue) > 0)
                {
                    rangeStartValue = value;
                    rangeStartBoundary = BoundaryType.EXCLUSIVE;
                }
            }

            for (int i=mInclusiveRangeStartFilters.size(); --i>=0; ) {
                Object value = values.getValue(mInclusiveRangeStartFilters.get(i));
                if (rangeStartBoundary == BoundaryType.OPEN ||
                    compareWithNullHigh(value, rangeStartValue) > 0)
                {
                    rangeStartValue = value;
                    rangeStartBoundary = BoundaryType.INCLUSIVE;
                }
            }

            for (int i=mExclusiveRangeEndFilters.size(); --i>=0; ) {
                Object value = values.getValue(mExclusiveRangeEndFilters.get(i));
                if (rangeEndBoundary == BoundaryType.OPEN ||
                    compareWithNullHigh(value, rangeEndValue) < 0)
                {
                    rangeEndValue = value;
                    rangeEndBoundary = BoundaryType.EXCLUSIVE;
                }
            }

            for (int i=mInclusiveRangeEndFilters.size(); --i>=0; ) {
                Object value = values.getValue(mInclusiveRangeEndFilters.get(i));
                if (rangeEndBoundary == BoundaryType.OPEN ||
                    compareWithNullHigh(value, rangeEndValue) < 0)
                {
                    rangeEndValue = value;
                    rangeEndBoundary = BoundaryType.INCLUSIVE;
                }
            }
        }

        return mSupport.fetchSubset(mIndex, identityValues,
                                    rangeStartBoundary, rangeStartValue,
                                    rangeEndBoundary, rangeEndValue,
                                    mReverseRange,
                                    mReverseOrder);
    }

    public Filter<S> getFilter() {
        Filter<S> filter = mIdentityFilter;

        for (PropertyFilter<S> p : mExclusiveRangeStartFilters) {
            filter = filter == null ? p : filter.and(p);
        }
        for (PropertyFilter<S> p : mInclusiveRangeStartFilters) {
            filter = filter == null ? p : filter.and(p);
        }
        for (PropertyFilter<S> p : mExclusiveRangeEndFilters) {
            filter = filter == null ? p : filter.and(p);
        }
        for (PropertyFilter<S> p : mInclusiveRangeEndFilters) {
            filter = filter == null ? p : filter.and(p);
        }

        if (filter == null) {
            return Filter.getOpenFilter(getStorableType());
        }
        
        return filter;
    }

    public OrderingList<S> getOrdering() {
        OrderingList<S> list;
        if (mHandledCount == 0) {
            list = OrderingList.emptyList();
        } else {
            list = OrderingList.get(mIndex.getOrderedProperties());
            if (mIdentityCount > 0) {
                list = list.subList(mIdentityCount, list.size());
            }
            if (mHandledCount < list.size()) {
                list = list.subList(0, mHandledCount);
            }
            if (mReverseOrder) {
                list = list.reverseDirections();
            }
        }
        return list;
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        indent(app, indentLevel);
        if (mReverseOrder) {
            app.append("reverse ");
        }
        if (mIndex.isClustered()) {
            app.append("clustered ");
        }
        app.append("index scan: ");
        app.append(mIndex.getStorableType().getName());
        newline(app);
        indent(app, indentLevel);
        app.append("...index: ");
        mIndex.appendTo(app);
        newline(app);
        if (mIdentityFilter != null) {
            indent(app, indentLevel);
            app.append("...identity filter: ");
            mIdentityFilter.appendTo(app, values);
            newline(app);
        }
        if (mInclusiveRangeStartFilters.size() > 0 || mExclusiveRangeStartFilters.size() > 0 ||
            mInclusiveRangeEndFilters.size() > 0 || mExclusiveRangeEndFilters.size() > 0)
        {
            indent(app, indentLevel);
            app.append("...range filter: ");
            int count = 0;
            for (PropertyFilter<S> p : mExclusiveRangeStartFilters) {
                if (count++ > 0) {
                    app.append(" & ");
                }
                p.appendTo(app, values);
            }
            for (PropertyFilter<S> p : mInclusiveRangeStartFilters) {
                if (count++ > 0) {
                    app.append(" & ");
                }
                p.appendTo(app, values);
            }
            for (PropertyFilter<S> p : mExclusiveRangeEndFilters) {
                if (count++ > 0) {
                    app.append(" & ");
                }
                p.appendTo(app, values);
            }
            for (PropertyFilter<S> p : mInclusiveRangeEndFilters) {
                if (count++ > 0) {
                    app.append(" & ");
                }
                p.appendTo(app, values);
            }
            newline(app);
        }
        return true;
    }

    /**
     * Provides support for {@link IndexedQueryExecutor}.
     */
    public static interface Support<S extends Storable> {
        /**
         * Perform an index scan of a subset of Storables referenced by an
         * index. The identity values are aligned with the index properties at
         * property 0. An optional range start or range end aligns with the index
         * property following the last of the identity values.
         *
         *
         * @param index index to open, which may be a primary key index
         * @param identityValues optional list of exactly matching values to apply to index
         * @param rangeStartBoundary start boundary type
         * @param rangeStartValue value to start at if boundary is not open
         * @param rangeEndBoundary end boundary type
         * @param rangeEndValue value to end at if boundary is not open
         * @param reverseRange indicates that range operates on a property whose
         * natural order is descending. Only the code that opens the physical
         * cursor should examine this parameter. If true, then the range start and
         * end parameter pairs need to be swapped.
         * @param reverseOrder when true, iteration should be reversed from its
         * natural order
         */
        Cursor<S> fetchSubset(StorableIndex<S> index,
                              Object[] identityValues,
                              BoundaryType rangeStartBoundary,
                              Object rangeStartValue,
                              BoundaryType rangeEndBoundary,
                              Object rangeEndValue,
                              boolean reverseRange,
                              boolean reverseOrder)
            throws FetchException;
    }
}
