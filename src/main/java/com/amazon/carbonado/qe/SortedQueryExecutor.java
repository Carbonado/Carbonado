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

import java.util.Comparator;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.ArraySortBuffer;
import com.amazon.carbonado.cursor.MergeSortBuffer;
import com.amazon.carbonado.cursor.SortBuffer;
import com.amazon.carbonado.cursor.SortedCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * QueryExecutor which wraps another and sorts the results.
 *
 * @author Brian S O'Neill
 * @see SortedCursor
 */
public class SortedQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    private final Support<S> mSupport;
    private final QueryExecutor<S> mExecutor;

    private final Comparator<S> mHandledComparator;
    private final Comparator<S> mFinisherComparator;

    private final OrderingList<S> mHandledOrdering;
    private final OrderingList<S> mRemainderOrdering;

    /**
     * @param support optional support to control sort buffer; if null, array is used
     * @param executor executor to wrap
     * @param handledOrdering optional handled ordering
     * @param remainderOrdering required remainder ordering
     * @throws IllegalArgumentException if executor is null or if
     * remainder ordering is empty
     */
    public SortedQueryExecutor(Support<S> support,
                               QueryExecutor<S> executor,
                               OrderingList<S> handledOrdering,
                               OrderingList<S> remainderOrdering)
    {
        if (support == null) {
            if (this instanceof Support) {
                support = (Support<S>) this;
            } else {
                support = new ArraySortSupport<S>();
            }
        }
        if (executor == null) {
            throw new IllegalArgumentException();
        }

        mSupport = support;
        mExecutor = executor;

        if (handledOrdering != null && handledOrdering.size() == 0) {
            handledOrdering = null;
        }
        if (remainderOrdering != null && remainderOrdering.size() == 0) {
            remainderOrdering = null;
        }

        if (remainderOrdering == null) {
            throw new IllegalArgumentException();
        }

        if (handledOrdering == null) {
            mHandledComparator = null;
            mHandledOrdering = OrderingList.emptyList();
        } else {
            mHandledComparator = SortedCursor.createComparator(handledOrdering);
            mHandledOrdering = handledOrdering;
        }

        mFinisherComparator = SortedCursor.createComparator(remainderOrdering);
        mRemainderOrdering = remainderOrdering;
    }

    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        Cursor<S> cursor = mExecutor.fetch(values);
        SortBuffer<S> buffer = mSupport.createSortBuffer();
        return new SortedCursor<S>(cursor, buffer, mHandledComparator, mFinisherComparator);
    }

    @Override
    public long count(FilterValues<S> values) throws FetchException {
        return mExecutor.count(values);
    }

    public Filter<S> getFilter() {
        return mExecutor.getFilter();
    }

    public OrderingList<S> getOrdering() {
        return mHandledOrdering.concat(mRemainderOrdering);
    }

    /**
     * Prints native query of the wrapped executor.
     */
    @Override
    public boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        return mExecutor.printNative(app, indentLevel, values);
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        indent(app, indentLevel);
        app.append("sort: ");
        if (mHandledOrdering.size() > 0) {
            app.append(mHandledOrdering.toString());
            app.append(", ");
        }
        app.append(mRemainderOrdering.toString());
        newline(app);
        mExecutor.printPlan(app, increaseIndent(indentLevel), values);
        return true;
    }

    /**
     * Provides support for {@link SortedQueryExecutor}.
     */
    public static interface Support<S extends Storable> {
        /**
         * Implementation must return an empty buffer for sorting.
         */
        SortBuffer<S> createSortBuffer();
    }

    /**
     * @since 1.2
     */
    public static class ArraySortSupport<S extends Storable> implements Support<S> {
        /**
         * Returns a new ArraySortBuffer.
         */
        public SortBuffer<S> createSortBuffer() {
            return new ArraySortBuffer<S>();
        }
    }

    /**
     * @since 1.2
     */
    public static class MergeSortSupport<S extends Storable> implements Support<S> {
        /**
         * Returns a new MergeSortBuffer.
         */
        public SortBuffer<S> createSortBuffer() {
            return new MergeSortBuffer<S>();
        }
    }
}
