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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.SortBuffer;
import com.amazon.carbonado.cursor.SortedCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;

/**
 * Abstract QueryExecutor which wraps another and sorts the results.
 *
 * @author Brian S O'Neill
 * @see SortedCursor
 */
public abstract class SortedQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    private final QueryExecutor<S> mExecutor;

    private final Comparator<S> mHandledComparator;
    private final Comparator<S> mFinisherComparator;

    private final List<OrderedProperty<S>> mHandledOrderings;
    private final List<OrderedProperty<S>> mRemainderOrderings;

    /**
     * @param executor executor to wrap
     * @throws IllegalArgumentException if executor is null or if remainder
     * orderings is empty
     */
    public SortedQueryExecutor(QueryExecutor<S> executor,
                               List<OrderedProperty<S>> handledOrderings,
                               List<OrderedProperty<S>> remainderOrderings)
    {
        if (executor == null) {
            throw new IllegalArgumentException();
        }
        mExecutor = executor;

        if (handledOrderings != null && handledOrderings.size() == 0) {
            handledOrderings = null;
        }
        if (remainderOrderings != null && remainderOrderings.size() == 0) {
            remainderOrderings = null;
        }

        if (remainderOrderings == null) {
            throw new IllegalArgumentException();
        }

        if (handledOrderings == null) {
            mHandledComparator = null;
            mHandledOrderings = Collections.emptyList();
        } else {
            mHandledComparator = SortedCursor.createComparator(handledOrderings);
            mHandledOrderings = handledOrderings;
        }

        mFinisherComparator = SortedCursor.createComparator(remainderOrderings);
        mRemainderOrderings = remainderOrderings;
    }

    public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
        Cursor<S> cursor = mExecutor.openCursor(values);
        SortBuffer<S> buffer = createSortBuffer();
        return new SortedCursor<S>(cursor, buffer, mHandledComparator, mFinisherComparator);
    }

    @Override
    public long count(FilterValues<S> values) throws FetchException {
        return mExecutor.count(values);
    }

    public Filter<S> getFilter() {
        return mExecutor.getFilter();
    }

    public List<OrderedProperty<S>> getOrdering() {
        if (mHandledOrderings.size() == 0) {
            return mRemainderOrderings;
        }
        if (mRemainderOrderings.size() == 0) {
            return mHandledOrderings;
        }
        List<OrderedProperty<S>> ordering = new ArrayList<OrderedProperty<S>>
            (mHandledOrderings.size() + mRemainderOrderings.size());

        ordering.addAll(mHandledOrderings);
        ordering.addAll(mRemainderOrderings);

        return ordering;
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        indent(app, indentLevel);
        if (mHandledOrderings.size() == 0) {
            app.append("full sort: ");
        } else {
            app.append("finish sort: ");
        }
        app.append(mRemainderOrderings.toString());
        newline(app);
        mExecutor.printPlan(app, increaseIndent(indentLevel), values);
        return true;
    }

    /**
     * Implementation must return an empty buffer for sorting.
     */
    protected abstract SortBuffer<S> createSortBuffer();
}
