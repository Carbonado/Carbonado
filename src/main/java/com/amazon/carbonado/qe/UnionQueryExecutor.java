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
import java.util.Comparator;
import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.SortedCursor;
import com.amazon.carbonado.cursor.UnionCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;

/**
 * QueryExecutor which wraps several others and unions the results.
 *
 * @author Brian S O'Neill
 * @see UnionCursor
 */
public class UnionQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    private static <E> E ensureNotNull(E e) {
        if (e == null) {
            throw new IllegalArgumentException();
        }
        return e;
    }

    private final QueryExecutor<S>[] mExecutors;
    private final List<OrderedProperty<S>> mTotalOrderings;
    private final Comparator<S> mOrderComparator;

    /**
     * @param executors executors to wrap, each must have the exact same total ordering
     * @throws IllegalArgumentException if any parameter is null or if ordering doesn't match
     */
    public UnionQueryExecutor(QueryExecutor<S>... executors) {
        this(Arrays.asList(ensureNotNull(executors)));
    }

    /**
     * @param executors executors to wrap, each must have the exact same total ordering
     * @throws IllegalArgumentException if any parameter is null or if ordering doesn't match
     */
    public UnionQueryExecutor(List<QueryExecutor<S>> executors) {
        if (executors == null || executors.size() == 0) {
            throw new IllegalArgumentException();
        }
        List<OrderedProperty<S>> totalOrderings = executors.get(0).getOrdering();
        // Compare for consistency.
        for (int i=1; i<executors.size(); i++) {
            if (!totalOrderings.equals(executors.get(i).getOrdering())) {
                throw new IllegalArgumentException("Ordering doesn't match");
            }
        }
        mExecutors = new QueryExecutor[executors.size()];
        executors.toArray(mExecutors);
        mTotalOrderings = totalOrderings;
        mOrderComparator = SortedCursor.createComparator(totalOrderings);
    }

    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        Cursor<S> cursor = null;
        for (QueryExecutor<S> executor : mExecutors) {
            Cursor<S> subCursor = executor.fetch(values);
            cursor = (cursor == null) ? subCursor
                : new UnionCursor<S>(cursor, subCursor, mOrderComparator);
        }
        return cursor;
    }

    /**
     * Returns the combined filter of the wrapped executors.
     */
    public Filter<S> getFilter() {
        Filter<S> filter = null;
        for (QueryExecutor<S> executor : mExecutors) {
            Filter<S> subFilter = executor.getFilter();
            filter = filter == null ? subFilter : filter.or(subFilter);
        }
        return filter;
    }

    public List<OrderedProperty<S>> getOrdering() {
        return mTotalOrderings;
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        indent(app, indentLevel);
        app.append("union");
        newline(app);
        for (QueryExecutor<S> executor : mExecutors) {
            executor.printPlan(app, increaseIndent(indentLevel), values);
        }
        return true;
    }
}
