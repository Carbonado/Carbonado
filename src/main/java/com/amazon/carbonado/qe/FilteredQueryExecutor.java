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

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.FilteredCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * QueryExecutor which wraps another and filters results.
 *
 * @author Brian S O'Neill
 * @see FilteredCursor
 */
public class FilteredQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    private final QueryExecutor<S> mExecutor;
    private final Filter<S> mFilter;

    /**
     * @param executor executor to wrap
     * @param filter filter to apply to cursor
     * @throws IllegalArgumentException if any argument is null or filter is open or closed
     */
    public FilteredQueryExecutor(QueryExecutor<S> executor, Filter<S> filter) {
        if (executor == null) {
            throw new IllegalArgumentException();
        }
        if (filter == null || filter.isOpen() || filter.isClosed()) {
            throw new IllegalArgumentException();
        }
        mExecutor = executor;
        // Ensure filter is same as what will be provided by values.
        FilterValues<S> values = filter.initialFilterValues();
        if (values != null) {
            filter = values.getFilter();
        }
        mFilter = filter;
    }

    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        return FilteredCursor.applyFilter(mFilter, values, mExecutor.fetch(values));
    }

    /**
     * Returns the combined filter of the wrapped executor and the extra filter.
     */
    public Filter<S> getFilter() {
        return mExecutor.getFilter().and(mFilter);
    }

    public OrderingList<S> getOrdering() {
        return mExecutor.getOrdering();
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
        app.append("filter: ");
        mFilter.appendTo(app, values);
        newline(app);
        mExecutor.printPlan(app, increaseIndent(indentLevel), values);
        return true;
    }
}
