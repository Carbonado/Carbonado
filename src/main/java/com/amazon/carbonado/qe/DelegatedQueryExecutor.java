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
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * QueryExecutor which delegates by executing a Query on a Storage.
 *
 * @author Brian S O'Neill
 */
public class DelegatedQueryExecutor<S extends Storable> implements QueryExecutor<S> {
    private static final <T> T check(T object) {
        if (object == null) {
            throw new IllegalArgumentException();
        }
        return object;
    }

    private final Filter<S> mFilter;
    private final OrderingList<S> mOrdering;
    private final Query<S> mQuery;

    /**
     * @param rootStorage root storage to query
     * @param executor executor to emulate
     * @throws IllegalArgumentException if any parameter is null
     */
    public DelegatedQueryExecutor(Storage<S> rootStorage, QueryExecutor<S> executor)
        throws FetchException
    {
        this(rootStorage, (executor = check(executor)).getFilter(), executor.getOrdering());
    }

    /**
     * @param rootStorage root storage to query
     * @param filter optional query filter
     * @param ordering optional ordering
     * @throws IllegalArgumentException if rootStorage is null
     */
    public DelegatedQueryExecutor(Storage<S> rootStorage,
                                  Filter<S> filter, OrderingList<S> ordering)
        throws FetchException
    {
        check(rootStorage);

        Query<S> query;
        if (filter == null) {
            query = rootStorage.query();
        } else {
            query = rootStorage.query(filter);
        }

        if (ordering == null) {
            ordering = OrderingList.emptyList();
        } else if (ordering.size() > 0) {
            query = query.orderBy(ordering.asStringArray());
        }

        mFilter = filter;
        mOrdering = ordering;
        mQuery = query;
    }

    public Class<S> getStorableType() {
        return mFilter.getStorableType();
    }

    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        return applyFilterValues(values).fetch();
    }

    public Cursor<S> fetchSlice(FilterValues<S> values, long from, Long to) throws FetchException {
        return applyFilterValues(values).fetchSlice(from, to);
    }

    public long count(FilterValues<S> values) throws FetchException {
        return applyFilterValues(values).count();
    }

    public Filter<S> getFilter() {
        return mFilter;
    }

    public OrderingList<S> getOrdering() {
        return mOrdering;
    }

    public boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        return applyFilterValues(values).printNative(app, indentLevel);
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        Query<S> query;
        try {
            query = applyFilterValues(values);
        } catch (IllegalStateException e) {
            query = mQuery;
        }
        return query.printPlan(app, indentLevel);
    }

    private Query<S> applyFilterValues(FilterValues<S> values) {
        Query<S> query = mQuery;
        Filter<S> filter = query.getFilter();
        if (values != null && filter != null && query.getBlankParameterCount() != 0) {
            query = query.withValues(values.getSuppliedValuesFor(filter));
        }
        return query;
    }
}
