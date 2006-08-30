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

import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;

/**
 * QueryExecutor which delegates by executing a Query on a Storage.
 *
 * @author Brian S O'Neill
 */
public class DelegatedQueryExecutor<S extends Storable> implements QueryExecutor<S> {
    private final QueryExecutor<S> mExecutor;
    private final Storage<S> mStorage;
    private final Query<S> mQuery;

    /**
     * @param executor executor to emulate
     * @param storage storage to query
     * @throws IllegalArgumentException if any parameter is null
     */
    public DelegatedQueryExecutor(QueryExecutor<S> executor, Storage<S> storage)
        throws FetchException
    {
        if (executor == null || storage == null) {
            throw new IllegalStateException();
        }

        mExecutor = executor;
        mStorage = storage;

        Filter<S> filter = executor.getFilter();

        Query<S> query;
        if (filter == null) {
            query = storage.query();
        } else {
            query = storage.query(filter);
        }

        List<OrderedProperty<S>> ordering = executor.getOrdering();
        if (ordering.size() > 0) {
            String[] orderBy = new String[ordering.size()];
            for (int i=0; i<orderBy.length; i++) {
                orderBy[i] = ordering.get(i).toString();
            }
            query = query.orderBy(orderBy);
        }

        mQuery = query;
    }

    public Class<S> getStorableType() {
        return mStorage.getStorableType();
    }

    public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
        return applyFilterValues(values).fetch();
    }

    public long count(FilterValues<S> values) throws FetchException {
        return applyFilterValues(values).count();
    }

    public Filter<S> getFilter() {
        return mExecutor.getFilter();
    }

    public List<OrderedProperty<S>> getOrdering() {
        return mExecutor.getOrdering();
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
        // FIXME: figure out how to transfer values directly to query.

        Query<S> query = mQuery;
        Filter<S> filter = query.getFilter();
        // FIXME: this code can get confused if filter has constants.
        if (values != null && filter != null && query.getBlankParameterCount() != 0) {
            query = query.withValues(values.getValuesFor(filter));
        }
        return query;
    }
}
