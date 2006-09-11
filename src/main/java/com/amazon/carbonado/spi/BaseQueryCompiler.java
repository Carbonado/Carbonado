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

package com.amazon.carbonado.spi;

import java.util.Map;

import org.cojen.util.KeyFactory;
import org.cojen.util.SoftValuedHashMap;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.MalformedFilterException;
import com.amazon.carbonado.Query;

import com.amazon.carbonado.filter.ClosedFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableInfo;

/**
 * BaseQueryCompiler caches compiled queries, and calls an abstract method
 * to compile queries it doesn't have cached.
 *
 * @author Brian S O'Neill
 * @deprecated Use {@link com.amazon.carbonado.qe.StandardQueryFactory}
 */
public abstract class BaseQueryCompiler<S extends Storable> {
    private final StorableInfo<S> mInfo;
    private final Map<String, Query<S>> mStringToQuery;
    private final Map<Filter<S>, Queries<S>> mFilterToQueries;

    /**
     * @throws IllegalArgumentException if type is null
     */
    // Note: Since constructor has parameters, this class is called Base
    // instead of Abstract.
    @SuppressWarnings("unchecked")
    protected BaseQueryCompiler(StorableInfo<S> info) {
        if (info == null) {
            throw new IllegalArgumentException();
        }
        mInfo = info;
        mStringToQuery = new SoftValuedHashMap(7);
        mFilterToQueries = new WeakIdentityMap(7);
    }

    /**
     * Looks up compiled query in the cache, and returns it. If not found, then
     * one is created and cached for later retrieval.
     *
     * @return cached compiled query which returns everything from storage
     */
    public synchronized Query<S> getCompiledQuery() throws FetchException {
        return getCompiledQuery(Filter.getOpenFilter(mInfo.getStorableType()));
    }

    /**
     * Looks up compiled query in the cache, and returns it. If not found, then
     * the filter expression is parsed, and compileQuery is invoked on the
     * result. The compiled query is cached for later retrieval.
     *
     * @param filter query filter expression to parse
     * @return cached compiled query
     * @throws IllegalArgumentException if query filter expression is null
     * @throws MalformedFilterException if query filter expression is malformed
     */
    public synchronized Query<S> getCompiledQuery(String filter) throws FetchException {
        if (filter == null) {
            throw new IllegalArgumentException("Query filter must not be null");
        }
        Query<S> query = mStringToQuery.get(filter);
        if (query == null) {
            query = getCompiledQuery(Filter.filterFor(mInfo.getStorableType(), filter));
            mStringToQuery.put(filter, query);
        }
        return query;
    }

    /**
     * Looks up compiled query in the cache, and returns it. If not found, then
     * compileQuery is invoked on the result. The compiled query is cached for
     * later retrieval.
     *
     * @param filter root filter tree
     * @return cached compiled query
     * @throws IllegalArgumentException if root filter is null
     */
    public synchronized Query<S> getCompiledQuery(Filter<S> filter) throws FetchException {
        if (filter == null) {
            throw new IllegalArgumentException("Filter is null");
        }
        Queries<S> queries = mFilterToQueries.get(filter);
        if (queries == null) {
            Query<S> query;
            FilterValues<S> values = filter.initialFilterValues();
            if (values != null) {
                // FilterValues applies to bound filter. Use that instead.
                Filter<S> altFilter = values.getFilter();
                if (altFilter != filter) {
                    return getCompiledQuery(altFilter);
                }
                query = compileQuery(values, null);
            } else {
                query = compileQuery(null, null);
                if (filter instanceof ClosedFilter) {
                    query = query.not();
                }
            }
            queries = new Queries<S>(query);
            mFilterToQueries.put(filter, queries);
        }
        return queries.mPlainQuery;
    }

    /**
     * Used by implementations to retrieve cached queries that have order-by
     * properties.
     *
     * @param values filter values produced earlier by this compiler, or null,
     * or a derived instance
     * @param propertyNames optional property names to order by, which may be
     * prefixed with '+' or '-'
     * @throws IllegalArgumentException if properties are not supported or if
     * filter did not originate from this compiler
     */
    @SuppressWarnings("unchecked")
    public Query<S> getOrderedQuery(FilterValues<S> values, String... propertyNames)
        throws FetchException, IllegalArgumentException, UnsupportedOperationException
    {
        final Filter<S> filter =
            values == null ? Filter.getOpenFilter(mInfo.getStorableType()) : values.getFilter();

        final Queries<S> queries = mFilterToQueries.get(filter);

        if (queries == null) {
            throw new IllegalArgumentException("Unknown filter provided");
        }

        if (propertyNames == null || propertyNames.length == 0) {
            return queries.mPlainQuery;
        }

        final Object key = KeyFactory.createKey(propertyNames);
        Query<S> query = queries.mOrderingsToQuery.get(key);

        if (query != null) {
            // Now transfer property values.
            if (values != null) {
                query = query.withValues(values.getSuppliedValues());
            }

            return query;
        }

        // Try again with property names that have an explicit direction,
        // hoping for a cache hit.

        boolean propertyNamesChanged = false;
        final int length = propertyNames.length;
        for (int i=0; i<length; i++) {
            String propertyName = propertyNames[i];
            if (propertyName == null) {
                throw new IllegalArgumentException("Order by property [" + i + "] is null");
            }
            if (!propertyName.startsWith("+") && !propertyName.startsWith("-")) {
                if (!propertyNamesChanged) {
                    propertyNames = propertyNames.clone();
                    propertyNamesChanged = true;
                }
                propertyNames[i] = "+".concat(propertyName);
            }
        }

        if (propertyNamesChanged) {
            return getOrderedQuery(values, propertyNames);
        }

        // If this point is reached, propertyNames is guaranteed to have no
        // null elements, and all have an explicit direction.

        OrderedProperty<S>[] orderings = new OrderedProperty[length];

        for (int i=0; i<length; i++) {
            orderings[i] = OrderedProperty.parse(mInfo, propertyNames[i]);
        }

        FilterValues<S> initialValues = filter.initialFilterValues();

        query = compileQuery(initialValues, orderings);
        queries.mOrderingsToQuery.put(key, query);

        // Now transfer property values.
        if (values != null) {
            query = query.withValues(values.getSuppliedValues());
        }

        return query;
    }

    /**
     * Returns the StorableInfo object in this object.
     */
    protected StorableInfo<S> getStorableInfo() {
        return mInfo;
    }

    /**
     * Compile the query represented by the type checked root node. If any
     * order-by properties are supplied, they have been checked as well.
     *
     * @param values values and filter for query, which may be null if
     * unfiltered
     * @param orderings optional list of properties to order by
     */
    protected abstract Query<S> compileQuery(FilterValues<S> values,
                                             OrderedProperty<S>[] orderings)
        throws FetchException, UnsupportedOperationException;

    private static class Queries<S extends Storable> {
        final Query<S> mPlainQuery;

        final Map<Object, Query<S>> mOrderingsToQuery;

        @SuppressWarnings("unchecked")
        Queries(Query<S> query) {
            mPlainQuery = query;
            mOrderingsToQuery = new SoftValuedHashMap(7);
        }
    }
}
