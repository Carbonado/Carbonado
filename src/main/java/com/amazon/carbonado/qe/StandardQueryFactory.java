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
import java.util.Map;

import org.cojen.util.SoftValuedHashMap;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Builds and caches StandardQuery instances.
 *
 * @author Brian S O'Neill
 */
public abstract class StandardQueryFactory<S extends Storable> implements QueryFactory<S> {
    private final Class<S> mType;
    private final boolean mLazySetExecutor;

    private final Map<String, Query<S>> mStringToQuery;

    // Maps filters to maps which map ordering lists to queries.
    private final Map<Filter<S>, Map<OrderingList<S>, Query<S>>> mFilterToQuery;

    protected StandardQueryFactory(Class<S> type) {
        this(type, false);
    }

    /**
     * @param lazySetExecutor by default, query executors are built and set
     * eagerly. Pass true to build and set executor on first query use.
     */
    protected StandardQueryFactory(Class<S> type, boolean lazySetExecutor) {
        if (type == null) {
            throw new IllegalArgumentException();
        }
        mType = type;
        mLazySetExecutor = lazySetExecutor;
        mStringToQuery = new SoftValuedHashMap(7);
        mFilterToQuery = new WeakIdentityMap(7);
    }

    public Class<S> getStorableType() {
        return mType;
    }

    /**
     * Returns a new or cached query that fetches everything.
     */
    public Query<S> query() throws FetchException {
        return query(Filter.getOpenFilter(mType), null);
    }

    /**
     * Returns a new or cached query for the given filter.
     *
     * @throws IllegalArgumentException if filter is null
     */
    public Query<S> query(String filter) throws FetchException {
        synchronized (mStringToQuery) {
            Query<S> query = mStringToQuery.get(filter);
            if (query == null) {
                if (filter == null) {
                    throw new IllegalArgumentException("Query filter must not be null");
                }
                query = query(Filter.filterFor(mType, filter), null);
                mStringToQuery.put(filter, query);
            }
            return query;
        }
    }

    /**
     * Returns a new or cached query for the given filter.
     *
     * @throws IllegalArgumentException if filter is null
     */
    public Query<S> query(Filter<S> filter) throws FetchException {
        return query(filter, null);
    }

    /**
     * Returns a new or cached query for the given query specification.
     *
     * @throws IllegalArgumentException if filter is null
     */
    public Query<S> query(Filter<S> filter, OrderingList<S> ordering) throws FetchException {
        return query(filter, ordering, null);
    }

    /**
     * Returns a new or cached query for the given query specification.
     *
     * @throws IllegalArgumentException if filter is null
     */
    public Query<S> query(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws FetchException
    {
        filter = filter.bind();

        Map<OrderingList<S>, Query<S>> map;
        synchronized (mFilterToQuery) {
            map = mFilterToQuery.get(filter);
            if (map == null) {
                if (filter == null) {
                    throw new IllegalArgumentException("Query filter must not be null");
                }
                map = new SoftValuedHashMap(7);
                mFilterToQuery.put(filter, map);
            }
        }

        Query<S> query;
        synchronized (map) {
            query = map.get(ordering);
            if (query == null) {
                FilterValues<S> values = filter.initialFilterValues();
                if (values == null && filter.isClosed()) {
                    query = new EmptyQuery<S>(this, ordering);
                } else {
                    StandardQuery<S> standardQuery = createQuery(filter, values, ordering, hints);
                    if (!mLazySetExecutor) {
                        try {
                            standardQuery.setExecutor();
                        } catch (RepositoryException e) {
                            throw e.toFetchException();
                        }
                    }
                    query = standardQuery;
                }
                map.put(ordering, query);
            }
        }

        return query;
    }

    /**
     * Returns a new or cached query for the given query specification.
     *
     * @param filter optional filter object, defaults to open filter if null
     * @param values optional values object, defaults to filter initial values
     * @param ordering optional order-by properties
     */
    public Query<S> query(Filter<S> filter, FilterValues<S> values, OrderingList<S> ordering)
        throws FetchException
    {
        return query(filter, values, ordering, null);
    }

    /**
     * Returns a new or cached query for the given query specification.
     *
     * @param filter optional filter object, defaults to open filter if null
     * @param values optional values object, defaults to filter initial values
     * @param ordering optional order-by properties
     * @param hints optional hints
     */
    public Query<S> query(Filter<S> filter, FilterValues<S> values, OrderingList<S> ordering,
                          QueryHints hints)
        throws FetchException
    {
        Query<S> query = query(filter != null ? filter : Filter.getOpenFilter(mType),
                               ordering, hints);

        if (values != null) {
            query = query.withValues(values.getSuppliedValues());
        }

        return query;
    }

    /**
     * For each cached query, calls {@link StandardQuery#setExecutor}.
     */
    public void setExecutors() throws RepositoryException {
        for (StandardQuery<S> query : gatherQueries()) {
            query.setExecutor();
        }
    }

    /**
     * For each cached query, calls {@link StandardQuery#resetExecutor}.
     * This call can be used to rebuild all cached query plans after the set of
     * available indexes has changed.
     */
    public void resetExecutors() throws RepositoryException {
        for (StandardQuery<S> query : gatherQueries()) {
            query.resetExecutor();
        }
    }

    /**
     * For each cached query, calls {@link StandardQuery#clearExecutor}.
     * This call can be used to clear all cached query plans after the set of
     * available indexes has changed.
     */
    public void clearExecutors() {
        for (StandardQuery<S> query : gatherQueries()) {
            query.clearExecutor();
        }
    }

    /**
     * Implement this method to return query implementations.
     *
     * @param filter optional filter object, defaults to open filter if null
     * @param values optional values object, defaults to filter initial values
     * @param ordering optional order-by properties
     * @param hints optional hints
     */
    protected abstract StandardQuery<S> createQuery(Filter<S> filter,
                                                    FilterValues<S> values,
                                                    OrderingList<S> ordering,
                                                    QueryHints hints)
        throws FetchException;

    private ArrayList<StandardQuery<S>> gatherQueries() {
        // Copy all queries and operate on the copy instead of holding lock for
        // potentially a long time.
        ArrayList<StandardQuery<S>> queries = new ArrayList<StandardQuery<S>>();

        synchronized (mFilterToQuery) {
            for (Map<OrderingList<S>, Query<S>> map : mFilterToQuery.values()) {
                for (Query<S> query : map.values()) {
                    if (query instanceof StandardQuery) {
                        queries.add((StandardQuery<S>) query);
                    }
                }
            }
        }

        return queries;
    }
}
