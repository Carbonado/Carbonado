/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
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

import java.util.LinkedHashMap;
import java.util.Map;

import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.util.SoftValuedCache;

/**
 * QueryExecutors should be cached since expensive analysis is often required to build
 * them. By default, a minimum of 100 query executors can be cached per Storable type.
 * The minimum can be changed with the
 * "com.amazon.carbonado.qe.QueryExecutorCache.minCapacity" system property.
 *
 * @author Brian S O'Neill
 */
public class QueryExecutorCache<S extends Storable> implements QueryExecutorFactory<S> {
    final static int cMinCapacity;

    static {
        int minCapacity = 100;

        String prop = System.getProperty(QueryExecutorCache.class.getName().concat(".minCapacity"));
        if (prop != null) {
            try {
                minCapacity = Integer.parseInt(prop);
            } catch (NumberFormatException e) {
            }
        }

        cMinCapacity = minCapacity;
    }

    private final QueryExecutorFactory<S> mFactory;

    private final Map<Key<S>, QueryExecutor<S>> mPrimaryCache;

    // Maps filters to maps which map ordering lists (possibly with hints) to executors.
    private final Map<Filter<S>, SoftValuedCache<Object, QueryExecutor<S>>> mFilterToExecutor;

    public QueryExecutorCache(QueryExecutorFactory<S> factory) {
        if (factory == null) {
            throw new IllegalArgumentException();
        }
        mFactory = factory;

        mPrimaryCache = new LinkedHashMap<Key<S>, QueryExecutor<S>>(17, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Key<S>, QueryExecutor<S>> eldest) {
                return size() > cMinCapacity;
            }
        };

        mFilterToExecutor = new WeakIdentityMap(7);
    }

    public Class<S> getStorableType() {
        return mFactory.getStorableType();
    }

    /**
     * Returns an executor from the cache.
     *
     * @param filter optional filter
     * @param ordering optional order-by properties
     * @param hints optional query hints
     */
    public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws RepositoryException
    {
        final Key<S> key = new Key<S>(filter, ordering, hints);

        synchronized (mPrimaryCache) {
            QueryExecutor<S> executor = mPrimaryCache.get(key);
            if (executor != null) {
                return executor;
            }
        }

        // Fallback to second level cache, which may still have the executor because
        // garbage collection has not reclaimed it yet. It also allows some concurrent
        // executor creation, by using filter-specific locks.

        SoftValuedCache<Object, QueryExecutor<S>> cache;
        synchronized (mFilterToExecutor) {
            cache = mFilterToExecutor.get(filter);
            if (cache == null) {
                cache = SoftValuedCache.newCache(7);
                mFilterToExecutor.put(filter, cache);
            }
        }

        Object subKey;
        if (hints == null || hints.isEmpty()) {
            subKey = ordering;
        } else {
            // Don't construct key with filter. It is not needed here and it would prevent
            // garbage collection of filters.
            subKey = new Key(null, ordering, hints);
        }

        QueryExecutor<S> executor;
        synchronized (cache) {
            executor = cache.get(subKey);
            if (executor == null) {
                executor = mFactory.executor(filter, ordering, hints);
                cache.put(subKey, executor);
            }
        }

        synchronized (mPrimaryCache) {
            mPrimaryCache.put(key, executor);
        }

        return executor;
    }

    private static class Key<S extends Storable> {
        private final Filter<S> mFilter;
        private final OrderingList<S> mOrdering;
        private final QueryHints mHints;

        Key(Filter<S> filter, OrderingList<S> ordering, QueryHints hints) {
            mFilter = filter;
            mOrdering = ordering;
            mHints = hints;
        }

        @Override
        public int hashCode() {
            Filter<S> filter = mFilter;
            int hash = filter == null ? 0 : filter.hashCode();
            OrderingList<S> ordering = mOrdering;
            if (ordering != null) {
                hash = hash * 31 + ordering.hashCode();
            }
            QueryHints hints = mHints;
            if (hints != null) {
                hash = hash * 31 + hints.hashCode();
            }
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Key) {
                Key other = (Key) obj;
                return equals(mFilter, other.mFilter)
                    && equals(mOrdering, other.mOrdering)
                    && equals(mHints, other.mHints);
            }
            return false;
        }

        private static boolean equals(Object a, Object b) {
            return a == null ? b == null : a.equals(b);
        }
    }
}
