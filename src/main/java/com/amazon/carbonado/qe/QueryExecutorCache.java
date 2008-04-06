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

import java.util.Map;

import org.cojen.util.SoftValuedHashMap;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;

/**
 * QueryExecutors should be cached since expensive analysis is often required
 * to build them.
 *
 * @author Brian S O'Neill
 */
public class QueryExecutorCache<S extends Storable> implements QueryExecutorFactory<S> {
    private final QueryExecutorFactory<S> mFactory;

    // Maps filters to maps which map ordering lists (possibly with hints) to executors.
    private final Map<Filter<S>, Map<Object, QueryExecutor<S>>> mFilterToExecutor;

    public QueryExecutorCache(QueryExecutorFactory<S> factory) {
        if (factory == null) {
            throw new IllegalArgumentException();
        }
        mFactory = factory;
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
        Map<Object, QueryExecutor<S>> map;
        synchronized (mFilterToExecutor) {
            map = mFilterToExecutor.get(filter);
            if (map == null) {
                map = new SoftValuedHashMap(7);
                mFilterToExecutor.put(filter, map);
            }
        }

        Object key;
        if (hints == null || hints.isEmpty()) {
            key = ordering;
        } else {
            key = new WithHintsKey(ordering, hints);
        }

        QueryExecutor<S> executor;
        synchronized (map) {
            executor = map.get(key);
            if (executor == null) {
                executor = mFactory.executor(filter, ordering, hints);
                map.put(key, executor);
            }
        }

        return executor;
    }

    private static class WithHintsKey {
        private final OrderingList mOrdering;
        private final QueryHints mHints;

        WithHintsKey(OrderingList ordering, QueryHints hints) {
            mOrdering = ordering;
            mHints = hints;
        }

        @Override
        public int hashCode() {
            return mOrdering == null ? 0 : (mOrdering.hashCode() * 31) + mHints.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof WithHintsKey) {
                WithHintsKey other = (WithHintsKey) obj;
                return (mOrdering == null ? other.mOrdering == null :
                        mOrdering.equals(other.mOrdering)) &&
                    mHints.equals(other.mHints);
            }
            return false;
        }
    }
}
