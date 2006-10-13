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

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Complete rule-based query engine implementation.
 *
 * @author Brian S O'Neill
 */
public class QueryEngine<S extends Storable> extends StandardQueryFactory<S>
    implements QueryExecutorFactory<S>
{
    final RepositoryAccess mRepoAccess;
    final QueryExecutorFactory<S> mExecutorFactory;

    public QueryEngine(Class<S> type, RepositoryAccess access) {
        super(type);
        mRepoAccess = access;
        mExecutorFactory = new QueryExecutorCache<S>(new UnionQueryAnalyzer<S>(type, access));
    }

    public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering)
        throws RepositoryException
    {
        return mExecutorFactory.executor(filter, ordering);
    }

    protected StandardQuery<S> createQuery(FilterValues<S> values, OrderingList<S> ordering) {
        return new Query(values, ordering, null);
    }

    private class Query extends StandardQuery<S> {
        Query(FilterValues<S> values, OrderingList<S> ordering, QueryExecutor<S> executor) {
            super(values, ordering, executor);
        }

        protected Transaction enterTransaction(IsolationLevel level) {
            return mRepoAccess.getRootRepository().enterTransaction(level);
        }

        protected QueryFactory<S> queryFactory() {
            return QueryEngine.this;
        }

        protected QueryExecutorFactory<S> executorFactory() {
            return mExecutorFactory;
        }

        protected StandardQuery<S> newInstance(FilterValues<S> values,
                                               OrderingList<S> ordering,
                                               QueryExecutor<S> executor)
        {
            return new Query(values, ordering, executor);
        }
    }
}
