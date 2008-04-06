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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.filter.Filter;

/**
 * QueryExecutorFactory which produces executors which delegate via {@link DelegatedQueryExecutor}.
 *
 * @author Brian S O'Neill
 */
public class DelegatedQueryExecutorFactory<S extends Storable> implements QueryExecutorFactory<S> {
    private final Storage<S> mStorage;

    public DelegatedQueryExecutorFactory(Storage<S> rootStorage) {
        if (rootStorage == null) {
            throw new IllegalArgumentException();
        }
        mStorage = rootStorage;
    }

    public Class<S> getStorableType() {
        return mStorage.getStorableType();
    }

    public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws FetchException
    {
        return new DelegatedQueryExecutor<S>(mStorage, filter, ordering);
    }
}
