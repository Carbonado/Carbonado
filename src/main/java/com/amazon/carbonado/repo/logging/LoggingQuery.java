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

package com.amazon.carbonado.repo.logging;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.spi.WrappedQuery;

/**
 *
 *
 * @author Brian S O'Neill
 */
class LoggingQuery<S extends Storable> extends WrappedQuery<S> {
    private final LoggingStorage<S> mStorage;

    LoggingQuery(LoggingStorage<S> storage, Query<S> query) {
        super(query);
        mStorage = storage;
    }

    public Cursor<S> fetch() throws FetchException {
        Log log = mStorage.mRepo.getLog();
        if (log.isEnabled()) {
            log.write("Query.fetch() on " + this);
        }
        return super.fetch();
    }

    public S loadOne() throws FetchException {
        Log log = mStorage.mRepo.getLog();
        if (log.isEnabled()) {
            log.write("Query.loadOne() on " + this);
        }
        return super.loadOne();
    }

    public S tryLoadOne() throws FetchException {
        Log log = mStorage.mRepo.getLog();
        if (log.isEnabled()) {
            log.write("Query.tryLoadOne() on " + this);
        }
        return super.tryLoadOne();
    }

    public void deleteOne() throws PersistException {
        Log log = mStorage.mRepo.getLog();
        if (log.isEnabled()) {
            log.write("Query.deleteOne() on " + this);
        }
        super.deleteOne();
    }

    public boolean tryDeleteOne() throws PersistException {
        Log log = mStorage.mRepo.getLog();
        if (log.isEnabled()) {
            log.write("Query.tryDeleteOne() on " + this);
        }
        return super.tryDeleteOne();
    }

    public void deleteAll() throws PersistException {
        Log log = mStorage.mRepo.getLog();
        if (log.isEnabled()) {
            log.write("Query.deleteAll() on " + this);
        }
        super.deleteAll();
    }

    protected S wrap(S storable) {
        return mStorage.wrap(storable);
    }

    protected WrappedQuery<S> newInstance(Query<S> query) {
        return new LoggingQuery<S>(mStorage, query);
    }
}
