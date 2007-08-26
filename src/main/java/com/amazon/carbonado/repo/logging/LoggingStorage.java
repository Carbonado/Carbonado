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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.filter.Filter;

/**
 *
 *
 * @author Brian S O'Neill
 */
class LoggingStorage<S extends Storable> implements Storage<S> {
    private final Storage<S> mStorage;
    final Log mLog;

    LoggingStorage(LoggingRepository repo, Storage<S> storage) {
        mStorage = storage;
        mLog = repo.getLog();
        storage.addTrigger(new LoggingTrigger<S>(mLog));
    }

    public Class<S> getStorableType() {
        return mStorage.getStorableType();
    }

    public S prepare() {
        return mStorage.prepare();
    }

    public Query<S> query() throws FetchException {
        return new LoggingQuery<S>(this, mStorage.query());
    }

    public Query<S> query(String filter) throws FetchException {
        return new LoggingQuery<S>(this, mStorage.query(filter));
    }

    public Query<S> query(Filter<S> filter) throws FetchException {
        return new LoggingQuery<S>(this, mStorage.query(filter));
    }

    /**
     * @since 1.2
     */
    public void truncate() throws PersistException {
        if (mLog.isEnabled()) {
            mLog.write("Storage.truncate() on " + getStorableType().getClass());
        }
        mStorage.truncate();
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mStorage.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mStorage.removeTrigger(trigger);
    }

    protected Query<S> wrap(Query<S> query) {
        return new LoggingQuery<S>(this, query);
    }

    private static class LoggingTrigger<S extends Storable> extends Trigger<S> {
        private final Log mLog;

        LoggingTrigger(Log log) {
            mLog = log;
        }

        @Override
        public Object beforeInsert(S storable) {
            if (mLog.isEnabled()) {
                mLog.write("Storable.insert() on " + storable.toString());
            }
            return null;
        }

        @Override
        public Object beforeTryInsert(S storable) {
            if (mLog.isEnabled()) {
                mLog.write("Storable.tryInsert() on " + storable.toString());
            }
            return null;
        }

        @Override
        public Object beforeUpdate(S storable) {
            if (mLog.isEnabled()) {
                mLog.write("Storable.update() on " + storable.toString());
            }
            return null;
        }

        @Override
        public Object beforeTryUpdate(S storable) {
            if (mLog.isEnabled()) {
                mLog.write("Storable.tryUpdate() on " + storable.toString());
            }
            return null;
        }

        @Override
        public Object beforeDelete(S storable) {
            if (mLog.isEnabled()) {
                mLog.write("Storable.delete() on " + storable.toString());
            }
            return null;
        }

        @Override
        public Object beforeTryDelete(S storable) {
            if (mLog.isEnabled()) {
                mLog.write("Storable.tryDelete() on " + storable.toString());
            }
            return null;
        }

        @Override
        public void afterLoad(S storable) {
            if (mLog.isEnabled()) {
                mLog.write("Loaded " + storable.toString());
            }
        }
    }
}
