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
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.spi.WrappedStorage;

/**
 *
 *
 * @author Brian S O'Neill
 */
class LoggingStorage<S extends Storable> extends WrappedStorage<S> {
    final LoggingRepository mRepo;

    LoggingStorage(LoggingRepository repo, Storage<S> storage) {
        super(storage, repo.mTriggerFactories);
        mRepo = repo;
    }

    protected S wrap(S storable) {
        return super.wrap(storable);
    }

    protected Query<S> wrap(Query<S> query) {
        return new LoggingQuery<S>(this, query);
    }

    protected Support createSupport(S storable) {
        return new Handler(storable);
    }

    private class Handler extends Support {
        private final S mStorable;

        Handler(S storable) {
            mStorable = storable;
        }

        public Repository getRootRepository() {
            return mRepo.getRootRepository();
        }

        public boolean isPropertySupported(String propertyName) {
            return mStorable.isPropertySupported(propertyName);
        }

        public void load() throws FetchException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.load() on " + mStorable.toStringKeyOnly());
            }
            mStorable.load();
        }

        public boolean tryLoad() throws FetchException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.tryLoad() on " + mStorable.toStringKeyOnly());
            }
            return mStorable.tryLoad();
        }

        public void insert() throws PersistException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.insert() on " + mStorable.toString());
            }
            mStorable.insert();
        }

        public boolean tryInsert() throws PersistException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.tryInsert() on " + mStorable.toString());
            }
            return mStorable.tryInsert();
        }

        public void update() throws PersistException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.update() on " + mStorable.toString());
            }
            mStorable.update();
        }

        public boolean tryUpdate() throws PersistException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.tryUpdate() on " + mStorable.toString());
            }
            return mStorable.tryUpdate();
        }

        public void delete() throws PersistException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.delete() on " + mStorable.toStringKeyOnly());
            }
            mStorable.delete();
        }

        public boolean tryDelete() throws PersistException {
            Log log = mRepo.getLog();
            if (log.isEnabled()) {
                log.write("Storable.tryDelete() on " + mStorable.toStringKeyOnly());
            }
            return mStorable.tryDelete();
        }

        public Support createSupport(S storable) {
            return new Handler(storable);
        }
    }
}
