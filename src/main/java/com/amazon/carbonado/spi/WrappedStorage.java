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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.util.QuickConstructorGenerator;

/**
 * Abstract storage that wraps all returned Storables and Queries, including
 * those returned from joins. Property access methods (get and set) are
 * delegated directly to the wrapped storable. Other operations are delegated
 * to a special {@link WrappedStorage.Support handler}.
 *
 * @author Brian S O'Neill
 */
public abstract class WrappedStorage<S extends Storable> implements Storage<S> {
    private final Storage<S> mStorage;
    private final WrappedStorableFactory<S> mFactory;
    final TriggerManager<S> mTriggerManager;

    /**
     * @param storage storage to wrap
     */
    public WrappedStorage(Storage<S> storage, Iterable<TriggerFactory> triggerFactories)
        throws RepositoryException
    {
        mStorage = storage;
        Class<? extends S> wrappedClass = StorableGenerator
            .getWrappedClass(storage.getStorableType());
        mFactory = QuickConstructorGenerator
            .getInstance(wrappedClass, WrappedStorableFactory.class);
        mTriggerManager = new TriggerManager<S>(storage.getStorableType(), triggerFactories);
    }

    public Class<S> getStorableType() {
        return mStorage.getStorableType();
    }

    public S prepare() {
        return wrap(mStorage.prepare());
    }

    public Query<S> query() throws FetchException {
        return wrap(mStorage.query());
    }

    public Query<S> query(String filter) throws FetchException {
        return wrap(mStorage.query(filter));
    }

    public Query<S> query(Filter<S> filter) throws FetchException {
        return wrap(mStorage.query(filter));
    }

    public void truncate() throws PersistException {
        mStorage.truncate();
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.removeTrigger(trigger);
    }

    /**
     * Wraps the storable into one which delegates some operations to the
     * storable handler.
     *
     * @param storable storable being wrapped
     * @see #createSupport
     */
    protected S wrap(S storable) {
        if (storable == null) {
            throw new IllegalArgumentException("Storable to wrap is null");
        }
        return mFactory.newWrappedStorable(createSupport(storable), storable);
    }

    /**
     * Wraps the query such that all storables returned by it are wrapped as
     * well.
     *
     * @param query query being wrapped
     * @see WrappedQuery
     */
    protected Query<S> wrap(Query<S> query) {
        return new QueryWrapper(query);
    }

    /**
     * Create a handler used by wrapped storables.
     *
     * @param storable storable being wrapped
     */
    protected abstract Support createSupport(S storable);

    protected Storage<S> getWrappedStorage() {
        return mStorage;
    }

    /**
     * Support for use with {@link WrappedStorage}. Most of the methods defined
     * here are a subset of those defined in Storable.
     *
     * @author Brian S O'Neill
     */
    public abstract class Support implements WrappedSupport<S> {
        public Trigger<? super S> getInsertTrigger() {
            return mTriggerManager.getInsertTrigger();
        }

        public Trigger<? super S> getUpdateTrigger() {
            return mTriggerManager.getUpdateTrigger();
        }

        public Trigger<? super S> getDeleteTrigger() {
            return mTriggerManager.getDeleteTrigger();
        }
    }

    /**
     * Support implementation which delegates all calls to a Storable.
     */
    public class BasicSupport extends Support {
        private final Repository mRepository;
        private final S mStorable;

        public BasicSupport(Repository repo, S storable) {
            mRepository = repo;
            mStorable = storable;
        }

        public Support createSupport(S storable) {
            return new BasicSupport(mRepository, storable);
        }

        public Repository getRootRepository() {
            return mRepository;
        }

        public boolean isPropertySupported(String propertyName) {
            return mStorable.isPropertySupported(propertyName);
        }

        public void load() throws FetchException {
            mStorable.load();
        }

        public boolean tryLoad() throws FetchException {
            return mStorable.tryLoad();
        }

        public void insert() throws PersistException {
            mStorable.insert();
        }

        public boolean tryInsert() throws PersistException {
            return mStorable.tryInsert();
        }

        public void update() throws PersistException {
            mStorable.update();
        }

        public boolean tryUpdate() throws PersistException {
            return mStorable.tryUpdate();
        }

        public void delete() throws PersistException {
            mStorable.delete();
        }

        public boolean tryDelete() throws PersistException {
            return mStorable.tryDelete();
        }

        protected S getWrappedStorable() {
            return mStorable;
        }
    }

    private class QueryWrapper extends WrappedQuery<S> {
        QueryWrapper(Query<S> query) {
            super(query);
        }

        protected S wrap(S storable) {
            return WrappedStorage.this.wrap(storable);
        }

        protected WrappedQuery<S> newInstance(Query<S> query) {
            return new QueryWrapper(query);
        }
    }

    /**
     * Used with QuickConstructorGenerator.
     */
    public static interface WrappedStorableFactory<S extends Storable> {
        /**
         * @param storable storable being wrapped
         * @param support handler for persistence methods
         */
        S newWrappedStorable(WrappedSupport<S> support, S storable);
    }
}
