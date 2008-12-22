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

package com.amazon.carbonado.repo.indexed;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistDeadlockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.capability.IndexInfo;

import com.amazon.carbonado.cursor.MergeSortBuffer;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.cursor.SortBuffer;

import com.amazon.carbonado.qe.BoundaryType;
import com.amazon.carbonado.qe.QueryEngine;
import com.amazon.carbonado.qe.QueryExecutorFactory;
import com.amazon.carbonado.qe.StorableIndexSet;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.util.Throttle;

import static com.amazon.carbonado.repo.indexed.ManagedIndex.*;

/**
 *
 *
 * @author Brian S O'Neill
 */
class IndexedStorage<S extends Storable> implements Storage<S>, StorageAccess<S> {
    final IndexedRepository mRepository;
    final Storage<S> mMasterStorage;

    // Maps managed and queryable indexes to IndexInfo objects.
    private final Map<StorableIndex<S>, IndexInfo> mAllIndexInfoMap;

    // Set of indexes available for queries to use.
    private final StorableIndexSet<S> mQueryableIndexSet;

    private final QueryEngine<S> mQueryEngine;

    IndexedStorage(IndexAnalysis<S> analysis) throws RepositoryException {
        mRepository = analysis.repository;
        mMasterStorage = analysis.masterStorage;
        mAllIndexInfoMap = analysis.allIndexInfoMap;
        mQueryableIndexSet = analysis.queryableIndexSet;

        if (analysis.indexesTrigger != null) {
            if (!addTrigger(analysis.indexesTrigger)) {
                // This might be caused by this storage being created again recursively.
                throw new RepositoryException("Unable to add trigger for managing indexes");
            }
        }

        // Okay, now start doing some damage. First, remove unnecessary indexes.
        for (StorableIndex<S> index : analysis.removeIndexSet) {
            removeIndex(index);
        }

        // Now add new indexes.
        for (StorableIndex<S> index : analysis.addIndexSet) {
            registerIndex((ManagedIndex) mAllIndexInfoMap.get(index));
        }

        mQueryEngine = new QueryEngine<S>(mMasterStorage.getStorableType(), mRepository);

        // Install triggers to manage derived properties in external Storables.
        if (analysis.derivedToDependencies != null) {
            for (ChainedProperty<?> derivedTo : analysis.derivedToDependencies) {
                addTrigger(new DerivedIndexesTrigger(mRepository, getStorableType(), derivedTo));
            }
        }
    }

    public Class<S> getStorableType() {
        return mMasterStorage.getStorableType();
    }

    public S prepare() {
        return mMasterStorage.prepare();
    }

    public Query<S> query() throws FetchException {
        return mQueryEngine.query();
    }

    public Query<S> query(String filter) throws FetchException {
        return mQueryEngine.query(filter);
    }

    public Query<S> query(Filter<S> filter) throws FetchException {
        return mQueryEngine.query(filter);
    }

    public void truncate() throws PersistException {
        hasManagedIndexes: {
            for (IndexInfo info : mAllIndexInfoMap.values()) {
                if (info instanceof ManagedIndex) {
                    break hasManagedIndexes;
                }
            }

            // No managed indexes, so nothing special to do.
            mMasterStorage.truncate();
            return;
        }

        Transaction txn = mRepository.enterTransaction();
        try {
            mMasterStorage.truncate();

            // Now truncate the indexes.
            for (IndexInfo info : mAllIndexInfoMap.values()) {
                if (info instanceof ManagedIndex) {
                    ((ManagedIndex) info).getIndexEntryStorage().truncate();
                }
            }

            txn.commit();
        } finally {
            txn.exit();
        }
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mMasterStorage.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mMasterStorage.removeTrigger(trigger);
    }

    // Required by StorageAccess.
    public QueryExecutorFactory<S> getQueryExecutorFactory() {
        return mQueryEngine;
    }

    // Required by StorageAccess.
    public Collection<StorableIndex<S>> getAllIndexes() {
        return mQueryableIndexSet;
    }

    // Required by StorageAccess.
    public Storage<S> storageDelegate(StorableIndex<S> index) {
        if (mAllIndexInfoMap.get(index) instanceof ManagedIndex) {
            // Index is managed by this storage, which is typical.
            return null;
        }
        // Index is managed by master storage, most likely a primary key index.
        return mMasterStorage;
    }

    public SortBuffer<S> createSortBuffer() {
        return new MergeSortBuffer<S>();
    }

    public long countAll() throws FetchException {
        return mMasterStorage.query().count();
    }

    public Cursor<S> fetchAll() throws FetchException {
        return mMasterStorage.query().fetch();
    }

    public Cursor<S> fetchOne(StorableIndex<S> index,
                              Object[] identityValues)
        throws FetchException
    {
        ManagedIndex<S> indexInfo = (ManagedIndex<S>) mAllIndexInfoMap.get(index);
        return indexInfo.fetchOne(this, identityValues);
    }

    public Query<?> indexEntryQuery(StorableIndex<S> index)
        throws FetchException
    {
        ManagedIndex<S> indexInfo = (ManagedIndex<S>) mAllIndexInfoMap.get(index);
        return indexInfo.getIndexEntryStorage().query();
    }

    public Cursor<S> fetchFromIndexEntryQuery(StorableIndex<S> index, Query<?> indexEntryQuery)
        throws FetchException
    {
        ManagedIndex<S> indexInfo = (ManagedIndex<S>) mAllIndexInfoMap.get(index);
        return indexInfo.fetchFromIndexEntryQuery(this, indexEntryQuery);
    }

    public Cursor<S> fetchSubset(StorableIndex<S> index,
                                 Object[] identityValues,
                                 BoundaryType rangeStartBoundary,
                                 Object rangeStartValue,
                                 BoundaryType rangeEndBoundary,
                                 Object rangeEndValue,
                                 boolean reverseRange,
                                 boolean reverseOrder)
        throws FetchException
    {
        // This method should never be called since a query was returned by indexEntryQuery.
        throw new UnsupportedOperationException();
    }

    private void registerIndex(ManagedIndex<S> managedIndex)
        throws RepositoryException
    {
        StorableIndex index = managedIndex.getIndex();

        if (StoredIndexInfo.class.isAssignableFrom(getStorableType())) {
            throw new IllegalStateException("StoredIndexInfo cannot have indexes");
        }
        StoredIndexInfo info = mRepository.getWrappedRepository()
            .storageFor(StoredIndexInfo.class).prepare();
        info.setIndexName(index.getNameDescriptor());

        try {
            Transaction txn = mRepository.getWrappedRepository()
                .enterTopTransaction(IsolationLevel.READ_COMMITTED);
            try {
                if (info.tryLoad()) {
                    // Index already exists and is registered.
                    return;
                }
            } finally {
                txn.exit();
            }
        } catch (FetchException e) {
            if (e instanceof FetchDeadlockException || e instanceof FetchTimeoutException) {
                // Might be caused by coarse locks. Switch to nested
                // transaction to share the locks.
                Transaction txn = mRepository.getWrappedRepository()
                    .enterTransaction(IsolationLevel.READ_COMMITTED);
                try {
                    if (info.tryLoad()) {
                        // Index already exists and is registered.
                        return;
                    }
                } finally {
                    txn.exit();
                }
            } else {
                throw e;
            }
        }

        // New index, so build it.
        managedIndex.buildIndex(mRepository.getIndexRepairThrottle());

        boolean top = true;
        while (true) {
            try {
                Transaction txn;
                if (top) {
                    txn = mRepository.getWrappedRepository()
                        .enterTopTransaction(IsolationLevel.READ_COMMITTED);
                } else {
                    txn = mRepository.getWrappedRepository()
                        .enterTransaction(IsolationLevel.READ_COMMITTED);
                }

                txn.setForUpdate(true);
                try {
                    if (!info.tryLoad()) {
                        info.setIndexTypeDescriptor(index.getTypeDescriptor());
                        info.setCreationTimestamp(System.currentTimeMillis());
                        info.setVersionNumber(0);
                        info.insert();
                        txn.commit();
                    }
                } finally {
                    txn.exit();
                }

                break;
            } catch (RepositoryException e) {
                if (e instanceof FetchDeadlockException ||
                    e instanceof FetchTimeoutException ||
                    e instanceof PersistDeadlockException ||
                    e instanceof PersistTimeoutException)
                {
                    // Might be caused by coarse locks. Switch to nested
                    // transaction to share the locks.
                    if (top) {
                        top = false;
                        continue;
                    }
                }
                throw e;
            }
        }
    }

    private void unregisterIndex(StorableIndex index) throws RepositoryException {
        if (StoredIndexInfo.class.isAssignableFrom(getStorableType())) {
            // Can't unregister when register wasn't allowed.
            return;
        }
        unregisterIndex(index.getNameDescriptor());
    }

    private void unregisterIndex(String indexName) throws RepositoryException {
        StoredIndexInfo info = mRepository.getWrappedRepository()
            .storageFor(StoredIndexInfo.class).prepare();
        info.setIndexName(indexName);
        info.tryDelete();
    }

    @SuppressWarnings("unchecked")
    private void removeIndex(StorableIndex index) throws RepositoryException {
        Log log = LogFactory.getLog(IndexedStorage.class);
        if (log.isInfoEnabled()) {
            StringBuilder b = new StringBuilder();
            b.append("Removing index on ");
            b.append(getStorableType().getName());
            b.append(": ");
            try {
                index.appendTo(b);
            } catch (java.io.IOException e) {
                // Not gonna happen.
            }
            log.info(b.toString());
        }

        Class<? extends Storable> indexEntryClass =
            IndexEntryGenerator.getIndexAccess(index).getReferenceClass();

        Storage<?> indexEntryStorage;
        try {
            indexEntryStorage = mRepository.getIndexEntryStorageFor(indexEntryClass);
        } catch (Exception e) {
            // Assume it doesn't exist.
            unregisterIndex(index);
            return;
        }

        {
            // Doesn't completely remove the index, but it should free up space.

            double desiredSpeed = mRepository.getIndexRepairThrottle();
            Throttle throttle = desiredSpeed < 1.0 ? new Throttle(BUILD_THROTTLE_WINDOW) : null;

            if (throttle == null) {
                indexEntryStorage.truncate();
            } else {
                long totalDropped = 0;
                while (true) {
                    Transaction txn = mRepository.getWrappedRepository()
                        .enterTopTransaction(IsolationLevel.READ_COMMITTED);
                    txn.setForUpdate(true);
                    try {
                        Cursor<? extends Storable> cursor = indexEntryStorage.query().fetch();
                        if (!cursor.hasNext()) {
                            break;
                        }
                        int count = 0;
                        final long savedTotal = totalDropped;
                        boolean anyFailure = false;
                        try {
                            while (count++ < BUILD_BATCH_SIZE && cursor.hasNext()) {
                                if (cursor.next().tryDelete()) {
                                    totalDropped++;
                                } else {
                                    anyFailure = true;
                                }
                            }
                        } finally {
                            cursor.close();
                        }
                        txn.commit();
                        if (log.isInfoEnabled()) {
                            log.info("Removed " + totalDropped + " index entries");
                        }
                        if (anyFailure && totalDropped <= savedTotal) {
                            log.warn("No indexes removed in last batch. " +
                                     "Aborting index removal cleanup");
                            break;
                        }
                    } catch (FetchException e) {
                        throw e.toPersistException();
                    } finally {
                        txn.exit();
                    }

                    try {
                        throttle.throttle(desiredSpeed, BUILD_THROTTLE_SLEEP_PRECISION);
                    } catch (InterruptedException e) {
                        throw new RepositoryException("Index removal interrupted");
                    }
                }
            }
        }

        unregisterIndex(index);
    }
}
