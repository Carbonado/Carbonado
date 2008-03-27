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

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;

import com.amazon.carbonado.cursor.ArraySortBuffer;
import com.amazon.carbonado.cursor.MergeSortBuffer;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
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

    private Storage<S> mRootStorage;

    @SuppressWarnings("unchecked")
    IndexedStorage(IndexedRepository repository, Storage<S> masterStorage)
        throws RepositoryException
    {
        mRepository = repository;
        mMasterStorage = masterStorage;
        mAllIndexInfoMap = new IdentityHashMap<StorableIndex<S>, IndexInfo>();

        StorableInfo<S> info = StorableIntrospector.examine(masterStorage.getStorableType());

        // The set of indexes that the Storable defines, reduced.
        final StorableIndexSet<S> desiredIndexSet;
        {
            desiredIndexSet = IndexAnalysis.gatherDesiredIndexes(info);
            desiredIndexSet.reduce(Direction.ASCENDING);
        }

        // The set of indexes that are populated and available for use. This is
        // determined by examining index metadata. If the Storable has not
        // changed, it will be the same as desiredIndexSet. If any existing
        // indexes use a property whose type has changed, it is added to
        // bogusIndexSet. Bogus indexes are removed if repair is enabled.
        final StorableIndexSet<S> existingIndexSet;
        final StorableIndexSet<S> bogusIndexSet;
        {
            existingIndexSet = new StorableIndexSet<S>();
            bogusIndexSet = new StorableIndexSet<S>();

            Query<StoredIndexInfo> query = repository.getWrappedRepository()
                .storageFor(StoredIndexInfo.class)
                // Primary key of StoredIndexInfo is an index descriptor, which
                // starts with the storable type name. This emulates a
                // "wildcard at the end" search.
                .query("indexName >= ? & indexName < ?")
                .with(getStorableType().getName() + '~')
                .with(getStorableType().getName() + '~' + '\uffff');

            List<StoredIndexInfo> storedInfos;
            Transaction txn = repository.getWrappedRepository()
                .enterTopTransaction(IsolationLevel.READ_COMMITTED);
            try {
                storedInfos = query.fetch().toList();
            } finally {
                txn.exit();
            }

            for (StoredIndexInfo indexInfo : storedInfos) {
                String name = indexInfo.getIndexName();
                StorableIndex index;
                try {
                    index = StorableIndex.parseNameDescriptor(name, info);
                } catch (IllegalArgumentException e) {
                    // Remove unrecognized descriptors.
                    unregisterIndex(name);
                    continue;
                }
                if (index.getTypeDescriptor().equals(indexInfo.getIndexTypeDescriptor())) {
                    existingIndexSet.add(index);
                } else {
                    bogusIndexSet.add(index);
                }
            }
        }

        nonUniqueSearch: {
            // If any existing indexes are non-unique, then indexes are for an
            // older version. For compatibility, don't uniquify the
            // indexes. Otherwise, these indexes would need to be rebuilt.
            for (StorableIndex<S> index : existingIndexSet) {
                if (!index.isUnique()) {
                    break nonUniqueSearch;
                }
            }

            // The index implementation includes all primary key properties
            // anyhow, so adding them here allows query analyzer to see these
            // properties. As a side-effect of uniquify, all indexes are
            // unique, and thus have 'U' in the descriptor. Each time
            // nonUniqueSearch is run, it will not find any non-unique indexes.
            desiredIndexSet.uniquify(info);
        }

        // Gather free indexes, which are already provided by the underlying
        // storage. They can be used for querying, but we should not manage them.
        final StorableIndexSet<S> freeIndexSet;
        {
            freeIndexSet = new StorableIndexSet<S>();

            IndexInfoCapability cap = repository.getWrappedRepository()
                .getCapability(IndexInfoCapability.class);

            if (cap != null) {
                for (IndexInfo ii : cap.getIndexInfo(masterStorage.getStorableType())) {
                    StorableIndex<S> freeIndex;
                    try {
                        freeIndex = new StorableIndex<S>(masterStorage.getStorableType(), ii);
                    } catch (IllegalArgumentException e) {
                        // Assume index is malformed, so ignore it.
                        continue;
                    }
                    if (mRepository.isAllClustered()) {
                        freeIndex = freeIndex.clustered(true);
                    }
                    mAllIndexInfoMap.put(freeIndex, ii);
                    freeIndexSet.add(freeIndex);
                }
            }
        }

        // The set of indexes that can actually be used for querying. If index
        // repair is enabled, this set will be the same as
        // desiredIndexSet. Otherwise, it will be the intersection of
        // existingIndexSet and desiredIndexSet. In both cases, "free" indexes
        // are added to the set too.
        final StorableIndexSet<S> queryableIndexSet;
        {
            queryableIndexSet = new StorableIndexSet<S>(desiredIndexSet);

            if (!mRepository.isIndexRepairEnabled()) {
                // Can only query the intersection.
                queryableIndexSet.retainAll(existingIndexSet);
            }

            // Add the indexes we get for free.
            queryableIndexSet.addAll(freeIndexSet);

            if (mRepository.isAllClustered()) {
                queryableIndexSet.markClustered(true);
            }
        }

        // The set of indexes that should be kept up-to-date. If index repair
        // is enabled, this set will be the same as desiredIndexSet. Otherwise,
        // it will be the union of existingIndexSet and desiredIndexSet. In
        // both cases, "free" indexes are removed from the set too. By doing a
        // union, no harm is caused by changing the index set and then reverting.
        final StorableIndexSet<S> managedIndexSet;
        {
            managedIndexSet = new StorableIndexSet<S>(desiredIndexSet);

            if (!mRepository.isIndexRepairEnabled()) {
                // Must manage the union.
                managedIndexSet.addAll(existingIndexSet);
            }

            // Remove the indexes we get for free.
            managedIndexSet.removeAll(freeIndexSet);
        }

        // The set of indexes that should be removed and no longer managed. If
        // index repair is enabled, this set will be the existingIndexSet minus
        // desiredIndexSet minus freeIndexSet plus bogusIndexSet. Otherwise, it
        // will be empty.
        final StorableIndexSet<S> removeIndexSet;
        {
            removeIndexSet = new StorableIndexSet<S>();

            if (mRepository.isIndexRepairEnabled()) {
                removeIndexSet.addAll(existingIndexSet);
                removeIndexSet.removeAll(desiredIndexSet);
                removeIndexSet.removeAll(freeIndexSet);
                removeIndexSet.addAll(bogusIndexSet);
            }
        }

        // The set of indexes that should be freshly populated. If index repair
        // is enabled, this set will be the desiredIndexSet minus
        // existingIndexSet minus freeIndexSet. Otherwise, it will be empty.
        final StorableIndexSet<S> addIndexSet;
        {
            addIndexSet = new StorableIndexSet<S>();

            if (mRepository.isIndexRepairEnabled()) {
                addIndexSet.addAll(desiredIndexSet);
                addIndexSet.removeAll(existingIndexSet);
                addIndexSet.removeAll(freeIndexSet);
            }
        }

        // Support for managed indexes...
        if (managedIndexSet.size() > 0) {
            ManagedIndex<S>[] managedIndexes = new ManagedIndex[managedIndexSet.size()];
            int i = 0;
            for (StorableIndex<S> index : managedIndexSet) {
                IndexEntryGenerator<S> builder = IndexEntryGenerator.getInstance(index);
                Class<? extends Storable> indexEntryClass = builder.getIndexEntryClass();
                Storage<?> indexEntryStorage = repository.getIndexEntryStorageFor(indexEntryClass);
                ManagedIndex managedIndex = new ManagedIndex<S>(index, builder, indexEntryStorage);

                mAllIndexInfoMap.put(index, managedIndex);
                managedIndexes[i++] = managedIndex;
            }

            if (!addTrigger(new IndexesTrigger<S>(managedIndexes))) {
                throw new RepositoryException("Unable to add trigger for managing indexes");
            }
        }

        // Okay, now start doing some damage. First, remove unnecessary indexes.
        for (StorableIndex<S> index : removeIndexSet) {
            removeIndex(index);
        }

        // Now add new indexes.
        for (StorableIndex<S> index : addIndexSet) {
            registerIndex((ManagedIndex) mAllIndexInfoMap.get(index));
        }

        mQueryableIndexSet = queryableIndexSet;
        mQueryEngine = new QueryEngine<S>(masterStorage.getStorableType(), repository);

        // Install triggers to manage derived properties in external Storables.

        Set<ChainedProperty<?>> derivedToDependencies =
            IndexAnalysis.gatherDerivedToDependencies(info);

        if (derivedToDependencies != null) {
            for (ChainedProperty<?> derivedTo : derivedToDependencies) {
                addTrigger(new DerivedIndexesTrigger(repository, getStorableType(), derivedTo));
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

    // Required by IndexInfoCapability.
    public IndexInfo[] getIndexInfo() {
        IndexInfo[] infos = new IndexInfo[mAllIndexInfoMap.size()];
        return mAllIndexInfoMap.values().toArray(infos);
    }

    // Required by IndexEntryAccessCapability.
    @SuppressWarnings("unchecked")
    public IndexEntryAccessor<S>[] getIndexEntryAccessors() {
        List<IndexEntryAccessor<S>> accessors =
            new ArrayList<IndexEntryAccessor<S>>(mAllIndexInfoMap.size());
        for (IndexInfo info : mAllIndexInfoMap.values()) {
            if (info instanceof IndexEntryAccessor) {
                accessors.add((IndexEntryAccessor<S>) info);
            }
        }
        return accessors.toArray(new IndexEntryAccessor[accessors.size()]);
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
        // FIXME: This is messy. If Storables had built-in serialization
        // support, then MergeSortBuffer would not need a root storage.
        if (mRootStorage == null) {
            try {
                mRootStorage = mRepository.getRootRepository().storageFor(getStorableType());
            } catch (RepositoryException e) {
                LogFactory.getLog(IndexedStorage.class).warn(null, e);
                return new ArraySortBuffer<S>();
            }
        }

        return new MergeSortBuffer<S>(mRootStorage);
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

        // New index, so populate it.
        managedIndex.populateIndex(mRepository, mMasterStorage,
                                   mRepository.getIndexRepairThrottle());

        txn = mRepository.getWrappedRepository()
            .enterTopTransaction(IsolationLevel.READ_COMMITTED);
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
            IndexEntryGenerator.getInstance(index).getIndexEntryClass();

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
            Throttle throttle = desiredSpeed < 1.0 ? new Throttle(POPULATE_THROTTLE_WINDOW) : null;

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
                            while (count++ < POPULATE_BATCH_SIZE && cursor.hasNext()) {
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
                        throttle.throttle(desiredSpeed, POPULATE_THROTTLE_SLEEP_PRECISION);
                    } catch (InterruptedException e) {
                        throw new RepositoryException("Index removal interrupted");
                    }
                }
            }
        }

        unregisterIndex(index);
    }
}
