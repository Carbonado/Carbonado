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
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;

import com.amazon.carbonado.cursor.ArraySortBuffer;
import com.amazon.carbonado.cursor.MergeSortBuffer;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.cursor.SortBuffer;

import com.amazon.carbonado.qe.BoundaryType;
import com.amazon.carbonado.qe.QueryEngine;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.spi.RepairExecutor;
import com.amazon.carbonado.spi.StorableIndexSet;

/**
 *
 *
 * @author Brian S O'Neill
 */
class IndexedStorage<S extends Storable> implements Storage<S>, StorageAccess<S> {
    static <S extends Storable> StorableIndexSet<S> gatherRequiredIndexes(StorableInfo<S> info) {
        StorableIndexSet<S> indexSet = new StorableIndexSet<S>();
        indexSet.addIndexes(info);
        indexSet.addAlternateKeys(info);
        return indexSet;
    }

    final IndexedRepository mRepository;
    final Storage<S> mMasterStorage;

    private final Map<StorableIndex<S>, IndexInfo> mIndexInfoMap;
    private final StorableIndexSet<S> mIndexSet;

    private final QueryEngine<S> mQueryEngine;

    private Storage<S> mRootStorage;

    @SuppressWarnings("unchecked")
    IndexedStorage(IndexedRepository repository, Storage<S> masterStorage)
        throws RepositoryException
    {
        mRepository = repository;
        mMasterStorage = masterStorage;
        mIndexInfoMap = new IdentityHashMap<StorableIndex<S>, IndexInfo>();

        StorableInfo<S> info = StorableIntrospector.examine(masterStorage.getStorableType());

        // Determine what the set of indexes should be.
        StorableIndexSet<S> newIndexSet = gatherRequiredIndexes(info);

        // Mix in the indexes we get for free, but remove after reduce. A free
        // index is one that the underlying storage is providing for us. We
        // don't want to create redundant indexes.
        IndexInfo[] infos = repository.getWrappedRepository()
            .getCapability(IndexInfoCapability.class)
            .getIndexInfo(masterStorage.getStorableType());

        StorableIndex<S>[] freeIndexes = new StorableIndex[infos.length];
        for (int i=0; i<infos.length; i++) {
            try {
                freeIndexes[i] = new StorableIndex<S>(masterStorage.getStorableType(), infos[i]);
                newIndexSet.add(freeIndexes[i]);
                mIndexInfoMap.put(freeIndexes[i], infos[i]);
            } catch (IllegalArgumentException e) {
                // Assume index is bogus, so ignore it.
            }
        }

        newIndexSet.reduce(Direction.ASCENDING);

        // Gather current indexes.
        StorableIndexSet<S> currentIndexSet = new StorableIndexSet<S>();
        // Gather indexes to remove.
        StorableIndexSet<S> indexesToRemove = new StorableIndexSet<S>();

        Query<StoredIndexInfo> query = repository.getWrappedRepository()
            .storageFor(StoredIndexInfo.class)
            // Primary key of StoredIndexInfo is an index descriptor, which
            // starts with the storable type name. This emulates a "wildcard at
            // the end" search.
            .query("indexName >= ? & indexName < ?")
            .with(getStorableType().getName() + '~')
            .with(getStorableType().getName() + '~' + '\uffff');

        for (StoredIndexInfo indexInfo : query.fetch().toList()) {
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
                currentIndexSet.add(index);
            } else {
                indexesToRemove.add(index);
            }
        }

        nonUniqueSearch: {
            // If any current indexes are non-unique, then indexes are for an
            // older version. For compatibility, don't uniquify the
            // indexes. Otherwise, these indexes would need to be rebuilt.
            for (StorableIndex<S> index : currentIndexSet) {
                if (!index.isUnique()) {
                    break nonUniqueSearch;
                }
            }

            // The index implementation includes all primary key properties
            // anyhow, so adding them here allows query analyzer to see these
            // properties. As a side-effect of uniquify, all indexes are
            // unique, and thus have 'U' in the descriptor. Each time
            // nonUniqueSearch is run, it will not find any non-unique indexes.
            newIndexSet.uniquify(info);
        }

        // Remove any old indexes.
        {
            indexesToRemove.addAll(currentIndexSet);

            // Remove "free" indexes, since they don't need to be built.
            for (int i=0; i<freeIndexes.length; i++) {
                newIndexSet.remove(freeIndexes[i]);
            }

            indexesToRemove.removeAll(newIndexSet);

            for (StorableIndex<S> index : indexesToRemove) {
                removeIndex(index);
            }
        }

        currentIndexSet = newIndexSet;

        // Open all the indexes.
        List<ManagedIndex<S>> managedIndexList = new ArrayList<ManagedIndex<S>>();
        for (StorableIndex<S> index : currentIndexSet) {
            IndexEntryGenerator<S> builder = IndexEntryGenerator.getInstance(index);
            Class<? extends Storable> indexEntryClass = builder.getIndexEntryClass();
            Storage<?> indexEntryStorage = repository.getIndexEntryStorageFor(indexEntryClass);
            ManagedIndex managedIndex = new ManagedIndex<S>(index, builder, indexEntryStorage);

            registerIndex(managedIndex);

            mIndexInfoMap.put(index, managedIndex);
            managedIndexList.add(managedIndex);
        }

        if (managedIndexList.size() > 0) {
            // Add trigger to keep indexes up-to-date.
            ManagedIndex<S>[] managedIndexes =
                managedIndexList.toArray(new ManagedIndex[managedIndexList.size()]);

            if (!addTrigger(new IndexesTrigger<S>(managedIndexes))) {
                throw new RepositoryException("Unable to add trigger for managing indexes");
            }
        }

        // Add "free" indexes back, in order for query engine to consider them.
        for (int i=0; i<freeIndexes.length; i++) {
            currentIndexSet.add(freeIndexes[i]);
        }

        mIndexSet = currentIndexSet;

        mQueryEngine = new QueryEngine<S>(masterStorage.getStorableType(), repository);
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

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mMasterStorage.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mMasterStorage.removeTrigger(trigger);
    }

    public IndexInfo[] getIndexInfo() {
        IndexInfo[] infos = new IndexInfo[mIndexInfoMap.size()];
        return mIndexInfoMap.values().toArray(infos);
    }

    @SuppressWarnings("unchecked")
    public IndexEntryAccessor<S>[] getIndexEntryAccessors() {
        List<IndexEntryAccessor<S>> accessors =
            new ArrayList<IndexEntryAccessor<S>>(mIndexInfoMap.size());
        for (IndexInfo info : mIndexInfoMap.values()) {
            if (info instanceof IndexEntryAccessor) {
                accessors.add((IndexEntryAccessor<S>) info);
            }
        }
        return accessors.toArray(new IndexEntryAccessor[accessors.size()]);
    }

    public Collection<StorableIndex<S>> getAllIndexes() {
        return mIndexSet;
    }

    public Storage<S> storageDelegate(StorableIndex<S> index) {
        if (mIndexInfoMap.get(index) instanceof ManagedIndex) {
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

        // FIXME: sort buffer should be on repository access. Also, create abstract
        // repository access that creates the correct merge sort buffer. And more:
        // create capability for managing merge sort buffers.
        return new MergeSortBuffer<S>(mRootStorage);
    }

    public Cursor<S> fetchAll() throws FetchException {
        return mMasterStorage.query().fetch();
    }

    public Cursor<S> fetchOne(StorableIndex<S> index,
                              Object[] identityValues)
        throws FetchException
    {
        // TODO: optimize fetching one by loading storable by primary key
        return fetchSubset(index, identityValues,
                           BoundaryType.OPEN, null,
                           BoundaryType.OPEN, null,
                           false, false);
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
        // Note: this code ignores the reverseRange parameter to avoid double
        // reversal. Only the lowest storage layer should examine this
        // parameter.

        ManagedIndex<S> indexInfo = (ManagedIndex<S>) mIndexInfoMap.get(index);

        Query<?> query = indexInfo.getIndexEntryQueryFor
            (identityValues == null ? 0 : identityValues.length,
             rangeStartBoundary, rangeEndBoundary, reverseOrder);

        if (identityValues != null) {
            query = query.withValues(identityValues);
        }

        if (rangeStartBoundary != BoundaryType.OPEN) {
            query = query.with(rangeStartValue);
        }
        if (rangeEndBoundary != BoundaryType.OPEN) {
            query = query.with(rangeEndValue);
        }

        Cursor<? extends Storable> indexEntryCursor = query.fetch();

        return new IndexedCursor<S>
            (indexEntryCursor, IndexedStorage.this, indexInfo.getIndexEntryClassBuilder());
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

        if (info.tryLoad()) {
            // Index already exists and is registered.
            return;
        }

        // New index, so populate it.
        managedIndex.populateIndex(mRepository, mMasterStorage);

        Transaction txn = mRepository.getWrappedRepository().enterTransaction();
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
        info.delete();
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

        // Doesn't completely remove the index, but it should free up space.
        // TODO: when truncate method exists, call that instead
        indexEntryStorage.query().deleteAll();
        unregisterIndex(index);
    }
}
