/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.qe.FilteringScore;
import com.amazon.carbonado.qe.StorableIndexSet;

import com.amazon.carbonado.synthetic.SyntheticStorableReferenceAccess;

/**
 * Builds various sets of indexes for a Storable type.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
class IndexAnalysis<S extends Storable> {

    final IndexedRepository repository;
    final Storage<S> masterStorage;

    // The set of indexes that can actually be used for querying. If index
    // repair is enabled, this set will be the same as desiredIndexSet.
    // Otherwise, it will be the intersection of existingIndexSet and
    // desiredIndexSet. In both cases, "free" indexes are added to the set too.
    final StorableIndexSet<S> queryableIndexSet;

    // The set of indexes that should be removed and no longer managed. If
    // index repair is enabled, this set will be the existingIndexSet minus
    // desiredIndexSet minus freeIndexSet plus bogusIndexSet. Otherwise, it
    // will be empty.
    final StorableIndexSet<S> removeIndexSet;

    // The set of indexes that should be freshly populated. If index repair is
    // enabled, this set will be the desiredIndexSet minus existingIndexSet
    // minus freeIndexSet. Otherwise, it will be empty.
    final StorableIndexSet<S> addIndexSet;

    // Maps free and managed indexes to IndexInfo and ManagedIndex objects.
    final Map<StorableIndex<S>, IndexInfo> allIndexInfoMap;

    // Trigger which must be installed to keep managed indexes up to date. Is
    // null if there are no managed indexes.
    final IndexesTrigger<S> indexesTrigger;

    // The set of derived-to properties in external storables that are used by
    // indexes. Is null if none.
    final Set<ChainedProperty<?>> derivedToDependencies;

    public IndexAnalysis(IndexedRepository repository, Storage<S> masterStorage)
        throws RepositoryException
    {
        this.repository = repository;
        this.masterStorage = masterStorage;

        StorableInfo<S> info = StorableIntrospector.examine(masterStorage.getStorableType());

        // The set of indexes that the Storable defines, reduced.
        final StorableIndexSet<S> desiredIndexSet;
        {
            desiredIndexSet = gatherDesiredIndexes(info);
            desiredIndexSet.reduce(Direction.ASCENDING);
            if (repository.isAllClustered()) {
                desiredIndexSet.markClustered(true);
            }            
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
                .with(info.getStorableType().getName() + '~')
                .with(info.getStorableType().getName() + '~' + '\uffff');

            List<StoredIndexInfo> storedInfos;
            try {
                Transaction txn = repository.getWrappedRepository()
                    .enterTopTransaction(IsolationLevel.READ_COMMITTED);
                try {
                    storedInfos = query.fetch().toList();
                } finally {
                    txn.exit();
                }
            } catch (FetchException e) {
                if (e instanceof FetchDeadlockException || e instanceof FetchTimeoutException) {
                    // Might be caused by coarse locks. Switch to nested
                    // transaction to share the locks.
                    Transaction txn = repository.getWrappedRepository()
                        .enterTransaction(IsolationLevel.READ_COMMITTED);
                    try {
                        storedInfos = query.fetch().toList();
                    } finally {
                        txn.exit();
                    }
                } else {
                    throw e;
                }
            }

            for (StoredIndexInfo indexInfo : storedInfos) {
                String name = indexInfo.getIndexName();
                StorableIndex index;
                try {
                    index = StorableIndex.parseNameDescriptor(name, info);
                } catch (IllegalArgumentException e) {
                    // Skip unrecognized descriptors.
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

        // The set of free indexes, which are already provided by the underlying
        // storage. They can be used for querying, but we should not manage them.
        final StorableIndexSet<S> freeIndexSet;
        {
            freeIndexSet = new StorableIndexSet<S>();
            allIndexInfoMap = new IdentityHashMap<StorableIndex<S>, IndexInfo>();

            IndexInfoCapability cap = repository.getWrappedRepository()
                .getCapability(IndexInfoCapability.class);

            if (cap != null) {
                for (IndexInfo ii : cap.getIndexInfo(info.getStorableType())) {
                    StorableIndex<S> freeIndex;
                    try {
                        freeIndex = new StorableIndex<S>(info.getStorableType(), ii);
                    } catch (IllegalArgumentException e) {
                        // Assume index is malformed, so ignore it.
                        continue;
                    }
                    if (repository.isAllClustered()) {
                        freeIndex = freeIndex.clustered(true);
                    }
                    freeIndexSet.add(freeIndex);
                    allIndexInfoMap.put(freeIndex, ii);
                }
            }
        }

        {
            queryableIndexSet = new StorableIndexSet<S>(desiredIndexSet);

            if (!repository.isIndexRepairEnabled()) {
                // Can only query the intersection.
                queryableIndexSet.retainAll(existingIndexSet);
            }

            // Add the indexes we get for free.
            queryableIndexSet.addAll(freeIndexSet);
        }

        // The set of indexes that should be kept up-to-date. If index repair
        // is enabled, this set will be the same as desiredIndexSet. Otherwise,
        // it will be the union of existingIndexSet and desiredIndexSet. In
        // both cases, "free" indexes are removed from the set too. By doing a
        // union, no harm is caused by changing the index set and then
        // reverting.
        final StorableIndexSet<S> managedIndexSet;
        {
            managedIndexSet = new StorableIndexSet<S>(desiredIndexSet);

            if (repository.isIndexRepairEnabled()) {
                // Must manage the union.
                managedIndexSet.addAll(existingIndexSet);
            }

            // Remove the indexes we get for free.
            managedIndexSet.removeAll(freeIndexSet);
        }

        {
            removeIndexSet = new StorableIndexSet<S>();

            if (repository.isIndexRepairEnabled()) {
                removeIndexSet.addAll(existingIndexSet);
                removeIndexSet.removeAll(desiredIndexSet);
                removeIndexSet.removeAll(freeIndexSet);
                removeIndexSet.addAll(bogusIndexSet);
            }
        }

        {
            addIndexSet = new StorableIndexSet<S>();

            if (repository.isIndexRepairEnabled()) {
                addIndexSet.addAll(desiredIndexSet);
                addIndexSet.removeAll(existingIndexSet);
                addIndexSet.removeAll(freeIndexSet);
            }
        }

        // Support for managed indexes...
        if (managedIndexSet.size() <= 0) {
            indexesTrigger = null;
        } else {
            ManagedIndex<S>[] managedIndexes = new ManagedIndex[managedIndexSet.size()];
            int i = 0;
            for (StorableIndex<S> index : managedIndexSet) {
                SyntheticStorableReferenceAccess<S> accessor =
                    IndexEntryGenerator.getIndexAccess(index);
                Class<? extends Storable> indexEntryClass = accessor.getReferenceClass();
                Storage<?> indexEntryStorage = repository.getIndexEntryStorageFor(indexEntryClass);
                ManagedIndex managedIndex = new ManagedIndex<S>
                    (repository, masterStorage, index, accessor, indexEntryStorage);

                allIndexInfoMap.put(index, managedIndex);
                managedIndexes[i++] = managedIndex;
            }

            indexesTrigger = new IndexesTrigger<S>(managedIndexes);
        }

        derivedToDependencies = gatherDerivedToDependencies(info);
    }

    static <S extends Storable> StorableIndexSet<S> gatherDesiredIndexes(StorableInfo<S> info) {
        StorableIndexSet<S> indexSet = new StorableIndexSet<S>();
        indexSet.addIndexes(info);
        indexSet.addAlternateKeys(info);

        // If any join properties are used by indexed derived properties, make
        // sure join internal properties are indexed.

        for (StorableProperty<S> property : info.getAllProperties().values()) {
            if (!isJoinAndUsedByIndexedDerivedProperty(property)) {
                continue;
            }

            // Internal properties of join need to be indexed. Check if a
            // suitable index exists before defining a new one.

            Filter<S> filter = Filter.getOpenFilter(info.getStorableType());
            for (int i=property.getJoinElementCount(); --i>=0; ) {
                filter = filter.and(property.getInternalJoinElement(i).getName(), RelOp.EQ);
            }

            for (int i=info.getIndexCount(); --i>=0; ) {
                FilteringScore<S> score = FilteringScore.evaluate(info.getIndex(i), filter);
                if (score.getIdentityCount() == property.getJoinElementCount()) {
                    // Suitable index already exists.
                    continue;
                }
            }

            Direction[] directions = new Direction[property.getJoinElementCount()];
            Arrays.fill(directions, Direction.UNSPECIFIED);

            StorableIndex<S> index =
                new StorableIndex<S>(property.getInternalJoinElements(), directions);

            indexSet.add(index);
        }

        return indexSet;
    }

    private static boolean isUsedByIndex(StorableProperty<?> property) {
        StorableInfo<?> info = StorableIntrospector.examine(property.getEnclosingType());
        for (int i=info.getIndexCount(); --i>=0; ) {
            StorableIndex<?> index = info.getIndex(i);
            int propertyCount = index.getPropertyCount();
            for (int j=0; j<propertyCount; j++) {
                if (index.getProperty(j).equals(property)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isJoinAndUsedByIndexedDerivedProperty(StorableProperty<?> property) {
        if (property.isJoin()) {
            for (ChainedProperty<?> derivedTo : property.getDerivedToProperties()) {
                if (isUsedByIndex(derivedTo.getPrimeProperty())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Set<ChainedProperty<?>> gatherDerivedToDependencies(StorableInfo<?> info) {
        Set<ChainedProperty<?>> set = null;
        for (StorableProperty<?> property : info.getAllProperties().values()) {
            for (ChainedProperty<?> derivedTo : property.getDerivedToProperties()) {
                if (derivedTo.getChainCount() > 0 && isUsedByIndex(derivedTo.getPrimeProperty())) {
                    if (set == null) {
                        set = new HashSet<ChainedProperty<?>>();
                    }
                    set.add(derivedTo);
                }
            }
        }
        return set;
    }
}
