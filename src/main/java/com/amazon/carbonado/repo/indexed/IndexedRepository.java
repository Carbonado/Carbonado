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
import java.util.Arrays;
import java.util.Map;
import java.util.IdentityHashMap;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.Repository;
import static com.amazon.carbonado.RepositoryBuilder.RepositoryReference;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.capability.Capability;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.capability.StorableInfoCapability;

import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.qe.RepositoryAccess;
import com.amazon.carbonado.qe.StorageAccess;

/**
 * Wraps another repository in order to make it support indexes. The wrapped
 * repository must support creation of new types.
 *
 * @author Brian S O'Neill
 */
class IndexedRepository implements Repository,
                                   RepositoryAccess,
                                   IndexInfoCapability,
                                   StorableInfoCapability,
                                   IndexEntryAccessCapability
{
    private final RepositoryReference mRootRef;
    private final Repository mRepository;
    private final String mName;
    private final Map<Class<?>, IndexedStorage<?>> mStorages;

    IndexedRepository(RepositoryReference rootRef, String name, Repository repository) {
        mRootRef = rootRef;
        mRepository = repository;
        mName = name;
        mStorages = new IdentityHashMap<Class<?>, IndexedStorage<?>>();
        if (repository.getCapability(IndexInfoCapability.class) == null) {
            throw new UnsupportedOperationException
                ("Wrapped repository doesn't support being indexed");
        }
    }

    public String getName() {
        return mName;
    }

    @SuppressWarnings("unchecked")
    public <S extends Storable> Storage<S> storageFor(Class<S> type)
        throws MalformedTypeException, SupportException, RepositoryException
    {
        synchronized (mStorages) {
            IndexedStorage<S> storage = (IndexedStorage<S>) mStorages.get(type);
            if (storage == null) {
                Storage<S> masterStorage = mRepository.storageFor(type);

                if (Unindexed.class.isAssignableFrom(type)) {
                    // Verify no indexes.
                    int indexCount = IndexedStorage
                        .gatherRequiredIndexes(StorableIntrospector.examine(type)).size();
                    if (indexCount > 0) {
                        throw new MalformedTypeException
                            (type, "Storable cannot have any indexes: " + type +
                             ", " + indexCount);
                    }
                    return masterStorage;
                }

                storage = new IndexedStorage<S>(this, masterStorage);
                mStorages.put(type, storage);
            }
            return storage;
        }
    }

    public Transaction enterTransaction() {
        return mRepository.enterTransaction();
    }

    public Transaction enterTransaction(IsolationLevel level) {
        return mRepository.enterTransaction(level);
    }

    public Transaction enterTopTransaction(IsolationLevel level) {
        return mRepository.enterTopTransaction(level);
    }

    public IsolationLevel getTransactionIsolationLevel() {
        return mRepository.getTransactionIsolationLevel();
    }

    @SuppressWarnings("unchecked")
    public <C extends Capability> C getCapability(Class<C> capabilityType) {
        if (capabilityType.isInstance(this)) {
            return (C) this;
        }
        return mRepository.getCapability(capabilityType);
    }

    public <S extends Storable> IndexInfo[] getIndexInfo(Class<S> storableType)
        throws RepositoryException
    {
        return ((IndexedStorage) storageFor(storableType)).getIndexInfo();
    }

    public <S extends Storable> IndexEntryAccessor<S>[]
        getIndexEntryAccessors(Class<S> storableType)
        throws RepositoryException
    {
        return ((IndexedStorage<S>) storageFor(storableType)).getIndexEntryAccessors();
    }

    public String[] getUserStorableTypeNames() throws RepositoryException {
        StorableInfoCapability cap = mRepository.getCapability(StorableInfoCapability.class);
        if (cap == null) {
            return new String[0];
        }
        ArrayList<String> names =
            new ArrayList<String>(Arrays.asList(cap.getUserStorableTypeNames()));

        // Exclude our own metadata types as well as indexes.

        names.remove(StoredIndexInfo.class.getName());

        Cursor<StoredIndexInfo> cursor =
            mRepository.storageFor(StoredIndexInfo.class)
            .query().fetch();

        while (cursor.hasNext()) {
            StoredIndexInfo info = cursor.next();
            names.remove(info.getIndexName());
        }

        return names.toArray(new String[names.size()]);
    }

    public boolean isSupported(Class<Storable> type) {
        StorableInfoCapability cap = mRepository.getCapability(StorableInfoCapability.class);
        return (cap == null) ? null : cap.isSupported(type);
    }

    public boolean isPropertySupported(Class<Storable> type, String name) {
        StorableInfoCapability cap = mRepository.getCapability(StorableInfoCapability.class);
        return (cap == null) ? null : cap.isPropertySupported(type, name);
    }

    public void close() {
        mRepository.close();
    }

    public Repository getRootRepository() {
        return mRootRef.get();
    }

    public <S extends Storable> StorageAccess<S> storageAccessFor(Class<S> type)
        throws SupportException, RepositoryException
    {
        return (StorageAccess<S>) storageFor(type);
    }

    Storage<?> getIndexEntryStorageFor(Class<? extends Storable> indexEntryClass)
        throws RepositoryException
    {
        return mRepository.storageFor(indexEntryClass);
    }

    Repository getWrappedRepository() {
        return mRepository;
    }
}
