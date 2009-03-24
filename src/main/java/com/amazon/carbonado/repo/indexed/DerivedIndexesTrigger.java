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

import java.util.ArrayList;
import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.info.ChainedProperty;

/**
 * Handles index updates for derived-to properties.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
class DerivedIndexesTrigger<S extends Storable, D extends Storable> extends Trigger<S> {
    private final DependentStorableFetcher<S, D> mFetcher;

    /**
     * @param derivedTo special chained property from StorableProperty.getDerivedToProperties
     */
    DerivedIndexesTrigger(IndexedRepository repository,
                          Class<S> sType, ChainedProperty<D> derivedTo)
        throws RepositoryException
    {
        this(new DependentStorableFetcher(repository, sType, derivedTo));
    }

    private DerivedIndexesTrigger(DependentStorableFetcher<S, D> fetcher) {
        mFetcher = fetcher;
    }

    @Override
    public Object beforeInsert(S storable) throws PersistException {
        return createDependentIndexEntries(storable);
    }

    @Override
    public void afterInsert(S storable, Object state) throws PersistException {
        updateValues(storable, state);
    }

    @Override
    public Object beforeUpdate(S storable) throws PersistException {
        return createDependentIndexEntries(storable);
    }

    @Override
    public void afterUpdate(S storable, Object state) throws PersistException {
        updateValues(storable, state);
    }

    @Override
    public Object beforeDelete(S storable) throws PersistException {
        try {
            if (storable.copy().tryLoad()) {
                return createDependentIndexEntries(storable);
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }
        return null;
    }

    @Override
    public void afterDelete(S storable, Object state) throws PersistException {
        updateValues(storable, state);
    }

    @Override
    public int hashCode() {
        return mFetcher.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof DerivedIndexesTrigger) {
            DerivedIndexesTrigger other = (DerivedIndexesTrigger) obj;
            return mFetcher.equals(other.mFetcher);
        }
        return false;
    }

    private List<Storable> createDependentIndexEntries(S storable) throws PersistException {
        List<Storable> dependentIndexEntries = new ArrayList<Storable>();
        createDependentIndexEntries(storable, dependentIndexEntries);
        return dependentIndexEntries;
    }

    private void createDependentIndexEntries(S storable, List<Storable> dependentIndexEntries)
        throws PersistException
    {
        try {
            Transaction txn = mFetcher.enterTransaction();
            try {
                // Make sure write lock is acquired when reading dependencies
                // since they might be updated later. Locks are held after this
                // transaction exits since it is nested in the trigger's transaction.
                txn.setForUpdate(true);

                Cursor<D> dependencies = mFetcher.fetchDependenentStorables(storable);
                try {
                    while (dependencies.hasNext()) {
                        mFetcher.createIndexEntries(dependencies.next(), dependentIndexEntries);
                    }
                } finally {
                    dependencies.close();
                }
            } finally {
                txn.exit();
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    private void updateValues(S storable, Object state) throws PersistException {
        if (state == null) {
            return;
        }

        List<Storable> oldIndexEntries = (List<Storable>) state;
        int size = oldIndexEntries.size();

        List<Storable> newIndexEntries = new ArrayList<Storable>(size);
        createDependentIndexEntries(storable, newIndexEntries);

        if (size != newIndexEntries.size()) {
            // This is not expected to happen.
            throw new PersistException("Amount of affected dependent indexes changed: " +
                                       size + " != " + newIndexEntries.size());
        }

        for (int i=0; i<size; i++) {
            Storable oldIndexEntry = oldIndexEntries.get(i);
            Storable newIndexEntry = newIndexEntries.get(i);
            if (!oldIndexEntry.equalProperties(newIndexEntry)) {
                // Try delete old entry, just in case it is missing.
                oldIndexEntry.tryDelete();
            }
            // Always try to insert index entry, just in case it is missing.
            newIndexEntry.tryInsert();
        }
    }
}
