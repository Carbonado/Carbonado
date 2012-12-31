/*
 * Copyright 2006-2012 Amazon Technologies, Inc. or its affiliates.
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

import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

/**
 * Handles index updates.
 *
 * @author Brian S O'Neill
 */
class IndexesTrigger<S extends Storable> extends Trigger<S> {
    private final ManagedIndex<S>[] mManagedIndexes;

    /**
     * @param managedIndexes all the indexes that need to be updated.
     */
    IndexesTrigger(ManagedIndex<S>[] managedIndexes) {
        mManagedIndexes = managedIndexes;
    }

    @Override
    public void afterInsert(S storable, Object state) throws PersistException {
        for (ManagedIndex<S> managed : mManagedIndexes) {
            if (!managed.insertIndexEntry(storable)) {
                throw new UniqueConstraintException
                    ("Alternate key constraint: " + storable.toString() +
                     ", " + managed);
            }
        }
    }

    @Override
    public void afterTryInsert(S storable, Object state) throws PersistException {
        for (ManagedIndex<S> managed : mManagedIndexes) {
            if (!managed.insertIndexEntry(storable)) {
                throw abortTry();
            }
        }
    }

    @Override
    public Object beforeTryUpdate(Transaction txn, S storable) throws PersistException {
        return beforeUpdate(txn, storable);
    }

    @Override
    public Object beforeUpdate(Transaction txn, S storable) throws PersistException {
        // Ensure old storable is loaded with an upgradable lock, allowing
        // update to proceed without deadlock.
        txn.setForUpdate(true);

        // Return old storable for afterUpdate.
        S copy = (S) storable.copy();
        try {
            if (copy.tryLoad()) {
                return copy;
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }
        // If this point is reached, then afterUpdate is not called because
        // update will fail.
        return null;
    }

    @Override
    public void afterUpdate(S storable, Object state) throws PersistException {
        // Cast old storable as provided by beforeUpdate.
        S oldStorable = (S) state;
        for (ManagedIndex<S> managed : mManagedIndexes) {
            managed.updateIndexEntry(storable, oldStorable);
        }
    }

    @Override
    public Object beforeTryDelete(Transaction txn, S storable) throws PersistException {
        return beforeDelete(txn, storable);
    }

    @Override
    public Object beforeDelete(Transaction txn, S storable) throws PersistException {
        // Ensure old storable is loaded with an upgradable lock, allowing
        // delete to proceed without deadlock.
        txn.setForUpdate(true);

        // Delete index entries referenced by existing storable.
        S copy = (S) storable.copy();
        try {
            try {
                if (!copy.tryLoad()) {
                    return null;
                }
            } catch (CorruptEncodingException e) {
                LogFactory.getLog(IndexedStorage.class)
                    .warn("Unable to delete index entries because primary record is corrupt: " +
                          copy.toStringKeyOnly(), e);
                return null;
            }

            for (ManagedIndex<S> managed : mManagedIndexes) {
                managed.deleteIndexEntry(copy);
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }
        return null;
    }
}
