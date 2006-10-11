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

import java.util.NoSuchElementException;

import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.cursor.AbstractCursor;

import com.amazon.carbonado.spi.RepairExecutor;

/**
 * Wraps another cursor which contains index entries and extracts master
 * objects from them.
 *
 * @author Brian S O'Neill
 */
class IndexedCursor<S extends Storable> extends AbstractCursor<S> {
    private final Cursor<? extends Storable> mCursor;
    private final IndexedStorage<S> mStorage;
    private final IndexEntryGenerator<S> mGenerator;

    private S mNext;

    IndexedCursor(Cursor<? extends Storable> indexEntryCursor,
                  IndexedStorage<S> storage,
                  IndexEntryGenerator<S> indexEntryGenerator) {
        mCursor = indexEntryCursor;
        mStorage = storage;
        mGenerator = indexEntryGenerator;
    }

    public void close() throws FetchException {
        mCursor.close();
    }

    public boolean hasNext() throws FetchException {
        if (mNext != null) {
            return true;
        }
        while (mCursor.hasNext()) {
            final Storable indexEntry = mCursor.next();
            S master = mGenerator.loadMaster(indexEntry);
            if (master == null) {
                LogFactory.getLog(getClass()).warn
                    ("Master is missing for index entry: " + indexEntry);
            } else {
                if (mGenerator.isConsistent(indexEntry, master)) {
                    mNext = master;
                    return true;
                }

                // This index entry is stale. Repair is needed.

                // Insert a correct index entry, just to be sure.
                try {
                    final IndexedRepository repo = mStorage.mRepository;
                    final Storage<?> indexEntryStorage =
                        repo.getIndexEntryStorageFor(mGenerator.getIndexEntryClass());
                    Storable newIndexEntry = indexEntryStorage.prepare();
                    mGenerator.setAllProperties(newIndexEntry, master);

                    if (newIndexEntry.tryLoad()) {
                        // Good, the correct index entry exists. We'll see
                        // the master record eventually, so skip.
                    } else {
                        // We have no choice but to return the master, at
                        // the risk of seeing it multiple times. This is
                        // better than seeing it never.
                        LogFactory.getLog(getClass()).warn
                            ("Inconsistent index entry: " + indexEntry);
                        mNext = master;
                    }

                    // Repair the stale index entry.
                    RepairExecutor.execute(new Runnable() {
                        public void run() {
                            Transaction txn = repo.enterTransaction();
                            try {
                                // Reload master and verify inconsistency.
                                S master = mGenerator.loadMaster(indexEntry);
                                if (mGenerator.isConsistent(indexEntry, master)) {
                                    return;
                                }

                                Storable newIndexEntry = indexEntryStorage.prepare();
                                mGenerator.setAllProperties(newIndexEntry, master);

                                newIndexEntry.tryInsert();

                                indexEntry.tryDelete();
                                txn.commit();
                            } catch (FetchException fe) {
                                LogFactory.getLog(IndexedCursor.class).warn
                                    ("Unable to check if repair required for " +
                                     "inconsistent index entry " +
                                     indexEntry, fe);
                            } catch (PersistException pe) {
                                LogFactory.getLog(IndexedCursor.class).error
                                    ("Unable to repair inconsistent index entry " +
                                     indexEntry, pe);
                            } finally {
                                try {
                                    txn.exit();
                                } catch (PersistException pe) {
                                    LogFactory.getLog(IndexedCursor.class).error
                                        ("Unable to repair inconsistent index entry " +
                                         indexEntry, pe);
                                }
                            }
                        }
                    });
                } catch (RepositoryException re) {
                    LogFactory.getLog(getClass()).error
                        ("Unable to inspect inconsistent index entry " +
                         indexEntry, re);
                }

                if (mNext != null) {
                    return true;
                }
            }
        }
        return false;
    }

    public S next() throws FetchException {
        if (hasNext()) {
            S next = mNext;
            mNext = null;
            return next;
        }
        throw new NoSuchElementException();
    }

    public int skipNext(int amount) throws FetchException {
        if (mNext == null) {
            return mCursor.skipNext(amount);
        }

        if (amount <= 0) {
            if (amount < 0) {
                throw new IllegalArgumentException("Cannot skip negative amount: " + amount);
            }
            return 0;
        }

        mNext = null;
        return 1 + mCursor.skipNext(amount - 1);
    }
}
