/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.map;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.AbstractCursor;

import com.amazon.carbonado.txn.TransactionScope;

/**
 * Returns copies of Storables that it iterates over.
 *
 * @author Brian S O'Neill
 */
class MapCursor<S extends Storable> extends AbstractCursor<S> {
    private static final AtomicReferenceFieldUpdater<MapCursor, Iterator> cIteratorRef =
        AtomicReferenceFieldUpdater.newUpdater
        (MapCursor.class, Iterator.class, "mIterator");

    private final MapStorage<S> mStorage;
    private final TransactionScope<MapTransaction> mScope;
    private final MapTransaction mTxn;
    private final boolean mIsForUpdate;

    private volatile Iterator<S> mIterator;

    MapCursor(MapStorage<S> storage,
              TransactionScope<MapTransaction> scope,
              Iterable<S> iterable)
        throws Exception
    {
        MapTransaction txn = scope.getTxn();

        mStorage = storage;
        mScope = scope;
        mTxn = txn;

        if (txn == null) {
            mStorage.mLock.lockForRead(scope);
            mIsForUpdate = false;
        } else {
            // Since lock is so coarse, all reads in transaction scope are
            // upgrade to avoid deadlocks.
            txn.lockForUpgrade(mStorage.mLock, mIsForUpdate = scope.isForUpdate());
        }

        scope.register(storage.getStorableType(), this);
        mIterator = iterable.iterator();
    }

    public void close() {
        Iterator<S> it = mIterator;
        if (it != null) {
            if (cIteratorRef.compareAndSet(this, it, null)) {
                UpgradableLock lock = mStorage.mLock;
                if (mTxn == null) {
                    lock.unlockFromRead(mScope);
                } else {
                    mTxn.unlockFromUpgrade(lock, mIsForUpdate);
                }
                mScope.unregister(mStorage.getStorableType(), this);
            }
        }
    }

    public boolean hasNext() throws FetchException {
        Iterator<S> it = mIterator;
        try {
            if (it != null && it.hasNext()) {
                return true;
            } else {
                close();
            }
            return false;
        } catch (ConcurrentModificationException e) {
            close();
            throw new FetchException(e);
        } catch (Error e) {
            try {
                close();
            } catch (Error e2) {
                // Ignore.
            }
            throw e;
        }
    }

    public S next() throws FetchException {
        Iterator<S> it = mIterator;
        if (it == null) {
            close();
            throw new NoSuchElementException();
        }
        try {
            S next = mStorage.copyAndFireLoadTrigger(it.next());
            if (!hasNext()) {
                close();
            }
            return next;
        } catch (ConcurrentModificationException e) {
            close();
            throw new FetchException(e);
        } catch (Error e) {
            try {
                close();
            } catch (Error e2) {
                // Ignore.
            }
            throw e;
        }
    }

    @Override
    public int skipNext(int amount) throws FetchException {
        if (amount <= 0) {
            if (amount < 0) {
                throw new IllegalArgumentException("Cannot skip negative amount: " + amount);
            }
            return 0;
        }

        // Skip over entries without copying them.

        int count = 0;
        Iterator<S> it = mIterator;

        if (it != null) {
            try {
                while (--amount >= 0 && it.hasNext()) {
                    it.next();
                    count++;
                }
            } catch (ConcurrentModificationException e) {
                close();
                throw new FetchException(e);
            } catch (Error e) {
                try {
                    close();
                } catch (Error e2) {
                    // Ignore.
                }
                throw e;
            }
        }

        return count;
    }
}
