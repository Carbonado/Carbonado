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

import java.util.concurrent.TimeUnit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistInterruptedException;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.Storable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class MapTransaction {
    private final MapTransaction mParent;
    private final IsolationLevel mLevel;
    private final Object mLocker;
    private final int mLockTimeout;
    private final TimeUnit mLockTimeoutUnit;

    private Set<UpgradableLock> mUpgradeLocks;
    private Set<UpgradableLock> mWriteLocks;

    private List<Undoable> mUndoLog;

    MapTransaction(MapTransaction parent, IsolationLevel level,
                   int lockTimeout, TimeUnit lockTimeoutUnit)
    {
        mParent = parent;
        mLevel = level;
        mLocker = parent == null ? this : parent.mLocker;
        mLockTimeout = lockTimeout;
        mLockTimeoutUnit = lockTimeoutUnit;
    }

    void lockForUpgrade(UpgradableLock lock, boolean isForUpdate) throws FetchException {
        if (!isForUpdate && mLevel.isAtMost(IsolationLevel.READ_COMMITTED)) {
            doLockForUpgrade(lock);
        } else {
            Set<UpgradableLock> locks = mUpgradeLocks;
            if (locks == null) {
                mUpgradeLocks = locks = new HashSet<UpgradableLock>();
                doLockForUpgrade(lock);
                locks.add(lock);
            } else if (!locks.contains(lock)) {
                doLockForUpgrade(lock);
                locks.add(lock);
            }
        }
    }

    private void doLockForUpgrade(UpgradableLock lock) throws FetchException {
        try {
            if (!lock.tryLockForUpgrade(mLocker, mLockTimeout, mLockTimeoutUnit)) {
                throw new FetchTimeoutException("" + mLockTimeout + ' ' +
                                                mLockTimeoutUnit.toString().toLowerCase());
            }
        } catch (InterruptedException e) {
            throw new FetchInterruptedException(e);
        }
    }

    void unlockFromUpgrade(UpgradableLock lock, boolean isForUpdate) {
        if (!isForUpdate && mLevel.isAtMost(IsolationLevel.READ_COMMITTED)) {
            lock.unlockFromUpgrade(mLocker);
        }
    }

    void lockForWrite(UpgradableLock lock) throws PersistException {
        Set<UpgradableLock> locks = mWriteLocks;
        if (locks == null) {
            mWriteLocks = locks = new HashSet<UpgradableLock>();
            doLockForWrite(lock);
            locks.add(lock);
        } else if (!locks.contains(lock)) {
            doLockForWrite(lock);
            locks.add(lock);
        }
    }

    private void doLockForWrite(UpgradableLock lock) throws PersistException {
        try {
            if (!lock.tryLockForWrite(mLocker, mLockTimeout, mLockTimeoutUnit)) {
                throw new PersistTimeoutException("" + mLockTimeout + ' ' +
                                                  mLockTimeoutUnit.toString().toLowerCase());
            }
        } catch (InterruptedException e) {
            throw new PersistInterruptedException(e);
        }
    }

    /**
     * Add to undo log.
     */
    <S extends Storable> void inserted(final MapStorage<S> storage, final S key) {
        addToUndoLog(new Undoable() {
            public void undo() {
                storage.mapRemove(key);
            }

            @Override
            public String toString() {
                return "undo insert by remove: " + key;
            }
        });
    }

    /**
     * Add to undo log.
     */
    <S extends Storable> void updated(final MapStorage<S> storage, final S old) {
        addToUndoLog(new Undoable() {
            public void undo() {
                storage.mapPut(old);
            }

            @Override
            public String toString() {
                return "undo update by put: " + old;
            }
        });
    }

    /**
     * Add to undo log.
     */
    <S extends Storable> void deleted(final MapStorage<S> storage, final S old) {
        addToUndoLog(new Undoable() {
            public void undo() {
                storage.mapPut(old);
            }

            @Override
            public String toString() {
                return "undo delete by put: " + old;
            }
        });
    }

    void commit() {
        MapTransaction parent = mParent;

        if (parent == null) {
            releaseLocks();
            return;
        }

        // Pass undo log to parent.
        if (parent.mUndoLog == null) {
            parent.mUndoLog = mUndoLog;
        } else if (mUndoLog != null) {
            parent.mUndoLog.addAll(mUndoLog);
        }
        mUndoLog = null;

        // Pass write locks to parent or release if parent already has the lock.
        {
            Set<UpgradableLock> locks = mWriteLocks;
            if (locks != null) {
                Set<UpgradableLock> parentLocks = parent.mWriteLocks;
                if (parentLocks == null) {
                    parent.mWriteLocks = locks;
                } else {
                    for (UpgradableLock lock : locks) {
                        if (!parentLocks.add(lock)) {
                            lock.unlockFromWrite(mLocker);
                        }
                    }
                }
                mWriteLocks = null;
            }
        }

        // Upgrade locks can simply be released.
        releaseUpgradeLocks();
    }

    void abort() {
        List<Undoable> log = mUndoLog;
        if (log != null) {
            for (int i=log.size(); --i>=0; ) {
                log.get(i).undo();
            }
        }
        mUndoLog = null;

        releaseLocks();
    }

    private void addToUndoLog(Undoable entry) {
        List<Undoable> log = mUndoLog;
        if (log == null) {
            mUndoLog = log = new ArrayList<Undoable>();
        }
        log.add(entry);
    }

    private void releaseLocks() {
        releaseWriteLocks();
        releaseUpgradeLocks();
    }

    private void releaseWriteLocks() {
        Set<UpgradableLock> locks = mWriteLocks;
        if (locks != null) {
            for (UpgradableLock lock : locks) {
                lock.unlockFromWrite(mLocker);
            }
            mWriteLocks = null;
        }
    }

    private void releaseUpgradeLocks() {
        Set<UpgradableLock> locks = mUpgradeLocks;
        if (locks != null) {
            for (UpgradableLock lock : locks) {
                lock.unlockFromUpgrade(mLocker);
            }
            mUpgradeLocks = null;
        }
    }

    private static interface Undoable {
        void undo();
    }
}
