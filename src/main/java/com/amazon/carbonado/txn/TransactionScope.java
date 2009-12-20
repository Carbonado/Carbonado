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

package com.amazon.carbonado.txn;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.spi.ExceptionTransformer;

/**
 * Container of thread local, scoped transactions.
 *
 * @param <Txn> Transaction implementation
 * @author Brian S O'Neill
 * @since 1.2
 * @see TransactionManager
 */
public class TransactionScope<Txn> {
    final TransactionManager<Txn> mTxnMgr;
    final Lock mLock;

    TransactionImpl<Txn> mActive;

    // Tracks all registered cursors by storage type.
    private Map<Class<?>, CursorList<TransactionImpl<Txn>>> mCursors;

    private boolean mClosed;
    private boolean mDetached;

    TransactionScope(TransactionManager<Txn> txnMgr, boolean closed) {
        mTxnMgr = txnMgr;
        mLock = new ReentrantLock(true);
    }

    /**
     * Enters a new transaction scope which becomes the active transaction.
     *
     * @param level desired isolation level (may be null)
     * @throws UnsupportedOperationException if isolation level higher than
     * supported by repository
     */
    public Transaction enter(IsolationLevel level) {
        mLock.lock();
        try {
            TransactionImpl<Txn> parent = mActive;
            IsolationLevel actualLevel = mTxnMgr.selectIsolationLevel(parent, level);
            if (actualLevel == null) {
                if (parent == null) {
                    throw new UnsupportedOperationException
                        ("Desired isolation level not supported: " + level);
                } else {
                    throw new UnsupportedOperationException
                        ("Desired isolation level not supported: " + level
                         + "; parent isolation level: " + parent.getIsolationLevel());
                }
            }

            return mActive = new TransactionImpl<Txn>(this, parent, false, actualLevel);
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Enters a new top-level transaction scope which becomes the active
     * transaction.
     *
     * @param level desired isolation level (may be null)
     * @throws UnsupportedOperationException if isolation level higher than
     * supported by repository
     */
    public Transaction enterTop(IsolationLevel level) {
        mLock.lock();
        try {
            IsolationLevel actualLevel = mTxnMgr.selectIsolationLevel(null, level);
            if (actualLevel == null) {
                throw new UnsupportedOperationException
                    ("Desired isolation level not supported: " + level);
            }

            return mActive = new TransactionImpl<Txn>(this, mActive, true, actualLevel);
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Registers the given cursor against the active transaction, allowing it
     * to be closed on transaction exit or transaction manager close. If there
     * is no active transaction in scope, the cursor is registered as not part
     * of a transaction. Cursors should register when created.
     */
    public <S extends Storable> void register(Class<S> type, Cursor<S> cursor) {
        mLock.lock();
        try {
            checkClosed();
            if (mCursors == null) {
                mCursors = new IdentityHashMap<Class<?>, CursorList<TransactionImpl<Txn>>>();
            }

            CursorList<TransactionImpl<Txn>> cursorList = mCursors.get(type);
            if (cursorList == null) {
                cursorList = new CursorList<TransactionImpl<Txn>>();
                mCursors.put(type, cursorList);
            }

            cursorList.register(cursor, mActive);

            if (mActive != null) {
                mActive.register(cursor);
            }
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Unregisters a previously registered cursor. Cursors should unregister
     * when closed.
     */
    public <S extends Storable> void unregister(Class<S> type, Cursor<S> cursor) {
        mLock.lock();
        try {
            if (mCursors != null) {
                CursorList<TransactionImpl<Txn>> cursorList = mCursors.get(type);
                if (cursorList != null) {
                    TransactionImpl<Txn> txnImpl = cursorList.unregister(cursor);
                    if (txnImpl != null) {
                        txnImpl.unregister(cursor);
                    }
                }
            }
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Returns lock used by TransactionScope. While holding lock, operations
     * are suspended.
     */
    public Lock getLock() {
        return mLock;
    }

    /**
     * Returns the implementation for the active transaction, or null if there
     * is no active transaction.
     *
     * @throws Exception thrown by createTxn or reuseTxn
     */
    public Txn getTxn() throws Exception {
        mLock.lock();
        try {
            checkClosed();
            return mActive == null ? null : mActive.getTxn();
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Returns true if an active transaction exists and it is for update.
     */
    public boolean isForUpdate() {
        mLock.lock();
        try {
            return (mClosed || mActive == null) ? false : mActive.isForUpdate();
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Returns the isolation level of the active transaction, or null if there
     * is no active transaction.
     */
    public IsolationLevel getIsolationLevel() {
        mLock.lock();
        try {
            return (mClosed || mActive == null) ? null : mActive.getIsolationLevel();
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Attach this scope to the current thread, if it has been {@link
     * TransactionManager#detachLocalScope detached}.
     *
     * @throws IllegalStateException if current thread has a different
     * transaction already attached
     */
    public void attach() {
        mLock.lock();
        try {
            if (mTxnMgr.setLocalScope(this, mDetached)) {
                mDetached = false;
            } else if (!mDetached) {
                throw new IllegalStateException("Transaction scope is not detached");
            } else {
                throw new IllegalStateException
                    ("Current thread has a different transaction already attached");
            }
        } finally {
            mLock.unlock();
        }
    }

    // Called by TransactionImpl.
    void detach() {
        mLock.lock();
        try {
            if (mDetached || mTxnMgr.removeLocalScope(this)) {
                mDetached = true;
            } else {
                throw new IllegalStateException("Transaction is attached to a different thread");
            }
        } finally {
            mLock.unlock();
        }
    }

    // Called by TransactionManager.
    void markDetached() {
        mLock.lock();
        try {
            mDetached = true;
        } finally {
            mLock.unlock();
        }
    }

    // Called by TransactionManager.
    boolean isInactive() {
        mLock.lock();
        try {
            return mActive == null;
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Exits all transactions and closes all cursors. Should be called only
     * when repository is closed.
     */
    void close() throws RepositoryException {
        mLock.lock();
        try {
            if (!mClosed) {
                while (mActive != null) {
                    mActive.exit();
                }
                if (mCursors != null) {
                    for (CursorList<TransactionImpl<Txn>> cursorList : mCursors.values()) {
                        cursorList.closeCursors();
                    }
                }
            }
        } finally {
            mClosed = true;
            mLock.unlock();
        }
    }

    /**
     * Caller must hold mLock.
     */
    private void checkClosed() {
        if (mClosed) {
            throw new IllegalStateException("Repository is closed");
        }
    }

    private static class TransactionImpl<Txn> implements Transaction {
        private final TransactionScope<Txn> mScope;
        private final TransactionImpl<Txn> mParent;
        private final boolean mTop;
        private final IsolationLevel mLevel;

        private boolean mForUpdate;
        private int mDesiredLockTimeout;
        private TimeUnit mTimeoutUnit;

        private TransactionImpl<Txn> mChild;
        private boolean mExited;
        private Txn mTxn;

        // Tracks all registered cursors.
        private CursorList<?> mCursorList;

        TransactionImpl(TransactionScope<Txn> scope,
                        TransactionImpl<Txn> parent,
                        boolean top,
                        IsolationLevel level) {
            mScope = scope;
            mParent = parent;
            mTop = top;
            mLevel = level;
            if (!top && parent != null) {
                parent.mChild = this;
                mDesiredLockTimeout = parent.mDesiredLockTimeout;
                mTimeoutUnit = parent.mTimeoutUnit;
            }
        }

        public void commit() throws PersistException {
            TransactionScope<Txn> scope = mScope;
            scope.mLock.lock();
            try {
                if (!mExited) {
                    if (mChild != null) {
                        mChild.commit();
                    }

                    closeCursors();

                    if (mTxn != null) {
                        if (mParent == null || mParent.mTxn != mTxn) {
                            try {
                                if (!scope.mTxnMgr.commitTxn(mTxn)) {
                                    mTxn = null;
                                }
                            } catch (Throwable e) {
                                mTxn = null;
                                throw ExceptionTransformer.getInstance().toPersistException(e);
                            }
                        } else {
                            // Indicate fake nested transaction committed.
                            mTxn = null;
                        }
                    }
                }
            } finally {
                scope.mLock.unlock();
            }
        }

        public void exit() throws PersistException {
            TransactionScope<Txn> scope = mScope;
            scope.mLock.lock();
            try {
                if (!mExited) {
                    Exception exception = null;
                    try {
                        if (mChild != null) {
                            try {
                                mChild.exit();
                            } catch (Exception e) {
                                if (exception == null) {
                                    exception = e;
                                }
                            }
                        }

                        try {
                            closeCursors();
                        } catch (Exception e) {
                            if (exception == null) {
                                exception = e;
                            }
                        }

                        if (mTxn != null) {
                            try {
                                if (mParent == null || mParent.mTxn != mTxn) {
                                    try {
                                        scope.mTxnMgr.abortTxn(mTxn);
                                    } catch (Exception e) {
                                        if (exception == null) {
                                            exception = e;
                                        }
                                    }
                                }
                            } finally {
                                mTxn = null;
                            }
                        }
                    } finally {
                        scope.mActive = mParent;
                        mExited = true;
                        if (exception != null) {
                            throw ExceptionTransformer.getInstance().toPersistException(exception);
                        }
                    }
                }
            } finally {
                scope.mLock.unlock();
            }
        }

        public void setForUpdate(boolean forUpdate) {
            mForUpdate = (forUpdate &= mScope.mTxnMgr.supportsForUpdate());
            Txn txn = mTxn;
            if (txn != null) {
                mScope.mTxnMgr.setForUpdate(txn, forUpdate);
            }
        }

        public boolean isForUpdate() {
            return mForUpdate;
        }

        public void setDesiredLockTimeout(int timeout, TimeUnit unit) {
            TransactionScope<Txn> scope = mScope;
            scope.mLock.lock();
            try {
                if (timeout < 0) {
                    mDesiredLockTimeout = 0;
                    mTimeoutUnit = null;
                } else {
                    mDesiredLockTimeout = timeout;
                    mTimeoutUnit = unit;
                }
            } finally {
                scope.mLock.unlock();
            }
        }

        public IsolationLevel getIsolationLevel() {
            return mLevel;
        }

        public void detach() {
            mScope.detach();
        }

        public void attach() {
            mScope.attach();
        }

        // Caller must hold mLock.
        <S extends Storable> void register(Cursor<S> cursor) {
            if (mCursorList == null) {
                mCursorList = new CursorList<Object>();
            }
            mCursorList.register(cursor, null);
        }

        // Caller must hold mLock.
        <S extends Storable> void unregister(Cursor<S> cursor) {
            if (mCursorList != null) {
                mCursorList.unregister(cursor);
            }
        }

        // Caller must hold mLock.
        Txn getTxn() throws Exception {
            TransactionScope<Txn> scope = mScope;
            if (mTxn != null) {
                scope.mTxnMgr.reuseTxn(mTxn);
            } else {
                Txn parentTxn = (mParent == null || mTop) ? null : mParent.getTxn();
                if (mTimeoutUnit == null) {
                    mTxn = scope.mTxnMgr.createTxn(parentTxn, mLevel);
                } else {
                    mTxn = scope.mTxnMgr.createTxn(parentTxn, mLevel,
                                                   mDesiredLockTimeout, mTimeoutUnit);
                }
            }
            if (mForUpdate) {
                scope.mTxnMgr.setForUpdate(mTxn, true);
            }
            return mTxn;
        }

        // Caller must hold mLock.
        private void closeCursors() throws PersistException {
            if (mCursorList != null) {
                mCursorList.closeCursors();
            }
        }
    }

    /**
     * Simple fast list/map for holding a small amount of cursors.
     */
    static class CursorList<V> {
        private int mSize;
        private Cursor<?>[] mCursors;
        private V[] mValues;

        CursorList() {
            mCursors = new Cursor[8];
        }

        /**
         * @param value optional value to associate
         */
        @SuppressWarnings("unchecked")
        void register(Cursor<?> cursor, V value) {
            int size = mSize;
            Cursor<?>[] cursors = mCursors;

            if (size == cursors.length) {
                int newLength = size << 1;

                Cursor<?>[] newCursors = new Cursor[newLength];
                System.arraycopy(cursors, 0, newCursors, 0, size);
                mCursors = cursors = newCursors;

                if (mValues != null) {
                    V[] newValues = (V[]) new Object[newLength];
                    System.arraycopy(mValues, 0, newValues, 0, size);
                    mValues = newValues;
                }
            }

            cursors[size] = cursor;

            if (value != null) {
                V[] values = mValues;
                if (values == null) {
                    mValues = values = (V[]) new Object[cursors.length];
                }
                values[size] = value;
            }

            mSize = size + 1;
        }

        V unregister(Cursor<?> cursor) {
            // Assuming that cursors are opened and closed in LIFO order
            // (stack order), search backwards to optimize.
            Cursor<?>[] cursors = mCursors;
            int size = mSize;
            int i = size;
            search: {
                while (--i >= 0) {
                    if (cursors[i] == cursor) {
                        break search;
                    }
                }
                // Not found.
                return null;
            }

            V[] values = mValues;
            V value;

            if (values == null) {
                value = null;
                if (i == size - 1) {
                    // Clear reference so that it can be garbage collected.
                    cursors[i] = null;
                } else {
                    // Shift array elements down.
                    System.arraycopy(cursors, i + 1, cursors, i, size - i - 1);
                }
            } else {
                value = values[i];
                if (i == size - 1) {
                    // Clear references so that they can be garbage collected.
                    cursors[i] = null;
                    values[i] = null;
                } else {
                    // Shift array elements down.
                    System.arraycopy(cursors, i + 1, cursors, i, size - i - 1);
                    System.arraycopy(values, i + 1, values, i, size - i - 1);
                }
            }

            mSize = size - 1;
            return value;
        }

        int size() {
            return mSize;
        }

        Cursor<?> getCursor(int index) {
            return mCursors[index];
        }

        V getValue(int index) {
            V[] values = mValues;
            return values == null ? null : values[index];
        }

        /**
         * Closes all cursors and resets the size of this list to 0.
         */
        void closeCursors() throws PersistException {
            // Note: Iteration must be in reverse order. Calling close on the
            // cursor should cause it to unregister from this list. This will
            // cause only a modification to the end of the list, which is no
            // longer needed by this method.
            try {
                Cursor<?>[] cursors = mCursors;
                V[] values = mValues;
                int i = mSize;
                if (values == null) {
                    while (--i >= 0) {
                        Cursor<?> cursor = cursors[i];
                        if (cursor != null) {
                            cursor.close();
                            cursors[i] = null;
                        }
                    }
                } else {
                    while (--i >= 0) {
                        Cursor<?> cursor = cursors[i];
                        if (cursor != null) {
                            cursor.close();
                            cursors[i] = null;
                            values[i] = null;
                        }
                    }
                }
            } catch (FetchException e) {
                throw e.toPersistException();
            }
            mSize = 0;
        }
    }
}
