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

package com.amazon.carbonado.spi;

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

/**
 * Generic transaction manager for repositories. Repositories should only have
 * thread local instances.
 *
 * @author Brian S O'Neill
 */
public abstract class TransactionManager<Txn> {

    final Lock mLock;
    final ExceptionTransformer mExTransformer;

    TransactionImpl<Txn> mCurrent;

    // Tracks all registered cursors by storage type.
    private Map<Class<?>, CursorList<TransactionImpl<Txn>>> mCursors;

    private boolean mClosed;

    public TransactionManager(ExceptionTransformer exTransformer) {
        // The use of a fair lock is essential for shutdown hooks that attempt
        // to acquire the locks of all TransactionManagers. Otherwise, the
        // shutdown can take a long time.
        mLock = new ReentrantLock(true);
        mExTransformer = exTransformer;
    }

    /**
     * Returns the exception transformer in use.
     */
    public ExceptionTransformer getExceptionTransformer() {
        return mExTransformer;
    }

    /**
     * Enters a new transaction scope.
     *
     * @param level desired isolation level (may be null)
     * @throws UnsupportedOperationException if isolation level higher than
     * supported by repository
     */
    public Transaction enter(IsolationLevel level) {
        mLock.lock();
        try {
            TransactionImpl<Txn> parent = mCurrent;
            IsolationLevel actualLevel = selectIsolationLevel(parent, level);
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

            return mCurrent = new TransactionImpl<Txn>(this, parent, false, actualLevel);
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Enters a new top-level transaction scope.
     *
     * @param level desired isolation level (may be null)
     * @throws UnsupportedOperationException if isolation level higher than
     * supported by repository
     */
    public Transaction enterTop(IsolationLevel level) {
        mLock.lock();
        try {
            IsolationLevel actualLevel = selectIsolationLevel(null, level);
            if (actualLevel == null) {
                throw new UnsupportedOperationException
                    ("Desired isolation level not supported: " + level);
            }

            return mCurrent = new TransactionImpl<Txn>(this, mCurrent, true, actualLevel);
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Registers the given cursor against the current transaction, allowing
     * it to be closed on transaction exit or transaction manager close. If
     * there is no current transaction scope, the cursor is registered as not
     * part of a transaction. Cursors should register when created.
     */
    public <S extends Storable> void register(Class<S> type, Cursor<S> cursor) {
        mLock.lock();
        try {
            checkState();
            if (mCursors == null) {
                mCursors = new IdentityHashMap<Class<?>, CursorList<TransactionImpl<Txn>>>();
            }

            CursorList<TransactionImpl<Txn>> cursorList = mCursors.get(type);
            if (cursorList == null) {
                cursorList = new CursorList<TransactionImpl<Txn>>();
                mCursors.put(type, cursorList);
            }

            cursorList.register(cursor, mCurrent);

            if (mCurrent != null) {
                mCurrent.register(cursor);
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
     * Returns the count of registered cursors of a specific type.
     */
    public <S extends Storable> int getRegisteredCount(Class<S> type) {
        mLock.lock();
        try {
            if (mCursors != null) {
                CursorList<TransactionImpl<Txn>> cursorList = mCursors.get(type);
                if (cursorList != null) {
                    return cursorList.size();
                }
            }
        } finally {
            mLock.unlock();
        }
        return 0;
    }

    /**
     * Returns a registered cursor of the given type, or null if none at given index.
     */
    @SuppressWarnings("unchecked")
    public <S extends Storable> Cursor<S> getRegisteredCursor(Class<S> type, int index) {
        mLock.lock();
        try {
            if (mCursors != null) {
                CursorList<TransactionImpl<Txn>> cursorList = mCursors.get(type);
                if (cursorList != null) {
                    if (index < cursorList.size()) {
                        return (Cursor<S>) cursorList.getCursor(index);
                    }
                }
            }
        } finally {
            mLock.unlock();
        }
        return null;
    }

    /**
     * Returns lock used by TransactionManager. While holding lock, operations
     * are suspended.
     */
    public Lock getLock() {
        return mLock;
    }

    /**
     * Exits all transactions and closes all cursors. Should be called only
     * when repository is closed.
     */
    public void close() throws RepositoryException {
        mLock.lock();
        try {
            if (!mClosed) {
                while (mCurrent != null) {
                    mCurrent.exit();
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
     * Returns null if no transaction is in progress.
     *
     * @throws Exception thrown by createTxn
     */
    public Txn getTxn() throws Exception {
        mLock.lock();
        try {
            checkState();
            return mCurrent == null ? null : mCurrent.getTxn();
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Returns true if a transaction is in progress and it is for update.
     */
    public boolean isForUpdate() {
        mLock.lock();
        try {
            return (mClosed || mCurrent == null) ? false : mCurrent.isForUpdate();
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Returns the isolation level of the current transaction, or null if there
     * is no transaction in the current thread.
     */
    public IsolationLevel getIsolationLevel() {
        mLock.lock();
        try {
            return (mClosed || mCurrent == null) ? null : mCurrent.getIsolationLevel();
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Caller must hold mLock.
     */
    private void checkState() {
        if (mClosed) {
            throw new IllegalStateException("Repository is closed");
        }
    }

    /**
     * Returns supported isolation level, which may be higher. If isolation
     * level cannot go higher (or lower than parent) then return null.
     *
     * @param parent optional parent transaction
     * @param level desired isolation level (may be null)
     */
    protected abstract IsolationLevel selectIsolationLevel(Transaction parent,
                                                           IsolationLevel level);

    /**
     * Creates an internal transaction representation, with the optional parent
     * transaction. If parent is not null and real nested transactions are not
     * supported, simply return parent transaction for supporting fake nested
     * transactions.
     *
     * @param parent optional parent transaction
     * @param level required isolation level
     * @return new transaction, parent transaction, or possibly null if required
     * isolation level is none
     */
    protected abstract Txn createTxn(Txn parent, IsolationLevel level) throws Exception;

    /**
     * Creates an internal transaction representation, with the optional parent
     * transaction. If parent is not null and real nested transactions are not
     * supported, simply return parent transaction for supporting fake nested
     * transactions.
     *
     * <p>The default implementation of this method just calls the regular
     * createTxn method, ignoring the timeout parameter.
     *
     * @param parent optional parent transaction
     * @param level required isolation level
     * @param timeout desired timeout for lock acquisition, never negative
     * @param unit timeout unit, never null
     * @return new transaction, parent transaction, or possibly null if required
     * isolation level is none
     */
    protected Txn createTxn(Txn parent, IsolationLevel level,
                            int timeout, TimeUnit unit)
        throws Exception
    {
        return createTxn(parent, level);
    }

    /**
     * Commits and closes the given internal transaction.
     *
     * @return true if transaction object is still valid
     */
    protected abstract boolean commitTxn(Txn txn) throws Exception;

    /**
     * Aborts and closes the given internal transaction.
     */
    protected abstract void abortTxn(Txn txn) throws Exception;

    private static class TransactionImpl<Txn> implements Transaction {
        private final TransactionManager<Txn> mTxnMgr;
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

        TransactionImpl(TransactionManager<Txn> txnMgr,
                        TransactionImpl<Txn> parent,
                        boolean top,
                        IsolationLevel level) {
            mTxnMgr = txnMgr;
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
            TransactionManager<Txn> txnMgr = mTxnMgr;
            txnMgr.mLock.lock();
            try {
                if (!mExited) {
                    if (mChild != null) {
                        mChild.commit();
                    }

                    closeCursors();

                    if (mTxn != null) {
                        if (mParent == null || mParent.mTxn != mTxn) {
                            try {
                                if (!txnMgr.commitTxn(mTxn)) {
                                    mTxn = null;
                                }
                            } catch (Throwable e) {
                                mTxn = null;
                                throw txnMgr.mExTransformer.toPersistException(e);
                            }
                        } else {
                            // Indicate fake nested transaction committed.
                            mTxn = null;
                        }
                    }
                }
            } finally {
                txnMgr.mLock.unlock();
            }
        }

        public void exit() throws PersistException {
            TransactionManager<Txn> txnMgr = mTxnMgr;
            txnMgr.mLock.lock();
            try {
                if (!mExited) {
                    if (mChild != null) {
                        mChild.exit();
                    }

                    closeCursors();

                    if (mTxn != null) {
                        try {
                            if (mParent == null || mParent.mTxn != mTxn) {
                                try {
                                    txnMgr.abortTxn(mTxn);
                                } catch (Throwable e) {
                                    throw txnMgr.mExTransformer.toPersistException(e);
                                }
                            }
                        } finally {
                            mTxn = null;
                        }
                    }

                    txnMgr.mCurrent = mParent;

                    mExited = true;
                }
            } finally {
                txnMgr.mLock.unlock();
            }
        }

        public void setForUpdate(boolean forUpdate) {
            mForUpdate = forUpdate;
        }

        public boolean isForUpdate() {
            return mForUpdate;
        }

        public void setDesiredLockTimeout(int timeout, TimeUnit unit) {
            if (timeout < 0) {
                mDesiredLockTimeout = 0;
                mTimeoutUnit = null;
            } else {
                mDesiredLockTimeout = timeout;
                mTimeoutUnit = unit;
            }
        }

        public IsolationLevel getIsolationLevel() {
            return mLevel;
        }

        <S extends Storable> void register(Cursor<S> cursor) {
            if (mCursorList == null) {
                mCursorList = new CursorList<Object>();
            }
            mCursorList.register(cursor, null);
        }

        <S extends Storable> void unregister(Cursor<S> cursor) {
            if (mCursorList != null) {
                mCursorList.unregister(cursor);
            }
        }

        Txn getTxn() throws Exception {
            TransactionManager<Txn> txnMgr = mTxnMgr;
            txnMgr.mLock.lock();
            try {
                if (mTxn == null) {
                    Txn parent = (mParent == null || mTop) ? null : mParent.getTxn();
                    if (mTimeoutUnit == null) {
                        mTxn = txnMgr.createTxn(parent, mLevel);
                    } else {
                        mTxn = txnMgr.createTxn(parent, mLevel, mDesiredLockTimeout, mTimeoutUnit);
                    }
                }
                return mTxn;
            } finally {
                txnMgr.mLock.unlock();
            }
        }

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
