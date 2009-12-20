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

package com.amazon.carbonado.txn;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Transaction;

/**
 * Generic transaction manager for repositories.
 *
 * @param <Txn> Transaction implementation
 * @author Brian S O'Neill
 */
public abstract class TransactionManager<Txn> {
    private static final int OPEN = 0, CLOSED = 1, SUSPENDED = 2;

    private final ThreadLocal<TransactionScope<Txn>> mLocalScope;
    private final Map<TransactionScope<Txn>, ?> mAllScopes;

    private int mState;

    public TransactionManager() {
        mLocalScope = new ThreadLocal<TransactionScope<Txn>>();
        mAllScopes = new WeakIdentityMap();
    }

    /**
     * Returns the thread-local TransactionScope, creating it if needed.
     */
    public TransactionScope<Txn> localScope() {
        TransactionScope<Txn> scope = mLocalScope.get();
        if (scope == null) {
            int state;
            synchronized (this) {
                state = mState;
                scope = new TransactionScope<Txn>(this, state != OPEN);
                mAllScopes.put(scope, null);
            }
            mLocalScope.set(scope);
            if (state == SUSPENDED) {
                // Immediately suspend new scope.
                scope.getLock().lock();
            }
        }
        return scope;
    }

    /**
     * Detaches the thread-local TransactionScope from the current thread. It
     * can be {@link TransactionScope#attach attached} later, and to any thread
     * which does not currently have a TransactionScope.
     *
     * @return detached thread-local TransactionScope or null if none
     * @since 1.2
     */
    public TransactionScope<Txn> detachLocalScope() {
        TransactionScope<Txn> scope = mLocalScope.get();
        if (scope != null) {
            scope.markDetached();
            mLocalScope.remove();
        }
        return scope;
    }

    // Called by TransactionScope.
    boolean removeLocalScope(TransactionScope<Txn> scope) {
        TransactionScope<Txn> existing = mLocalScope.get();
        if (existing == scope) {
            mLocalScope.remove();
            return true;
        }
        return false;
    }

    // Called by TransactionScope.
    boolean setLocalScope(TransactionScope<Txn> scope, boolean detached) {
        TransactionScope<Txn> existing = mLocalScope.get();
        if (((existing == null || existing.isInactive()) && detached) || existing == scope) {
            mLocalScope.set(scope);
            return true;
        }
        return false;
    }

    /**
     * Closes all transaction scopes. Should be called only when repository is
     * closed.
     *
     * @param suspend when true, indefinitely suspend all threads interacting
     * with transactions
     */
    public synchronized void close(boolean suspend) throws RepositoryException {
        if (mState == SUSPENDED) {
            // If suspended, attempting to close again will likely deadlock.
            return;
        }

        if (suspend) {
            for (TransactionScope<?> scope : mAllScopes.keySet()) {
                // Lock scope but don't release it. This prevents other threads
                // from beginning work during shutdown, which will likely fail
                // along the way.
                scope.getLock().lock();
            }
        }

        mState = suspend ? SUSPENDED : CLOSED;

        for (TransactionScope<?> scope : mAllScopes.keySet()) {
            scope.close();
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
     * Return true if transactions support "for update" mode.
     *
     * @since 1.2
     */
    protected abstract boolean supportsForUpdate();

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
     * Called when a transaction is about to be reused. The default
     * implementation of this method does nothing. Override if any preparation
     * is required to ready a transaction for reuse.
     *
     * @param txn transaction to reuse, never null
     * @since 1.1.3
     */
    protected void reuseTxn(Txn txn) throws Exception {
    }

    /**
     * Called when the Transaction.setForUpdate method is called. The default
     * implementation of this method does nothing. Override if internal
     * transaction needs to switch modes.
     *
     * @since 1.2.1
     */
    protected void setForUpdate(Txn txn, boolean forUpdate) {
    }

    /**
     * Commits and closes the given internal transaction.
     *
     * @return true if transaction object is still valid
     */
    protected abstract boolean commitTxn(Txn txn) throws PersistException;

    /**
     * Aborts and closes the given internal transaction.
     */
    protected abstract void abortTxn(Txn txn) throws PersistException;
}
