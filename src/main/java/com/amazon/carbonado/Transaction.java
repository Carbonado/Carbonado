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

package com.amazon.carbonado;

import java.util.concurrent.TimeUnit;

/**
 * Transactions define atomic operations which can be committed or aborted as a
 * unit. Transactions are entered by calling {@link Repository#enterTransaction()}.
 * Transactions are thread-local, and so no special action needs to be taken to
 * bind operations to them. Cursors which are opened in the scope of a
 * transaction are automatically closed when the transaction is committed or
 * aborted.
 *
 * <p>Transactions do not exit when they are committed. The transaction is
 * still valid after a commit, but new operations are grouped into a separate
 * atomic unit. The exit method <em>must</em> be invoked on every
 * transaction. The following pattern is recommended:
 *
 * <pre>
 * Transaction txn = repository.enterTransaction();
 * try {
 *     // Make updates to storage layer
 *     ...
 *
 *     // Commit the changes up to this point
 *     txn.commit();
 *
 *     // Optionally make more updates
 *     ...
 *
 *     // Commit remaining changes
 *     txn.commit();
 * } finally {
 *     // Ensure transaction exits, aborting uncommitted changes if an exception was thrown
 *     txn.exit();
 * }
 * </pre>
 *
 * <p>Transactions may be nested. Calling commit or abort on an outer
 * transaction will recursively apply the same operation to all inner
 * transactions as well. All Cursors contained within are also closed.
 *
 * <p>Transaction instances are mutable, but they are thread-safe.
 *
 * @author Brian S O'Neill
 */
public interface Transaction {
    /**
     * If currently in a transaction, commits all changes to the storage layer
     * since the last commit within the transaction.
     *
     * @throws PersistException if storage layer throws an exception
     */
    void commit() throws PersistException;

    /**
     * Closes the current transaction, aborting all changes since the last
     * commit.
     *
     * @throws PersistException if storage layer throws an exception
     */
    void exit() throws PersistException;

    /**
     * Set to true to force all read operations within this transaction to
     * acquire upgradable or write locks. This option eliminates deadlocks that
     * may occur when updating records, except it may increase contention.
     */
    void setForUpdate(boolean forUpdate);

    /**
     * Returns true if this transaction is in update mode, which is adjusted by
     * calling {@link #setForUpdate}.
     */
    boolean isForUpdate();

    /**
     * Specify a desired timeout for aquiring locks within this
     * transaction. Calling this method may have have no effect at all, if the
     * repository does not support this feature. In addition, the lock timeout
     * might not be alterable if the transaction contains uncommitted data.
     *
     * <p>Also, the range of lock timeout values supported might be small. For
     * example, only a timeout value of zero might be supported. In that case,
     * the transaction is configured to not wait at all when trying to acquire
     * locks. Expect immediate timeout exceptions when locks cannot be
     * granted.
     *
     * <p>Nested transactions inherit the desired lock timeout of their
     * parent. Top transactions always begin with the default lock timeout.
     *
     * @param timeout Desired lock timeout. If negative, revert lock timeout to
     * default value.
     * @param unit Time unit for timeout. If null, revert lock timeout to
     * default value.
     */
    void setDesiredLockTimeout(int timeout, TimeUnit unit);

    /**
     * Returns the isolation level of this transaction.
     */
    IsolationLevel getIsolationLevel();

    /**
     * Detaches this transaction from the current thread. It can be attached
     * later, and to any thread which currently has no thread-local
     * transaction.
     *
     * <p>Detaching a transaction also detaches any parent and nested child
     * transactions. Attaching any of them achieves the same result as
     * attaching this transaction.
     *
     * @throws IllegalStateException if transaction is attached to a different
     * thread
     * @since 1.2
     */
    void detach();

    /**
     * Attaches this transaction to the current thread, if it has been
     * detached. Attaching a transaction also attaches any parent and nested
     * child transactions.
     *
     * @throws IllegalStateException if current thread has a different
     * transaction already attached
     * @since 1.2
     */
    void attach();
}
