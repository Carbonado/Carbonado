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

import com.amazon.carbonado.capability.Capability;

/**
 * A Repository represents a database for {@link Storable}
 * instances. Some repositories do not have control over the schema (for example, a JDBC
 * Repository depends on the schema defined by the underlying relational database); such
 * repositories are called "dependent".  Conversely, a repository which has complete control
 * over the schema is termed "independent".
 *
 * <P>A dependent repository requires and will verify that Storables
 * have a matching definition in the external storage layer. An independent
 * repository will automatically update type definitions in its database to
 * match changes to Storable definitions.
 *
 * <p>Repository instances should be thread-safe and immutable. Therefore, it
 * is safe for multiple threads to be interacting with a Repository.
 *
 * @author Brian S O'Neill
 * @see RepositoryBuilder
 */
public interface Repository {
    /**
     * Returns the name of this repository.
     */
    String getName();

    /**
     * Returns a Storage instance for the given user defined Storable class or
     * interface.
     *
     * @return specific type of Storage instance
     * @throws IllegalArgumentException if specified type is null
     * @throws MalformedTypeException if specified type is not suitable
     * @throws SupportException if specified type cannot be supported
     * @throws RepositoryException if storage layer throws any other kind of
     * exception
     */
    <S extends Storable> Storage<S> storageFor(Class<S> type)
        throws SupportException, RepositoryException;

    /**
     * Causes the current thread to enter a transaction scope. Call commit
     * inside the transaction in order for any updates to the repository to be
     * applied. Be sure to call exit when leaving the scope.
     * <p>
     * To ensure exit is called, use transactions as follows:
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
     */
    Transaction enterTransaction();

    /**
     * Causes the current thread to enter a transaction scope with an explict
     * isolation level. The actual isolation level may be higher than
     * requested, if the repository does not support the exact level. If the
     * repository does not support a high enough level, it throws an
     * UnsupportedOperationException.
     *
     * @param level minimum desired transaction isolation level -- if null, a
     * suitable default is selected
     * @see #enterTransaction()
     * @throws UnsupportedOperationException if repository does not support
     * isolation as high as the desired level
     */
    Transaction enterTransaction(IsolationLevel level);

    /**
     * Causes the current thread to enter a <i>top-level</i> transaction scope
     * with an explict isolation level. The actual isolation level may be
     * higher than requested, if the repository does not support the exact
     * level. If the repository does not support a high enough level, it throws
     * an UnsupportedOperationException.
     *
     * <p>This method requests a top-level transaction, which means it never
     * has a parent transaction, but it still can be a parent transaction
     * itself. This kind of transaction is useful when a commit must absolutely
     * succeed, even if the current thread is already in a transaction
     * scope. If there was a parent transaction, then a commit might still be
     * rolled back by the parent.
     *
     * <p>Requesting a top-level transaction can be deadlock prone if the
     * current thread is already in a transaction scope. The top-level
     * transaction may not be able to obtain locks held by the parent
     * transaction. An alternative to requesting top-level transactions is to
     * execute transactions in separate threads.
     *
     * @param level minimum desired transaction isolation level -- if null, a
     * suitable default is selected
     * @see #enterTransaction()
     * @throws UnsupportedOperationException if repository does not support
     * isolation as high as the desired level
     */
    Transaction enterTopTransaction(IsolationLevel level);

    /**
     * Returns the isolation level of the current transaction, or null if there
     * is no transaction in the current thread.
     */
    IsolationLevel getTransactionIsolationLevel();

    /**
     * Requests a specific capability of this Repository. This allows
     * repositories to support extended features without having to clutter the
     * main repository interface. The list of supported capabilities is
     * documented with repository implementations.
     *
     * @param capabilityType type of capability requested
     * @return capability instance or null if not supported
     */
    <C extends Capability> C getCapability(Class<C> capabilityType);

    /**
     * Closes this repository reference, aborting any current
     * transactions. Operations on objects returned by this repository will
     * fail when accessing the storage layer.
     *
     * @throws SecurityException if caller does not have permission
     */
    void close();
}
