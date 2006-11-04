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

/**
 * Describes a transaction isolation level. Transaction levels, in order from
 * lowest to highest are:
 *
 * <ul>
 * <li>{@link #READ_UNCOMMITTED}
 * <li>{@link #READ_COMMITTED}
 * <li>{@link #REPEATABLE_READ}
 * <li>{@link #SNAPSHOT}
 * <li>{@link #SERIALIZABLE}
 * </ul>
 *
 * A transaction's isolation level is usually {@code READ_COMMITTED} or
 * {@code REPEATABLE_READ} by default. Forcing a lower level, like
 * {@code READ_COMMITTED}, is useful when performing a long cursor
 * iteration. It releases locks during iteration rather than holding on to them
 * until the transaction exits.
 *
 * <p>{@code SNAPSHOT} isolation is special in that it uses multiversion
 * concurrency control (MVCC). A commit may fail with an {@link
 * OptimisticLockException}. Few repositories are expected to support this
 * level, however.
 *
 * @author Brian S O'Neill
 * @see Repository#enterTransaction(IsolationLevel)
 * @see Transaction
 */
public enum IsolationLevel {

    /**
     * Indicates that no actual transaction is in progress. If this level is
     * specified when entering a transaction, it uses auto-commit mode.
     */
    NONE,

    /**
     * Indicates that dirty reads, non-repeatable reads and phantom reads can
     * occur. This level allows modifications by one transaction to be read by
     * another transaction before any changes have been committed (a "dirty
     * read"). If any of the changes are rolled back, the second transaction
     * will have retrieved an invalid modification.
     *
     * <p>This level is also known as degree 1 isolation.
     */
    READ_UNCOMMITTED,

    /**
     * Indicates that dirty reads are prevented. Non-repeatable reads and
     * phantom reads can occur. This level only prohibits a transaction from
     * reading modifications with uncommitted changes in it.
     *
     * <p>This level is also known as degree 2 isolation.
     */
    READ_COMMITTED,

    /**
     * Indicates that dirty reads and non-repeatable reads are prevented.
     * Phantom reads can occur. This level prohibits a transaction from reading
     * uncommitted changes, and it also prohibits the situation where one
     * transaction reads a record, a second transaction alters the record, and
     * the first transaction rereads the record, getting different values the
     * second time (a "non-repeatable read").
     */
    REPEATABLE_READ,

    /**
     * Indicates that dirty reads, non-repeatable reads and phantom reads are
     * prevented. Commits can still fail however, as snapshot isolation avoids
     * using locks.
     */
    SNAPSHOT,

    /**
     * Indicates that dirty reads, non-repeatable reads and phantom reads are
     * prevented. Phantoms are records returned as a result of a search, but
     * which were not seen by the same transaction when the identical search
     * criteria was previously used. For example, another transaction may have
     * inserted records which match the original search.
     *
     * <p>This level is also known as degree 3 isolation.
     */
    SERIALIZABLE;

    /**
     * Returns true if this isolation level is at least as high as the one
     * given.
     */
    public boolean isAtLeast(IsolationLevel level) {
        return ordinal() >= level.ordinal();
    }

    /**
     * Returns true if this isolation level is no higher than the one given.
     */
    public boolean isAtMost(IsolationLevel level) {
        return ordinal() <= level.ordinal();
    }

    /**
     * Returns the lowest common isolation level between this and the one
     * given.
     */
    public IsolationLevel lowestCommon(IsolationLevel level) {
        return (ordinal() >= level.ordinal()) ? level : this;
    }

    /**
     * Returns the highest common isolation level between this and the one
     * given.
     */
    public IsolationLevel highestCommon(IsolationLevel level) {
        return (ordinal() <= level.ordinal()) ? level : this;
    }
}
