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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.repo.indexed.IndexedRepositoryBuilder;

import com.amazon.carbonado.spi.AbstractRepositoryBuilder;

/**
 * Volatile repository implementation backed by a concurrent map. Locks used by
 * repository are coarse, much like <i>table locks</i>. Loads and queries
 * acquire read locks, and modifications acquire write locks. Within
 * transactions, loads and queries always acquire upgradable locks, to reduce
 * the likelihood of deadlock.
 *
 * <p>This repository supports transactions, which also may be
 * nested. Supported isolation levels are read committed and serializable. Read
 * uncommitted is promoted to read committed, and repeatable read is promoted
 * to serializable.
 *
 * <p>
 * The following extra capabilities are supported:
 * <ul>
 * <li>{@link com.amazon.carbonado.capability.IndexInfoCapability IndexInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.ShutdownCapability ShutdownCapability}
 * <li>{@link com.amazon.carbonado.sequence.SequenceCapability SequenceCapability}
 * </ul>
 *
 * <p>Note: This repository uses concurrent navigable map classes, which became
 * available in JDK1.6.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public class MapRepositoryBuilder extends AbstractRepositoryBuilder {
    /**
     * Convenience method to build a new MapRepository.
     */
    public static Repository newRepository() {
        try {
            MapRepositoryBuilder builder = new MapRepositoryBuilder();
            return builder.build();
        } catch (RepositoryException e) {
            // Not expected.
            throw new RuntimeException(e);
        }
    }

    private String mName = "";
    private boolean mIsMaster = true;
    private boolean mIndexSupport = true;
    private int mLockTimeout;
    private TimeUnit mLockTimeoutUnit;

    public MapRepositoryBuilder() {
        setLockTimeoutMillis(500);
    }

    public Repository build(AtomicReference<Repository> rootRef) throws RepositoryException {
        if (mIndexSupport) {
            // Temporarily set to false to avoid infinite recursion.
            mIndexSupport = false;
            try {
                IndexedRepositoryBuilder ixBuilder = new IndexedRepositoryBuilder();
                ixBuilder.setWrappedRepository(this);
                ixBuilder.setMaster(isMaster());
                ixBuilder.setAllClustered(true);
                return ixBuilder.build(rootRef);
            } finally {
                mIndexSupport = true;
            }
        }

        assertReady();

        Repository repo = new MapRepository(rootRef, this);

        rootRef.set(repo);
        return repo;
    }

    public String getName() {
        return mName;
    }

    public void setName(String name) {
        mName = name;
    }

    public boolean isMaster() {
        return mIsMaster;
    }

    public void setMaster(boolean b) {
        mIsMaster = b;
    }

    /**
     * Set the lock timeout, in milliseconds. Default value is 500 milliseconds.
     */
    public void setLockTimeoutMillis(int timeout) {
        setLockTimeout(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Set the lock timeout. Default value is 500 milliseconds.
     */
    public void setLockTimeout(int timeout, TimeUnit unit) {
        if (timeout < 0 || unit == null) {
            throw new IllegalArgumentException();
        }
        mLockTimeout = timeout;
        mLockTimeoutUnit = unit;
    }

    /**
     * Returns the lock timeout. Call getLockTimeoutUnit to get the unit.
     */
    public int getLockTimeout() {
        return mLockTimeout;
    }

    /**
     * Returns the lock timeout unit. Call getLockTimeout to get the timeout.
     */
    public TimeUnit getLockTimeoutUnit() {
        return mLockTimeoutUnit;
    }
}
