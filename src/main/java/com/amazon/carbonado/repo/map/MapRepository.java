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

import org.apache.commons.logging.Log;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;

import com.amazon.carbonado.sequence.SequenceValueGenerator;
import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.qe.RepositoryAccess;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.spi.AbstractRepository;
import com.amazon.carbonado.spi.LobEngine;

import com.amazon.carbonado.txn.TransactionManager;
import com.amazon.carbonado.txn.TransactionScope;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see MapRepositoryBuilder
 */
class MapRepository extends AbstractRepository<MapTransaction>
    implements RepositoryAccess, IndexInfoCapability
{
    private final AtomicReference<Repository> mRootRef;
    private final boolean mIsMaster;
    private final int mLockTimeout;
    private final TimeUnit mLockTimeoutUnit;

    final Iterable<TriggerFactory> mTriggerFactories;
    private final MapTransactionManager mTxnManager;
    private LobEngine mLobEngine;

    MapRepository(AtomicReference<Repository> rootRef, MapRepositoryBuilder builder) {
        super(builder.getName());
        mRootRef = rootRef;
        mIsMaster = builder.isMaster();
        mLockTimeout = builder.getLockTimeout();
        mLockTimeoutUnit = builder.getLockTimeoutUnit();

        mTriggerFactories = builder.getTriggerFactories();
        mTxnManager = new MapTransactionManager(mLockTimeout, mLockTimeoutUnit);
    }

    public Repository getRootRepository() {
        return mRootRef.get();
    }

    public <S extends Storable> StorageAccess<S> storageAccessFor(Class<S> type)
        throws RepositoryException
    {
        return (StorageAccess<S>) storageFor(type);
    }

    public <S extends Storable> IndexInfo[] getIndexInfo(Class<S> storableType)
        throws RepositoryException
    {
        return ((MapStorage) storageFor(storableType)).getIndexInfo();
    }

    @Override
    protected Log getLog() {
        return null;
    }

    @Override
    protected TransactionManager<MapTransaction> transactionManager() {
        return mTxnManager;
    }

    @Override
    protected TransactionScope<MapTransaction> localTransactionScope() {
        return mTxnManager.localScope();
    }

    @Override
    protected <S extends Storable> Storage<S> createStorage(Class<S> type)
        throws RepositoryException
    {
        return new MapStorage<S>(this, type, mLockTimeout, mLockTimeoutUnit);
    }

    @Override
    protected SequenceValueProducer createSequenceValueProducer(String name)
        throws RepositoryException
    {
        return new SequenceValueGenerator(this, name);
    }

    LobEngine getLobEngine() throws RepositoryException {
        if (mLobEngine == null) {
            mLobEngine = new LobEngine(this, getRootRepository());
        }
        return mLobEngine;
    }

    boolean isMaster() {
        return mIsMaster;
    }
}
