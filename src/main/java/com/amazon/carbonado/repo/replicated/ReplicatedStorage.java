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
package com.amazon.carbonado.repo.replicated;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UnsupportedTypeException;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.spi.BelatedStorageCreator;

/**
 * ReplicatedStorage
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
class ReplicatedStorage<S extends Storable> implements Storage<S> {
    final Storage<S> mReplicaStorage;
    final ReplicationTrigger<S> mTrigger;

    public ReplicatedStorage(ReplicatedRepository aRepository, Class<S> aType)
        throws SupportException, RepositoryException
    {
        mReplicaStorage = aRepository.getReplicaRepository().storageFor(aType);

        // Create master using BelatedStorageCreator such that we can start up
        // and read from replica even if master is down.

        Log log = LogFactory.getLog(getClass());
        BelatedStorageCreator<S> creator = new BelatedStorageCreator<S>
            (log, aRepository.getMasterRepository(), aType,
             ReplicatedRepositoryBuilder.DEFAULT_RETRY_MILLIS);

        Storage<S> masterStorage;
        try {
            masterStorage = creator.get(ReplicatedRepositoryBuilder.DEFAULT_MASTER_TIMEOUT_MILLIS);
        } catch (UnsupportedTypeException e) {
            // Master doesn't support Storable, but it is marked as Independent.
            masterStorage = null;
        }

        mTrigger = new ReplicationTrigger<S>(aRepository, mReplicaStorage, masterStorage);
        addTrigger(mTrigger);
    }

    /**
     * For testing only.
     */
    ReplicatedStorage(Repository aRepository,
                      Storage<S> replicaStorage,
                      Storage<S> masterStorage)
    {
        mReplicaStorage = replicaStorage;
        mTrigger = new ReplicationTrigger<S>(aRepository, mReplicaStorage, masterStorage);
        addTrigger(mTrigger);
    }

    public Class<S> getStorableType() {
        return mReplicaStorage.getStorableType();
    }

    public S prepare() {
        return mReplicaStorage.prepare();
    }

    public Query<S> query() throws FetchException {
        return mReplicaStorage.query();
    }

    public Query<S> query(String filter) throws FetchException {
        return mReplicaStorage.query(filter);
    }

    public Query<S> query(Filter<S> filter) throws FetchException {
        return mReplicaStorage.query(filter);
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mReplicaStorage.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mReplicaStorage.removeTrigger(trigger);
    }

    ReplicationTrigger<S> getTrigger() {
        return mTrigger;
    }
}
