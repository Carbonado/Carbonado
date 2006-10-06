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

package com.amazon.carbonado.repo.logging;

import java.util.IdentityHashMap;
import java.util.Map;

import java.util.concurrent.atomic.AtomicReference;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.capability.Capability;

/**
 *
 *
 * @author Brian S O'Neill
 */
class LoggingRepository implements Repository, LogAccessCapability {
    private final AtomicReference<Repository> mRootRef;
    private final Repository mRepo;
    private final Log mLog;

    // Map of storages by storable class
    private final Map<Class<?>, LoggingStorage<?>> mStorages;

    LoggingRepository(AtomicReference<Repository> rootRef, Repository actual, Log log) {
        mRootRef = rootRef;
        mRepo = actual;
        mLog = log;

        mStorages = new IdentityHashMap<Class<?>, LoggingStorage<?>>();
    }

    public String getName() {
        return mRepo.getName();
    }

    public <S extends Storable> Storage<S> storageFor(Class<S> type)
        throws SupportException, RepositoryException
    {
        synchronized (mStorages) {
            LoggingStorage storage = mStorages.get(type);
            if (storage == null) {
                storage = new LoggingStorage(this, mRepo.storageFor(type));
                mStorages.put(type, storage);
            }
            return storage;
        }
    }

    public Transaction enterTransaction() {
        mLog.write("Repository.enterTransaction()");
        return new LoggingTransaction(mLog, mRepo.enterTransaction());
    }

    public Transaction enterTransaction(IsolationLevel level) {
        if (mLog.isEnabled()) {
            mLog.write("Repository.enterTransaction(" + level + ')');
        }
        return new LoggingTransaction(mLog, mRepo.enterTransaction(level));
    }

    public Transaction enterTopTransaction(IsolationLevel level) {
        if (mLog.isEnabled()) {
            mLog.write("Repository.enterTopTransaction(" + level + ')');
        }
        return new LoggingTransaction(mLog, mRepo.enterTopTransaction(level));
    }

    public IsolationLevel getTransactionIsolationLevel() {
        return mRepo.getTransactionIsolationLevel();
    }

    public <C extends Capability> C getCapability(Class<C> capabilityType) {
        if (capabilityType.isInstance(this)) {
            return (C) this;
        }
        return mRepo.getCapability(capabilityType);
    }

    public void close() {
        mRepo.close();
    }

    public Log getLog() {
        return mLog;
    }

    Repository getRootRepository() {
        return mRootRef.get();
    }
}
