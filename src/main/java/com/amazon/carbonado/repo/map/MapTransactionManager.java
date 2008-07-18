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

import java.util.concurrent.TimeUnit;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.txn.TransactionManager;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class MapTransactionManager extends TransactionManager<MapTransaction> {
    private final int mLockTimeout;
    private final TimeUnit mLockTimeoutUnit;

    MapTransactionManager(int lockTimeout, TimeUnit lockTimeoutUnit) {
        mLockTimeout = lockTimeout;
        mLockTimeoutUnit = lockTimeoutUnit;
    }

    @Override
    protected IsolationLevel selectIsolationLevel(Transaction parent, IsolationLevel level) {
        if (level == null) {
            if (parent == null) {
                return IsolationLevel.READ_COMMITTED;
            }
            return parent.getIsolationLevel();
        }

        switch (level) {
        case NONE:
            return IsolationLevel.NONE;
        case READ_UNCOMMITTED:
        case READ_COMMITTED:
            return IsolationLevel.READ_COMMITTED;
        case REPEATABLE_READ:
        case SERIALIZABLE:
            return IsolationLevel.SERIALIZABLE;
        default:
            // Not supported.
            return null;
        }
    }

    @Override
    protected boolean supportsForUpdate() {
        return true;
    }

    @Override
    protected MapTransaction createTxn(MapTransaction parent, IsolationLevel level)
        throws Exception
    {
        if (level == IsolationLevel.NONE) {
            return null;
        }
        return new MapTransaction(parent, level, mLockTimeout, mLockTimeoutUnit);
    }

    @Override
    protected MapTransaction createTxn(MapTransaction parent, IsolationLevel level,
                                       int timeout, TimeUnit unit)
        throws Exception
    {
        if (level == IsolationLevel.NONE) {
            return null;
        }
        return new MapTransaction(parent, level, timeout, unit);
    }

    @Override
    protected boolean commitTxn(MapTransaction txn) throws PersistException {
        txn.commit();
        return false;
    }

    @Override
    protected void abortTxn(MapTransaction txn) throws PersistException {
        txn.abort();
    }
}
