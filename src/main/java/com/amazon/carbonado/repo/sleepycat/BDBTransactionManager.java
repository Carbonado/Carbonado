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

package com.amazon.carbonado.repo.sleepycat;

import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.spi.ExceptionTransformer;
import com.amazon.carbonado.spi.TransactionManager;

/**
 * This class is used for tracking transactions and open cursors. Each
 * thread that uses the BDBRepository instance is assigned at most one
 * BDBTransactionManager instance.
 *
 * @author Brian S O'Neill
 */
class BDBTransactionManager<Txn> extends TransactionManager<Txn> {
    // Weakly reference repository because thread locals are not cleaned up
    // very quickly and BDB environments hang on to a ton of memory.
    private final WeakReference<BDBRepository<Txn>> mRepositoryRef;

    BDBTransactionManager(ExceptionTransformer exTransformer, BDBRepository<Txn> repository) {
        super(exTransformer);
        mRepositoryRef = new WeakReference<BDBRepository<Txn>>(repository);
    }

    protected IsolationLevel selectIsolationLevel(Transaction parent, IsolationLevel level) {
        return repository().selectIsolationLevel(parent, level);
    }

    protected Txn createTxn(Txn parent, IsolationLevel level) throws Exception {
        if (level == IsolationLevel.NONE) {
            return null;
        }
        return repository().txn_begin(parent, level);
    }

    @Override
    protected Txn createTxn(Txn parent, IsolationLevel level, int timeout, TimeUnit unit)
        throws Exception
    {
        if (level == IsolationLevel.NONE) {
            return null;
        }
        if (timeout == 0) {
            return repository().txn_begin_nowait(parent, level);
        } else {
            return repository().txn_begin(parent, level);
        }
    }

    protected boolean commitTxn(Txn txn) throws Exception {
        repository().txn_commit(txn);
        return false;
    }

    protected void abortTxn(Txn txn) throws Exception {
        repository().txn_abort(txn);
    }

    private BDBRepository<Txn> repository() {
        BDBRepository<Txn> repo = mRepositoryRef.get();
        if (repo == null) {
            throw new IllegalStateException("Repository closed");
        }
        return repo;
    }
}
