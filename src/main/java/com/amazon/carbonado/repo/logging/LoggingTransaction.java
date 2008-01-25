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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;

/**
 *
 *
 * @author Brian S O'Neill
 */
class LoggingTransaction implements Transaction {
    private static final AtomicLong mNextID = new AtomicLong();

    private final ThreadLocal<LoggingTransaction> mActiveTxn;
    private final LoggingTransaction mParent;
    private final Log mLog;
    private final Transaction mTxn;
    private final long mID;
    private final boolean mTop;

    LoggingTransaction(ThreadLocal<LoggingTransaction> activeTxn,
                       Log log, Transaction txn, boolean top)
    {
        mActiveTxn = activeTxn;
        mParent = activeTxn.get();
        mLog = log;
        mTxn = txn;
        mID = mNextID.addAndGet(1);
        mTop = top;
        activeTxn.set(this);
        mLog.write("Entered transaction: " + idChain());
    }

    public void commit() throws PersistException {
        mLog.write("Transaction.commit() on " + idChain());
        mTxn.commit();
    }

    public void exit() throws PersistException {
        mLog.write("Transaction.exit() on " + idChain());
        mTxn.exit();
        mActiveTxn.set(mParent);
    }

    public void setForUpdate(boolean forUpdate) {
        if (mLog.isEnabled()) {
            mLog.write("Transaction.setForUpdate(" + forUpdate + ") on " + idChain());
        }
        mTxn.setForUpdate(forUpdate);
    }

    public boolean isForUpdate() {
        return mTxn.isForUpdate();
    }

    public void setDesiredLockTimeout(int timeout, TimeUnit unit) {
        mTxn.setDesiredLockTimeout(timeout, unit);
    }

    public IsolationLevel getIsolationLevel() {
        return mTxn.getIsolationLevel();
    }

    public void detach() {
        if (mLog.isEnabled()) {
            mLog.write("Transaction.detach() on " + idChain());
        }
        mTxn.detach();
    }

    public void attach() {
        if (mLog.isEnabled()) {
            mLog.write("Transaction.attach() on " + idChain());
        }
        mTxn.attach();
    }

    private String idChain() {
        if (mParent == null) {
            return String.valueOf(mID);
        }
        if (mTop) {
            return mParent.idChain() + " | " + mID;
        } else {
            return mParent.idChain() + " > " + mID;
        }
    }
}
