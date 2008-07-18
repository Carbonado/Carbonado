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

package com.amazon.carbonado.txn;

import java.util.concurrent.TimeUnit;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;

/**
 * Pairs two transaction together into one. The transaction cannot be atomic,
 * however. Inconsistencies can result if the primary transaction succeeds in
 * committing, but the secondary fails. Therefore, the designated primary
 * transaction should be the one that is more likely to fail. For example, the
 * primary transaction might rely on the network, but the secondary operates
 * locally.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public class TransactionPair implements Transaction {
    private final Transaction mPrimaryTransaction;
    private final Transaction mSecondaryTransaction;

    /**
     * @param primaryTransaction is committed first, exited last
     * @param secondaryTransaction is exited first, commited last
     */
    public TransactionPair(Transaction primaryTransaction, Transaction secondaryTransaction) {
        mPrimaryTransaction = primaryTransaction;
        mSecondaryTransaction = secondaryTransaction;
    }

    public void commit() throws PersistException {
        mPrimaryTransaction.commit();
        try {
            mSecondaryTransaction.commit();
        } catch (Exception e) {
            throw new PersistException
                ("Failure to commit secondary transaction has likely caused an inconsistency", e);
        }
    }

    public void exit() throws PersistException {
        try {
            mSecondaryTransaction.exit();
        } finally {
            // Do this second so if there is an exception, the user sees the
            // primary exception, which is presumably more important.
            mPrimaryTransaction.exit();
        }
    }

    public void setForUpdate(boolean forUpdate) {
        mPrimaryTransaction.setForUpdate(forUpdate);
        mSecondaryTransaction.setForUpdate(forUpdate);
    }

    public boolean isForUpdate() {
        return mPrimaryTransaction.isForUpdate() && mSecondaryTransaction.isForUpdate();
    }

    public void setDesiredLockTimeout(int timeout, TimeUnit unit) {
        mPrimaryTransaction.setDesiredLockTimeout(timeout, unit);
        mSecondaryTransaction.setDesiredLockTimeout(timeout, unit);
    }

    public IsolationLevel getIsolationLevel() {
        return mPrimaryTransaction.getIsolationLevel()
            .lowestCommon(mSecondaryTransaction.getIsolationLevel());
    }

    public void detach() {
        mPrimaryTransaction.detach();
        try {
            mSecondaryTransaction.detach();
        } catch (IllegalStateException e) {
            mPrimaryTransaction.attach();
            throw e;
        }
    }

    public void attach() {
        mPrimaryTransaction.attach();
        try {
            mSecondaryTransaction.attach();
        } catch (IllegalStateException e) {
            mPrimaryTransaction.detach();
            throw e;
        }
    }
}
