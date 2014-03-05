/*
 * Copyright 2006-2013 Amazon Technologies, Inc. or its affiliates.
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

import java.util.concurrent.TimeUnit;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;

/**
 * ReadOnlyTransaction wraps an another transaction. Its only function is to
 * serve as a marker for the ReplicatedStorage triggers that no write
 * operations are allowed.
 *
 * @author Jesse Morgan
 */
class ReadOnlyTransaction implements Transaction {
    private final Transaction mTxn;

    public ReadOnlyTransaction(Transaction txn) {
        mTxn = txn;
    }

    @Override
    public void commit() throws PersistException {
        mTxn.commit();
    }

    @Override
    public void exit() throws PersistException {
        mTxn.exit();
    }

    @Override
    public void setForUpdate(boolean forUpdate) {
        mTxn.setForUpdate(forUpdate);
    }

    @Override
    public boolean isForUpdate() {
        return mTxn.isForUpdate();
    }

    @Override
    public void setDesiredLockTimeout(int timeout, TimeUnit unit) {
        mTxn.setDesiredLockTimeout(timeout, unit);
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return mTxn.getIsolationLevel();
    }

    @Override
    public void detach() {
        mTxn.detach();
    }

    @Override
    public void attach() {
        mTxn.attach();
    }

    @Override
    public boolean preCommit() throws PersistException {
        return mTxn.preCommit();
    }

    @Override
    public String toString() {
        return "ReadOnlyTransaction wrapping { " + mTxn.toString() + " }";
    }

    @Override
    public void close() throws PersistException {
        exit();
    }
}

