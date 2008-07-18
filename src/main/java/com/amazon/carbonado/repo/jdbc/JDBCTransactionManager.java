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

package com.amazon.carbonado.repo.jdbc;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.txn.TransactionManager;

/**
 * Manages transactions for JDBCRepository.
 *
 * @author Brian S O'Neill
 */
class JDBCTransactionManager extends TransactionManager<JDBCTransaction> {
    private final JDBCExceptionTransformer mExTransformer;

    // Weakly reference repository because thread locals are not cleaned up
    // very quickly.
    private final WeakReference<JDBCRepository> mRepositoryRef;

    JDBCTransactionManager(JDBCRepository repository) {
        mExTransformer = repository.getExceptionTransformer();
        mRepositoryRef = new WeakReference<JDBCRepository>(repository);
    }

    @Override
    protected IsolationLevel selectIsolationLevel(Transaction parent, IsolationLevel level) {
        JDBCRepository repo = mRepositoryRef.get();
        if (repo == null) {
            throw new IllegalStateException("Repository closed");
        }
        return repo.selectIsolationLevel(parent, level);
    }

    @Override
    protected boolean supportsForUpdate() {
        JDBCRepository repo = mRepositoryRef.get();
        return repo != null && repo.supportsSelectForUpdate();
    }

    @Override
    protected JDBCTransaction createTxn(JDBCTransaction parent, IsolationLevel level)
        throws SQLException, FetchException
    {
        JDBCRepository repo = mRepositoryRef.get();
        if (repo == null) {
            throw new IllegalStateException("Repository closed");
        }

        if (parent != null) {
            if (!repo.supportsSavepoints()) {
                // No support for nested transactions, so fake it.
                return parent;
            }
            return new JDBCTransaction(parent, level);
        }

        return new JDBCTransaction(repo.getConnectionForTxn(level));
    }

    @Override
    protected void reuseTxn(JDBCTransaction txn) throws SQLException {
        txn.reuse();
    }

    @Override
    protected boolean commitTxn(JDBCTransaction txn) throws PersistException {
        try {
            txn.commit();
            return true;
        } catch (Throwable e) {
            throw mExTransformer.toPersistException(e);
        }
    }

    @Override
    protected void abortTxn(JDBCTransaction txn) throws PersistException {
        try {
            Connection con;
            if ((con = txn.abort()) != null) {
                JDBCRepository repo = mRepositoryRef.get();
                if (repo == null) {
                    con.close();
                } else {
                    repo.closeConnection(con);
                }
            }
        } catch (Throwable e) {
            throw mExTransformer.toPersistException(e);
        }
    }
}
