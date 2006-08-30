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

import java.util.ArrayList;
import java.util.List;

import java.sql.Connection;
import java.sql.Savepoint;
import java.sql.SQLException;

import com.amazon.carbonado.IsolationLevel;

/**
 * JDBCTransaction is just a wrapper around a connection and (optionally) a
 * savepoint.
 *
 * @author Brian S O'Neill
 */
class JDBCTransaction {
    private final Connection mConnection;
    // Use TRANSACTION_NONE as a magic value to indicate that the isolation
    // level need not be changed when the transaction ends. This is a little
    // optimization to avoid a round trip call to the remote database.
    private final int mOriginalLevel;
    private Savepoint mSavepoint;

    private List<JDBCLob> mRegisteredLobs;

    JDBCTransaction(Connection con) {
        mConnection = con;
        // Don't change level upon abort.
        mOriginalLevel = Connection.TRANSACTION_NONE;
    }

    /**
     * Construct a nested transaction.
     */
    JDBCTransaction(JDBCTransaction parent, IsolationLevel level) throws SQLException {
        mConnection = parent.mConnection;

        if (level == null) {
            // Don't change level upon abort.
            mOriginalLevel = Connection.TRANSACTION_NONE;
        } else {
            int newLevel = JDBCRepository.mapIsolationLevelToJdbc(level);
            int originalLevel = mConnection.getTransactionIsolation();
            if (newLevel == originalLevel) {
                // Don't change level upon abort.
                mOriginalLevel = Connection.TRANSACTION_NONE;
            } else {
                // Don't change level upon abort.
                mOriginalLevel = originalLevel;
                mConnection.setTransactionIsolation(newLevel);
            }
        }

        mSavepoint = mConnection.setSavepoint();
    }

    Connection getConnection() {
        return mConnection;
    }

    void commit() throws SQLException {
        if (mSavepoint == null) {
            mConnection.commit();
        } else {
            // Don't commit, make a new savepoint. Root transaction has no
            // savepoint, and so it will do the real commit.
            mSavepoint = mConnection.setSavepoint();
        }
    }

    /**
     * @return connection to close, or null if not ready to because this was a
     * nested transaction
     */
    Connection abort() throws SQLException {
        if (mRegisteredLobs != null) {
            for (JDBCLob lob : mRegisteredLobs) {
                lob.close();
            }
            mRegisteredLobs = null;
        }
        if (mSavepoint == null) {
            mConnection.rollback();
            mConnection.setAutoCommit(true);
            return mConnection;
        } else {
            mConnection.rollback(mSavepoint);
            if (mOriginalLevel != Connection.TRANSACTION_NONE) {
                mConnection.setTransactionIsolation(mOriginalLevel);
            }
            mSavepoint = null;
            return null;
        }
    }

    void register(JDBCLob lob) {
        if (mRegisteredLobs == null) {
            mRegisteredLobs = new ArrayList<JDBCLob>(4);
        }
        mRegisteredLobs.add(lob);
    }
}
