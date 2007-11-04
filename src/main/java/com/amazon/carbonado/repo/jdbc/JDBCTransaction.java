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
    // Use this magic value to indicate that the isolation level need not be
    // changed when the transaction ends. This is a little optimization to
    // avoid a round trip call to the remote database.
    private static final int LEVEL_NOT_CHANGED = -1;

    private final boolean mIsNested;
    private final Connection mConnection;
    private final int mOriginalLevel;

    private boolean mReady = true;

    private Savepoint mSavepoint;

    private List<JDBCLob> mRegisteredLobs;

    JDBCTransaction(Connection con) {
        mIsNested = false;
        mConnection = con;
        // Don't change level upon abort.
        mOriginalLevel = LEVEL_NOT_CHANGED;
    }

    /**
     * Construct a nested transaction.
     */
    JDBCTransaction(JDBCTransaction parent, IsolationLevel level) throws SQLException {
        mIsNested = true;
        mConnection = parent.mConnection;

        if (level == null) {
            // Don't change level upon abort.
            mOriginalLevel = LEVEL_NOT_CHANGED;
        } else {
            int newLevel = JDBCRepository.mapIsolationLevelToJdbc(level);
            int originalLevel = mConnection.getTransactionIsolation();
            if (newLevel == originalLevel) {
                // Don't change level upon abort.
                mOriginalLevel = LEVEL_NOT_CHANGED;
            } else {
                // Do change level upon abort.
                mOriginalLevel = originalLevel;
                if (originalLevel == Connection.TRANSACTION_NONE) {
                    mConnection.setAutoCommit(false);
                }
                mConnection.setTransactionIsolation(newLevel);
            }
        }

        mSavepoint = mConnection.setSavepoint();
    }

    Connection getConnection() {
        return mConnection;
    }

    void reuse() throws SQLException {
        if (mIsNested && mSavepoint == null) {
            mSavepoint = mConnection.setSavepoint();
        }
        mReady = true;
    }

    void commit() throws SQLException {
        if (mIsNested) {
            mSavepoint = null;
        } else {
            mConnection.commit();
        }
        mReady = false;
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

        if (mIsNested) {
            if (mReady) {
                if (mSavepoint != null) {
                    mConnection.rollback(mSavepoint);
                    mSavepoint = null;
                }
                mReady = false;
            }

            if (mOriginalLevel != LEVEL_NOT_CHANGED) {
                if (mOriginalLevel == Connection.TRANSACTION_NONE) {
                    mConnection.setAutoCommit(true);
                } else {
                    mConnection.setTransactionIsolation(mOriginalLevel);
                }
            }

            return null;
        } else {
            if (mReady) {
                mConnection.rollback();
                mReady = false;
            }
            return mConnection;
        }
    }

    void register(JDBCLob lob) {
        if (mRegisteredLobs == null) {
            mRegisteredLobs = new ArrayList<JDBCLob>(4);
        }
        mRegisteredLobs.add(lob);
    }
}
