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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.cursor.AbstractCursor;

/**
 * Cursor implementation that queries a PreparedStatement.
 *
 * @author Brian S O'Neill
 */
class JDBCCursor<S extends Storable> extends AbstractCursor<S> {
    private final JDBCStorage<S> mStorage;
    private final Connection mConnection;
    private final PreparedStatement mStatement;
    private final boolean mScrollInsensitiveReadOnly;

    private ResultSet mResultSet;
    private boolean mHasNext;

    /**
     * @param scrollInsensitiveReadOnly when true, statement is
     * TYPE_SCROLL_INSENSITIVE and CONCUR_READ_ONLY.
     * @throws SQLException from executeQuery on statement. Caller must clean
     * up when this happens by closing statement and connection.
     */
    JDBCCursor(JDBCStorage<S> storage,
               Connection con,
               PreparedStatement statement,
               boolean scrollInsensitiveReadOnly)
        throws SQLException
    {
        mStorage = storage;
        mConnection = con;
        mStatement = statement;
        mScrollInsensitiveReadOnly = scrollInsensitiveReadOnly;
        mResultSet = statement.executeQuery();
    }

    public void close() throws FetchException {
        if (mResultSet != null) {
            try {
                mResultSet.close();
                mStatement.close();
                mStorage.mRepository.yieldConnection(mConnection);
            } catch (SQLException e) {
                throw mStorage.getJDBCRepository().toFetchException(e);
            } finally {
                mResultSet = null;
                mHasNext = false;
            }
        }
    }

    public boolean hasNext() throws FetchException {
        ResultSet rs = mResultSet;
        if (rs == null) {
            return false;
        }
        if (!mHasNext) {
            try {
                mHasNext = rs.next();
            } catch (SQLException e) {
                try {
                    close();
                } catch (FetchException e2) {
                    // Don't care.
                }
                throw mStorage.getJDBCRepository().toFetchException(e);
            }
            if (!mHasNext) {
                close();
            }
        }
        return mHasNext;
    }

    public S next() throws FetchException, NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        try {
            S obj = mStorage.instantiate(mResultSet);
            mHasNext = false;
            return obj;
        } catch (SQLException e) {
            try {
                close();
            } catch (FetchException e2) {
                // Don't care.
            }
            throw mStorage.getJDBCRepository().toFetchException(e);
        }
    }

    public int skipNext(int amount) throws FetchException {
        if (amount <= 0) {
            if (amount < 0) {
                throw new IllegalArgumentException("Cannot skip negative amount: " + amount);
            }
            return 0;
        }

        int actual = 0;

        if (amount > 1 && mScrollInsensitiveReadOnly) {
            // Skip a relative amount, which is preferred.
            if (hasNext()) {
                ResultSet rs = mResultSet;
                try {
                    int rowStart = rs.getRow();
                    mHasNext = rs.relative(amount);
                    int rowEnd = rs.getRow();
                    if (rowEnd == 0) {
                        // Skipped past the end. Move back to find the last row number.
                        if (rs.previous() && (rowEnd = rs.getRow()) != 0) {
                            rowEnd++;
                        } else if (rs.last() && (rowEnd = rs.getRow()) != 0) {
                            rowEnd++;
                        } else {
                            // No clue how many were skipped. It's at least one.
                            rowEnd = rowStart + 1;
                        }
                        // Make sure ResultSet is closed below.
                        mHasNext = false;
                    }
                    actual += rowEnd - rowStart;
                } catch (SQLException e) {
                    try {
                        close();
                    } catch (FetchException e2) {
                        // Don't care.
                    }
                    throw mStorage.getJDBCRepository().toFetchException(e);
                }
                if (!mHasNext) {
                    close();
                }
            }
        } else {
            // Call next a bunch, which is likely slower than relative skipping.
            while (amount > 0) {
                if (hasNext()) {
                    actual++;
                    amount--;
                    mHasNext = false;
                }
            }
        }

        return actual;
    }
}
