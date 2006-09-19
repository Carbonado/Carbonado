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
    private Connection mConnection;
    private PreparedStatement mStatement;
    private ResultSet mResultSet;

    private boolean mHasNext;

    JDBCCursor(JDBCStorage<S> storage,
               Connection con,
               PreparedStatement statement)
        throws SQLException
    {
        mStorage = storage;
        mConnection = con;
        mStatement = statement;
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

        ResultSet rs = mResultSet;
        if (rs == null) {
            return 0;
        }

        mHasNext = true;

        int actual = 0;
        while (amount > 0) {
            try {
                if (rs.next()) {
                    actual++;
                } else {
                    mHasNext = false;
                    close();
                    break;
                }
            } catch (SQLException e) {
                throw mStorage.getJDBCRepository().toFetchException(e);
            }
        }

        return actual;
    }
}
