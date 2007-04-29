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
import java.sql.SQLException;
import java.io.PrintWriter;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wraps another DataSource such that all SQL statements are logged as debug.
 *
 * @author Brian S O'Neill
 */
public class LoggingDataSource implements DataSource {
    /**
     * Wraps the given DataSource which logs to the default log. If debug
     * logging is disabled, the original DataSource is returned.
     */
    public static DataSource create(DataSource ds) {
        return create(ds, null);
    }

    /**
     * Wraps the given DataSource which logs to the given log. If debug logging
     * is disabled, the original DataSource is returned.
     */
    public static DataSource create(DataSource ds, Log log) {
        if (ds == null) {
            throw new IllegalArgumentException();
        }
        if (log == null) {
            log = LogFactory.getLog(LoggingDataSource.class);
        }
        if (!log.isDebugEnabled()) {
            return ds;
        }
        return new LoggingDataSource(log, ds);
    }

    private final Log mLog;
    private final DataSource mDataSource;

    private LoggingDataSource(Log log, DataSource ds) {
        mLog = log;
        mDataSource = ds;
    }

    public Connection getConnection() throws SQLException {
        mLog.debug("DataSource.getConnection()");
        return new LoggingConnection(mLog, mDataSource.getConnection());
    }

    public Connection getConnection(String username, String password) throws SQLException {
        mLog.debug("DataSource.getConnection(username, password)");
        return new LoggingConnection(mLog, mDataSource.getConnection(username, password));
    }

    public PrintWriter getLogWriter() throws SQLException {
        return mDataSource.getLogWriter();
    }

    public void setLogWriter(PrintWriter writer) throws SQLException {
        mDataSource.setLogWriter(writer);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        mDataSource.setLoginTimeout(seconds);
    }

    public int getLoginTimeout() throws SQLException {
        return mDataSource.getLoginTimeout();
    }

    /**
     * @since 1.2
     */
    public void close() throws SQLException {
        mLog.debug("DataSource.close()");
        if (!JDBCRepository.closeDataSource(mDataSource)) {
            mLog.debug("DataSource doesn't have a close method: " +
                       mDataSource.getClass().getName());
        }
    }

    /**
     * @since 1.2
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * @since 1.2
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
