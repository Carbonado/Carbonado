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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.PrintWriter;
import java.util.Properties;

import javax.sql.DataSource;

/**
 * SimpleDataSource does not implement any connection pooling.
 *
 * @author Brian S O'Neill
 */
public class SimpleDataSource implements DataSource {
    private final String mURL;
    private final Properties mProperties;

    private PrintWriter mLogWriter;

    /**
     * @param driverClass JDBC driver to load; can pass null if already loaded
     * @param driverURL JDBC driver URL
     * @param properties optional connection properties
     * @since 1.2
     */
    public SimpleDataSource(String driverClass, String driverURL, Properties properties)
        throws SQLException
    {
        this(driverClass, driverURL, null, null, properties);
    }

    /**
     * @param driverClass JDBC driver to load; can pass null if already loaded
     * @param driverURL JDBC driver URL
     * @param username optional username to connect with
     * @param password optional password to connect with
     */
    public SimpleDataSource(String driverClass, String driverURL, String username, String password)
        throws SQLException
    {
        this(driverClass, driverURL, username, password, null);
    }

    /**
     * @param driverClass JDBC driver to load; can pass null if already loaded
     * @param driverURL JDBC driver URL
     * @param username optional username to connect with
     * @param password optional password to connect with
     * @param properties optional connection properties
     * @since 1.2
     */
    public SimpleDataSource(String driverClass, String driverURL, String username, String password,
                            Properties properties)
        throws SQLException
    {
        if (driverURL == null) {
            throw new IllegalArgumentException("Must supply JDBC URL");
        }

        if (driverClass != null) {
            try {
                Class.forName(driverClass);
            } catch (ClassNotFoundException e) {
                SQLException e2 = new SQLException();
                e2.initCause(e);
                throw e2;
            }
        }

        if (properties == null) {
            properties = new Properties();
        }

        if (username != null) {
            properties.put("user", username);
        }
        if (password != null) {
            properties.put("password", password);
        }

        mURL = driverURL;
        mProperties = properties;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(mURL, mProperties);
    }

    public Connection getConnection(String username, String password) throws SQLException {
        Properties properties = new Properties(mProperties);
        if (username != null) {
            properties.put("user", username);
        }
        if (password != null) {
            properties.put("password", password);
        }
        return DriverManager.getConnection(mURL, properties);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return mLogWriter;
    }

    public void setLogWriter(PrintWriter writer) throws SQLException {
        mLogWriter = writer;
    }

    public void setLoginTimeout(int seconds) throws SQLException {
    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    /**
     * @since 1.2
     */
    public void close() throws SQLException {
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
