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

    public SimpleDataSource(String driverClass, String driverURL, String username, String password)
        throws SQLException
    {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            SQLException e2 = new SQLException();
            e2.initCause(e);
            throw e2;
        }
        mURL = driverURL;
        mProperties = new Properties();
        if (username != null) {
            mProperties.put("user", username);
        }
        if (password != null) {
            mProperties.put("password", password);
        }
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(mURL, mProperties);
    }

    public Connection getConnection(String username, String password) throws SQLException {
        Properties props = new Properties();
        if (username != null) {
            props.put("user", username);
        }
        if (password != null) {
            props.put("password", password);
        }
        return DriverManager.getConnection(mURL, props);
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

    /* JDK 1.6 features
    public <T extends java.sql.BaseQuery> T createQueryObject(Class<T> ifc)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public <T extends java.sql.BaseQuery> T createQueryObject(Class<T> ifc, DataSource ds)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.QueryObjectGenerator getQueryObjectGenerator() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
    */
}
