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

import java.sql.SQLException;
import java.util.Collection;

import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.spi.AbstractRepositoryBuilder;

/**
 * Builds a repository instance backed by a JDBC accessible database.
 * JDBCRepository is not independent of the underlying database schema, and so
 * it requires matching tables and columns in the database. It will not alter
 * or create tables. Use the {@link com.amazon.carbonado.Alias Alias}
 * annotation to control precisely which tables and columns must be matched up.
 *
 * <p>Note: The current JDBC repository implementation makes certain
 * assumptions about the database it is accessing. It must support transactions
 * and multiple statements per connection. If it doesn't support savepoints,
 * then nested transactions are faked -- rollback of inner transaction will
 * appear to do nothing.
 *
 * <p>
 * The following extra capabilities are supported:
 * <ul>
 * <li>{@link com.amazon.carbonado.capability.IndexInfoCapability IndexInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.StorableInfoCapability StorableInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.ShutdownCapability ShutdownCapability}
 * <li>{@link JDBCConnectionCapability JDBCConnectionCapability}
 * </ul>
 *
 * @author Brian S O'Neill
 */
public class JDBCRepositoryBuilder extends AbstractRepositoryBuilder {
    private String mName;
    private boolean mIsMaster = true;
    private DataSource mDataSource;
    private boolean mDataSourceLogging;
    private String mCatalog;
    private String mSchema;
    private String mDriverClassName;
    private String mURL;
    private String mUsername;
    private String mPassword;

    public JDBCRepositoryBuilder() {
    }

    public JDBCRepository build(AtomicReference<Repository> rootRef) throws RepositoryException {
        assertReady();
        JDBCRepository repo = new JDBCRepository
            (rootRef, getName(), isMaster(), getDataSource(), mCatalog, mSchema);
        rootRef.set(repo);
        return repo;
    }

    public String getName() {
        return mName;
    }

    public void setName(String name) {
        mName = name;
    }

    public boolean isMaster() {
        return mIsMaster;
    }

    public void setMaster(boolean b) {
        mIsMaster = b;
    }

    /**
     * Set the source of JDBC connections, overriding all other database
     * connectivity configuration in this object.
     */
    public void setDataSource(DataSource dataSource) {
        mDataSource = dataSource;
        mDriverClassName = null;
        mURL = null;
        mUsername = null;
        mPassword = null;
    }

    /**
     * Returns the source of JDBC connections, which defaults to a non-pooling
     * source if driver class, driver URL, username, and password are all
     * supplied.
     *
     * @throws ConfigurationException if driver class wasn't found
     */
    public DataSource getDataSource() throws ConfigurationException {
        if (mDataSource == null) {
            if (mDriverClassName != null && mURL != null) {
                try {
                    mDataSource = new SimpleDataSource
                        (mDriverClassName, mURL, mUsername, mPassword);
                } catch (SQLException e) {
                    Throwable cause = e.getCause();
                    if (cause == null) {
                        cause = e;
                    }
                    throw new ConfigurationException(cause);
                }
            }
        }

        DataSource ds = mDataSource;
        if (getDataSourceLogging() && !(ds instanceof LoggingDataSource)) {
            ds = LoggingDataSource.create(ds);
        }

        return ds;
    }

    /**
     * Pass true to enable debug logging. By default, it is false.
     *
     * @see LoggingDataSource
     */
    public void setDataSourceLogging(boolean b) {
        mDataSourceLogging = b;
    }

    /**
     * Returns true if debug logging is enabled.
     *
     * @see LoggingDataSource
     */
    public boolean getDataSourceLogging() {
        return mDataSourceLogging;
    }

    /**
     * Optionally set the catalog to search for metadata.
     */
    public void setCatalog(String catalog) {
        mCatalog = catalog;
    }

    /**
     * Returns the optional catalog to search for metadata.
     */
    public String getCatalog() {
        return mCatalog;
    }

    /**
     * Optionally set the schema to search for metadata.
     */
    public void setSchema(String schema) {
        mSchema = schema;
    }

    /**
     * Returns the optional schema to search for metadata.
     */
    public String getSchema() {
        return mSchema;
    }

    /**
     * Set the JDBC driver class name, which is required if a DataSource was not provided.
     */
    public void setDriverClassName(String driverClassName) {
        mDriverClassName = driverClassName;
    }

    /**
     * Returns the driver class name, which may be null if a DataSource was provided.
     */
    public String getDriverClassName() {
        return mDriverClassName;
    }

    /**
     * Set the JDBC connection URL, which is required if a DataSource was not
     * provided.
     */
    public void setDriverURL(String url) {
        mURL = url;
    }

    /**
     * Returns the connection URL, which may be null if a DataSource was
     * provided.
     */
    public String getDriverURL() {
        return mURL;
    }

    /**
     * Optionally set the username to use with DataSource.
     */
    public void setUserName(String username) {
        mUsername = username;
    }

    /**
     * Returns the optional username to use with DataSource.
     */
    public String getUserName() {
        return mUsername;
    }

    /**
     * Optionally set the password to use with DataSource.
     */
    public void setPassword(String password) {
        mPassword = password;
    }

    /**
     * Returns the optional password to use with DataSource.
     */
    public String getPassword() {
        return mPassword;
    }

    public void errorCheck(Collection<String> messages) throws ConfigurationException {
        super.errorCheck(messages);
        if (mDataSource == null) {
            if (mDriverClassName == null) {
                messages.add("driverClassName missing");
            }
            if (mURL == null) {
                messages.add("driverURL missing");
            }
            if (messages.size() == 0) {
                // Verify driver exists, only if no other errors.
                getDataSource();
            }
        }
    }
}
