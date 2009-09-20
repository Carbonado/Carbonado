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
import java.util.HashMap;
import java.util.Map;

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
 * <li>{@link com.amazon.carbonado.sequence.SequenceCapability SequenceCapability}
 * <li>{@link JDBCConnectionCapability JDBCConnectionCapability}
 * </ul>
 *
 * @author Brian S O'Neill
 * @author bcastill
 * @author Adam D Bradley
 */
public class JDBCRepositoryBuilder extends AbstractRepositoryBuilder {
    private String mName;
    private boolean mIsMaster = true;
    private DataSource mDataSource;
    private boolean mDataSourceClose;
    private boolean mDataSourceLogging;
    private String mCatalog;
    private String mSchema;
    private String mDriverClassName;
    private String mURL;
    private String mUsername;
    private String mPassword;
    private Integer mFetchSize;
    private Map<String, Boolean> mAutoVersioningMap;
    private Map<String, Boolean> mSuppressReloadMap;
    private String mSequenceSelectStatement;
    private boolean mForceStoredSequence;
    private boolean mPrimaryKeyCheckDisabled;

    private SchemaResolver mResolver;

    public JDBCRepositoryBuilder() {
    }

    public Repository build(AtomicReference<Repository> rootRef) throws RepositoryException {
        assertReady();
        JDBCRepository repo = new JDBCRepository
            (rootRef, getName(), isMaster(), getTriggerFactories(),
             getDataSource(), getDataSourceCloseOnShutdown(),
             mCatalog, mSchema,
             mFetchSize,
             getAutoVersioningMap(),
             getSuppressReloadMap(),
             mSequenceSelectStatement, mForceStoredSequence, mPrimaryKeyCheckDisabled,
             mResolver);
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
     * Set the source of JDBC connections, overriding any configuration
     * supported by these methods:
     *
     * <ul>
     * <li>{@link #setDriverClassName}
     * <li>{@link #setDriverURL}
     * <li>{@link #setUserName}
     * <li>{@link #setPassword}
     * </ul>
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
     * Pass true to cause the DataSource to be closed when the repository is
     * closed or shutdown. By default, this option is false.
     *
     * @since 1.2
     */
    public void setDataSourceCloseOnShutdown(boolean b) {
        mDataSourceClose = b;
    }

    /**
     * Returns true if DataSource is closed when the repository is closed or
     * shutdown. By default, this option is false.
     *
     * @since 1.2
     */
    public boolean getDataSourceCloseOnShutdown() {
        return mDataSourceClose;
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

    /**
     * Set the default fetch size when running queries. Pass null to let driver
     * use its own default.
     *
     * @since 1.2
     */
    public void setDefaultFetchSize(Integer size) {
        mFetchSize = size;
    }

    /**
     * Returns the default fetch size when running queries, or null if driver
     * default is used instead.
     *
     * @since 1.2
     */
    public Integer getDefaultFetchSize() {
        return mFetchSize;
    }

    /**
     * By default, JDBCRepository assumes that {@link
     * com.amazon.carbonado.Version version numbers} are initialized and
     * incremented by triggers installed on the database. Enabling automatic
     * versioning here causes the JDBCRepository to manage these operations
     * itself.
     *
     * @param enabled true to enable, false to disable
     * @param className name of Storable type to enable automatic version
     * management on; pass null to enable all
     * @since 1.2
     */
    public void setAutoVersioningEnabled(boolean enabled, String className) {
        if (mAutoVersioningMap == null) {
            mAutoVersioningMap = new HashMap<String, Boolean>();
        }
        mAutoVersioningMap.put(className, enabled);
    }

    private Map<String, Boolean> getAutoVersioningMap() {
        if (mAutoVersioningMap == null) {
            return null;
        }
        return new HashMap<String, Boolean>(mAutoVersioningMap);
    }

    /**
     * By default, JDBCRepository reloads Storables after every insert or
     * update. This ensures that any applied defaults or triggered changes are
     * available to the Storable. If the database has no such defaults or
     * triggers, suppressing reload can improve performance.
     *
     * <p>Note: If Storable has a version property and auto versioning is not
     * enabled, or if the Storable has any automatic properties, the Storable
     * might still be reloaded.
     *
     * @param suppress true to suppress, false to unsuppress
     * @param className name of Storable type to suppress reload for; pass null
     * to suppress all
     * @since 1.1.3
     */
    public void setSuppressReload(boolean suppress, String className) {
        if (mSuppressReloadMap == null) {
            mSuppressReloadMap = new HashMap<String, Boolean>();
        }
        mSuppressReloadMap.put(className, suppress);
    }

    private Map<String, Boolean> getSuppressReloadMap() {
        if (mSuppressReloadMap == null) {
            return null;
        }
        return new HashMap<String, Boolean>(mSuppressReloadMap);
    }

    /**
     * Returns the native sequence select statement, which is null if the
     * default is chosen.
     *
     * @since 1.2
     */
    public String getSequenceSelectStatement() {
        return mSequenceSelectStatement;
    }

    /**
     * Override the default native sequence select statement with a printf.
     * For example, "SELECT %s.NEXTVAL FROM DUAL".
     *
     * @since 1.2
     */
    public void setSequenceSelectStatement(String sequenceSelectStatement) {
        mSequenceSelectStatement = sequenceSelectStatement;
    }

    /**
     * Returns true if native sequences should not be used.
     *
     * @since 1.2
     */
    public boolean isForceStoredSequence() {
        return mForceStoredSequence;
    }

    /**
     * By default, native sequences are used if supported. Otherwise, a table
     * named "CARBONADO_SEQUENCE" or "CARBONADO_SEQUENCES" is used instead to
     * hold sequence values. When forced, the table is always used instead of
     * native sequences.
     *
     * @since 1.2
     */
    public void setForceStoredSequence(boolean forceStoredSequence) {
        mForceStoredSequence = forceStoredSequence;
    }

    /**
     * By default, JDBCRepository makes sure that every declared primary key
     * in the database table for a Storable lines up with a declared
     * PrimaryKey or AlternateKey.  This is not always the desired behavior; 
     * for example, you may have a table which uses a bigint for its actual
     * primary key but uses another column with a unique index as the
     * "primary" key from the application's point of view.  Setting this
     * value to true allows this check to fail gracefully instead of
     * throwing a {@link com.amazon.carbonado.MismatchException}.
     * 
     * @since 1.2
     */
    public void setPrimaryKeyCheckDisabled(boolean primaryKeyCheckDisabled) {
        mPrimaryKeyCheckDisabled = primaryKeyCheckDisabled;
    }

    @Override
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

    // Experimental feature.
    void setSchemaResolver(SchemaResolver resolver) {
        mResolver = resolver;
    }
}
