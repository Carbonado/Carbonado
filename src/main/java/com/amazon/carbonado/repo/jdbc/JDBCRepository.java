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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.TriggerFactory;
import com.amazon.carbonado.UnsupportedTypeException;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.capability.ShutdownCapability;
import com.amazon.carbonado.capability.StorableInfoCapability;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.sequence.SequenceCapability;
import com.amazon.carbonado.sequence.SequenceValueProducer;
import com.amazon.carbonado.spi.AbstractRepository;
import com.amazon.carbonado.txn.TransactionManager;
import com.amazon.carbonado.txn.TransactionScope;
import com.amazon.carbonado.util.ThrowUnchecked;

/**
 * Repository implementation backed by a JDBC accessible database.
 * JDBCRepository is not independent of the underlying database schema, and so
 * it requires matching tables and columns in the database. It will not alter
 * or create tables. Use the {@link com.amazon.carbonado.Alias Alias} annotation to
 * control precisely which tables and columns must be matched up.
 *
 * @author Brian S O'Neill
 * @author bcastill
 * @author Adam D Bradley
 * @see JDBCRepositoryBuilder
 */
class JDBCRepository extends AbstractRepository<JDBCTransaction>
    implements Repository,
               IndexInfoCapability,
               ShutdownCapability,
               StorableInfoCapability,
               JDBCConnectionCapability,
               SequenceCapability
{
    /**
     * Attempts to close a DataSource by searching for a "close" method. For
     * some reason, there's no standard way to close a DataSource.
     *
     * @return false if DataSource doesn't have a close method.
     */
    public static boolean closeDataSource(DataSource ds) throws SQLException {
        try {
            Method closeMethod = ds.getClass().getMethod("close");
            try {
                closeMethod.invoke(ds);
            } catch (Throwable e) {
                ThrowUnchecked.fireFirstDeclaredCause(e, SQLException.class);
            }
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    static IsolationLevel mapIsolationLevelFromJdbc(int jdbcLevel) {
        switch (jdbcLevel) {
        case Connection.TRANSACTION_NONE: default:
            return IsolationLevel.NONE;
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            return IsolationLevel.READ_UNCOMMITTED;
        case Connection.TRANSACTION_READ_COMMITTED:
            return IsolationLevel.READ_COMMITTED;
        case Connection.TRANSACTION_REPEATABLE_READ:
            return IsolationLevel.REPEATABLE_READ;
        case Connection.TRANSACTION_SERIALIZABLE:
            return IsolationLevel.SERIALIZABLE;
        }
    }

    static int mapIsolationLevelToJdbc(IsolationLevel level) {
        switch (level) {
        case NONE: default:
            return Connection.TRANSACTION_NONE;
        case READ_UNCOMMITTED:
            return Connection.TRANSACTION_READ_UNCOMMITTED;
        case READ_COMMITTED:
            return Connection.TRANSACTION_READ_COMMITTED;
        case REPEATABLE_READ:
            return Connection.TRANSACTION_REPEATABLE_READ;
        case SNAPSHOT:
            // TODO: not accurate for all databases.
            return Connection.TRANSACTION_SERIALIZABLE;
        case SERIALIZABLE:
            return Connection.TRANSACTION_SERIALIZABLE;
        }
    }

    /**
     * Returns the highest supported level for the given desired level.
     *
     * @return null if not supported
     */
    private static IsolationLevel selectIsolationLevel(DatabaseMetaData md,
                                                       IsolationLevel desiredLevel)
        throws SQLException, RepositoryException
    {
        while (!md.supportsTransactionIsolationLevel(mapIsolationLevelToJdbc(desiredLevel))) {
            switch (desiredLevel) {
            case READ_UNCOMMITTED:
                desiredLevel = IsolationLevel.READ_COMMITTED;
                break;
            case READ_COMMITTED:
                desiredLevel = IsolationLevel.REPEATABLE_READ;
                break;
            case REPEATABLE_READ:
                desiredLevel = IsolationLevel.SERIALIZABLE;
                break;
            case SNAPSHOT:
                desiredLevel = IsolationLevel.SERIALIZABLE;
                break;
            case SERIALIZABLE: default:
                return null;
            }
        }
        return desiredLevel;
    }

    private final Log mLog = LogFactory.getLog(getClass());

    private final boolean mIsMaster;
    final Iterable<TriggerFactory> mTriggerFactories;
    private final AtomicReference<Repository> mRootRef;
    private final String mDatabaseProductName;
    private final DataSource mDataSource;
    private final boolean mDataSourceClose;
    private final String mCatalog;
    private final String mSchema;
    private final Integer mFetchSize;
    private final boolean mPrimaryKeyCheckDisabled;

    // Maps Storable types which should have automatic version management.
    private Map<String, Boolean> mAutoVersioningMap;

    // Maps Storable types which should not auto reload after insert or update.
    private Map<String, Boolean> mSuppressReloadMap;

    // Track all open connections so that they can be closed when this
    // repository is closed.
    private Map<Connection, Object> mOpenConnections;
    private final Lock mOpenConnectionsLock;

    private final boolean mSupportsSavepoints;
    private final boolean mSupportsSelectForUpdate;
    private final boolean mSupportsScrollInsensitiveReadOnly;

    private final IsolationLevel mDefaultIsolationLevel;
    private final int mJdbcDefaultIsolationLevel;

    private final JDBCSupportStrategy mSupportStrategy;
    private JDBCExceptionTransformer mExceptionTransformer;

    private final SchemaResolver mResolver;

    private final JDBCTransactionManager mTxnMgr;

    // Mappings from IsolationLevel to best matching supported level.
    final IsolationLevel mReadUncommittedLevel;
    final IsolationLevel mReadCommittedLevel;
    final IsolationLevel mRepeatableReadLevel;
    final IsolationLevel mSerializableLevel;

    /**
     * @param name name to give repository instance
     * @param isMaster when true, storables in this repository must manage
     * version properties and sequence properties
     * @param dataSource provides JDBC database connections
     * @param catalog optional catalog to search for tables -- actual meaning
     * is database independent
     * @param schema optional schema to search for tables -- actual meaning is
     * is database independent
     * @param forceStoredSequence tells the repository to use a stored sequence
     * even if the database supports native sequences
     */
    @SuppressWarnings("unchecked")
    JDBCRepository(AtomicReference<Repository> rootRef,
                   String name, boolean isMaster,
                   Iterable<TriggerFactory> triggerFactories,
                   DataSource dataSource, boolean dataSourceClose,
                   String catalog, String schema,
                   Integer fetchSize,
                   Map<String, Boolean> autoVersioningMap,
                   Map<String, Boolean> suppressReloadMap,
                   String sequenceSelectStatement, boolean forceStoredSequence, boolean primaryKeyCheckDisabled,
                   SchemaResolver resolver)
        throws RepositoryException
    {
        super(name);
        if (dataSource == null) {
            throw new IllegalArgumentException("DataSource cannot be null");
        }
        mIsMaster = isMaster;
        mTriggerFactories = triggerFactories;
        mRootRef = rootRef;
        mDataSource = dataSource;
        mDataSourceClose = dataSourceClose;
        mCatalog = catalog;
        mSchema = schema;
        mFetchSize = fetchSize;
        mPrimaryKeyCheckDisabled = primaryKeyCheckDisabled;

        mAutoVersioningMap = autoVersioningMap;
        mSuppressReloadMap = suppressReloadMap;

        mResolver = resolver;

        mOpenConnections = new IdentityHashMap<Connection, Object>();
        mOpenConnectionsLock = new ReentrantLock(true);

        // Temporarily set to generic one, in case there's a problem during initialization.
        mExceptionTransformer = new JDBCExceptionTransformer();

        mTxnMgr = new JDBCTransactionManager(this);

        getLog().info("Opening repository \"" + getName() + '"');

        // Test connectivity and get some info on transaction isolation levels.
        Connection con = getConnection();
        try {
            DatabaseMetaData md = con.getMetaData();
            if (md == null || !md.supportsTransactions()) {
                throw new RepositoryException("Database does not support transactions");
            }

            mDatabaseProductName = md.getDatabaseProductName();

            boolean supportsSavepoints;
            try {
                supportsSavepoints = md.supportsSavepoints();
            } catch (AbstractMethodError e) {
                supportsSavepoints = false;
            }

            if (supportsSavepoints) {
                con.setAutoCommit(false);
                // Some JDBC drivers (HSQLDB) lie about their savepoint support.
                try {
                    con.setSavepoint();
                } catch (SQLException e) {
                    mLog.warn("JDBC driver for " + mDatabaseProductName +
                              " reports supporting savepoints, but it " +
                              "doesn't appear to work: " + e);
                    supportsSavepoints = false;
                } finally {
                    con.rollback();
                    con.setAutoCommit(true);
                }
            }

            mSupportsSavepoints = supportsSavepoints;
            mSupportsSelectForUpdate = md.supportsSelectForUpdate();
            mSupportsScrollInsensitiveReadOnly = md.supportsResultSetConcurrency
                (ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            mJdbcDefaultIsolationLevel = md.getDefaultTransactionIsolation();
            mDefaultIsolationLevel = mapIsolationLevelFromJdbc(mJdbcDefaultIsolationLevel);

            mReadUncommittedLevel = selectIsolationLevel(md, IsolationLevel.READ_UNCOMMITTED);
            mReadCommittedLevel   = selectIsolationLevel(md, IsolationLevel.READ_COMMITTED);
            mRepeatableReadLevel  = selectIsolationLevel(md, IsolationLevel.REPEATABLE_READ);
            mSerializableLevel    = selectIsolationLevel(md, IsolationLevel.SERIALIZABLE);
        } catch (SQLException e) {
            throw toRepositoryException(e);
        } finally {
            try {
                closeConnection(con);
            } catch (SQLException e) {
                // Don't care.
            }
        }

        mSupportStrategy = JDBCSupportStrategy.createStrategy(this);
        if (forceStoredSequence) {
            mSupportStrategy.setSequenceSelectStatement(null);
        } else if (sequenceSelectStatement != null && sequenceSelectStatement.length() > 0) {
            mSupportStrategy.setSequenceSelectStatement(sequenceSelectStatement);
        }
        mSupportStrategy.setForceStoredSequence(forceStoredSequence);
        mExceptionTransformer = mSupportStrategy.createExceptionTransformer();

        getLog().info("Opened repository \"" + getName() + '"');

        setAutoShutdownEnabled(true);
    }

    public DataSource getDataSource() {
        return mDataSource;
    }

    /**
     * Returns true if a transaction is in progress and it is for update.
     */
    public boolean isTransactionForUpdate() {
        return localTransactionScope().isForUpdate();
    }

    /**
     * Convenience method that calls into {@link JDBCStorableIntrospector}.
     *
     * @param type Storable type to examine
     * @throws MalformedTypeException if Storable type is not well-formed
     * @throws RepositoryException if there was a problem in accessing the database
     * @throws IllegalArgumentException if type is null
     */
    public <S extends Storable> JDBCStorableInfo<S> examineStorable(Class<S> type)
        throws RepositoryException, SupportException
    {
        try {
            return JDBCStorableIntrospector
                .examine(type, mDataSource, mCatalog, mSchema, mResolver, mPrimaryKeyCheckDisabled);
        } catch (SQLException e) {
            throw toRepositoryException(e);
        }
    }

    public <S extends Storable> IndexInfo[] getIndexInfo(Class<S> storableType)
        throws RepositoryException
    {
        return ((JDBCStorage) storageFor(storableType)).getIndexInfo();
    }

    public String[] getUserStorableTypeNames() {
        // We don't register Storable types persistently, so just return what
        // we know right now.
        ArrayList<String> names = new ArrayList<String>();
        for (Storage storage : allStorage()) {
            names.add(storage.getStorableType().getName());
        }
        return names.toArray(new String[names.size()]);
    }

    public boolean isSupported(Class<Storable> type) {
        if (type == null) {
            return false;
        }
        try {
            examineStorable(type);
            return true;
        } catch (RepositoryException e) {
            return false;
        }
    }

    public boolean isPropertySupported(Class<Storable> type, String name) {
        if (type == null || name == null) {
            return false;
        }
        try {
            JDBCStorableProperty<?> prop = examineStorable(type).getAllProperties().get(name);
            return prop == null ? false : prop.isSupported();
        } catch (RepositoryException e) {
            return false;
        }
    }

    /**
     * Convenience method to convert a regular StorableProperty into a
     * JDBCStorableProperty.
     *
     * @throws UnsupportedOperationException if JDBCStorableProperty is not supported
     */
    <S extends Storable> JDBCStorableProperty<S>
        getJDBCStorableProperty(StorableProperty<S> property)
        throws RepositoryException, SupportException
    {
        JDBCStorableInfo<S> info = examineStorable(property.getEnclosingType());
        JDBCStorableProperty<S> jProperty = info.getAllProperties().get(property.getName());
        if (!jProperty.isSupported()) {
            throw new UnsupportedOperationException
                ("Property is not supported: " + property.getName());
        }
        return jProperty;
    }

    public String getDatabaseProductName() {
        return mDatabaseProductName;
    }

    /**
     * Any connection returned by this method must be closed by calling
     * yieldConnection on this repository.
     */
    public Connection getConnection() throws FetchException {
        try {
            if (mOpenConnections == null) {
                throw new FetchException("Repository is closed");
            }

            JDBCTransaction txn = localTransactionScope().getTxn();
            if (txn != null) {
                // Return the connection used by the current transaction.
                return txn.getConnection();
            }

            // Get connection outside lock section since it may block.
            Connection con = mDataSource.getConnection();
            con.setAutoCommit(true);

            mOpenConnectionsLock.lock();
            try {
                if (mOpenConnections == null) {
                    con.close();
                    throw new FetchException("Repository is closed");
                }
                mOpenConnections.put(con, null);
            } finally {
                mOpenConnectionsLock.unlock();
            }

            return con;
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    /**
     * Called by JDBCTransactionManager.
     */
    Connection getConnectionForTxn(IsolationLevel level) throws FetchException {
        try {
            if (mOpenConnections == null) {
                throw new FetchException("Repository is closed");
            }

            // Get connection outside lock section since it may block.
            Connection con = mDataSource.getConnection();

            if (level == IsolationLevel.NONE) {
                con.setAutoCommit(true);
            } else {
                con.setAutoCommit(false);
                if (level != mDefaultIsolationLevel) {
                    con.setTransactionIsolation(mapIsolationLevelToJdbc(level));
                }
            }

            mOpenConnectionsLock.lock();
            try {
                if (mOpenConnections == null) {
                    con.close();
                    throw new FetchException("Repository is closed");
                }
                mOpenConnections.put(con, null);
            } finally {
                mOpenConnectionsLock.unlock();
            }

            return con;
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    /**
     * Gives up a connection returned from getConnection. Connection must be
     * yielded in same thread that retrieved it.
     */
    public void yieldConnection(Connection con) throws FetchException {
        try {
            if (con.getAutoCommit()) {
                closeConnection(con);
            }
            // Connections which aren't auto-commit are in a transaction. Keep
            // them around instead of closing them.
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    void closeConnection(Connection con) throws SQLException {
        mOpenConnectionsLock.lock();
        try {
            if (mOpenConnections != null) {
                mOpenConnections.remove(con);
            }
        } finally {
            mOpenConnectionsLock.unlock();
        }
        // Close connection outside lock section since it may block.
        con.close();
    }

    boolean supportsSavepoints() {
        return mSupportsSavepoints;
    }

    boolean supportsSelectForUpdate() {
        return mSupportsSelectForUpdate;
    }

    boolean supportsScrollInsensitiveReadOnly() {
        return mSupportsScrollInsensitiveReadOnly;
    }

    /**
     * Returns the highest supported level for the given desired level.
     *
     * @return null if not supported
     */
    IsolationLevel selectIsolationLevel(Transaction parent, IsolationLevel desiredLevel) {
        if (desiredLevel == null) {
            if (parent == null) {
                desiredLevel = mDefaultIsolationLevel;
            } else {
                desiredLevel = parent.getIsolationLevel();
            }
        } else if (parent != null) {
            IsolationLevel parentLevel = parent.getIsolationLevel();
            // Can promote to higher level, but not lower.
            if (parentLevel.compareTo(desiredLevel) >= 0) {
                desiredLevel = parentLevel;
            } else {
                return null;
            }
        }

        switch (desiredLevel) {
        case NONE:
            return IsolationLevel.NONE;
        case READ_UNCOMMITTED:
            return mReadUncommittedLevel;
        case READ_COMMITTED:
            return mReadCommittedLevel;
        case REPEATABLE_READ:
            return mRepeatableReadLevel;
        case SERIALIZABLE:
            return mSerializableLevel;
        }

        return null;
    }

    JDBCSupportStrategy getSupportStrategy() {
        return mSupportStrategy;
    }

    Repository getRootRepository() {
        return mRootRef.get();
    }

    Integer getFetchSize() {
        return mFetchSize;
    }

    /**
     * Transforms the given throwable into an appropriate fetch exception. If
     * it already is a fetch exception, it is simply casted.
     *
     * @param e required exception to transform
     * @return FetchException, never null
     */
    public FetchException toFetchException(Throwable e) {
        return mExceptionTransformer.toFetchException(e);
    }

    /**
     * Transforms the given throwable into an appropriate persist exception. If
     * it already is a persist exception, it is simply casted.
     *
     * @param e required exception to transform
     * @return PersistException, never null
     */
    public PersistException toPersistException(Throwable e) {
        return mExceptionTransformer.toPersistException(e);
    }

    /**
     * Transforms the given throwable into an appropriate repository
     * exception. If it already is a repository exception, it is simply casted.
     *
     * @param e required exception to transform
     * @return RepositoryException, never null
     */
    public RepositoryException toRepositoryException(Throwable e) {
        return mExceptionTransformer.toRepositoryException(e);
    }

    /**
     * Examines the SQLSTATE code of the given SQL exception and determines if
     * it is a unique constaint violation.
     */
    public boolean isUniqueConstraintError(SQLException e) {
        return mExceptionTransformer.isUniqueConstraintError(e);
    }

    JDBCExceptionTransformer getExceptionTransformer() {
        return mExceptionTransformer;
    }
    
    @Override
    protected void shutdownHook() {
        // Close all open connections.
        mOpenConnectionsLock.lock();
        try {
            if (mOpenConnections != null) {
                for (Connection con : mOpenConnections.keySet()) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        getLog().warn(null, e);
                    }
                }
                mOpenConnections = null;
            }
        } finally {
            mOpenConnectionsLock.unlock();
        }

        if (mDataSourceClose) {
            mLog.info("Closing DataSource: " + mDataSource);
            try {
                if (!closeDataSource(mDataSource)) {
                    mLog.info("DataSource doesn't have a close method: " +
                              mDataSource.getClass().getName());
                }
            } catch (SQLException e) {
                mLog.error("Failed to close DataSource", e);
            }
        }
    }

    @Override
    protected Log getLog() {
        return mLog;
    }

    @Override
    protected <S extends Storable> Storage<S> createStorage(Class<S> type)
        throws RepositoryException
    {
        JDBCStorableInfo<S> info = examineStorable(type);
        if (!info.isSupported()) {
            throw new UnsupportedTypeException("Independent type not supported", type);
        }

        Boolean autoVersioning = false;
        if (mAutoVersioningMap != null) {
            autoVersioning = mAutoVersioningMap.get(type.getName());
            if (autoVersioning == null) {
                // No explicit setting, so check wildcard setting.
                autoVersioning = mAutoVersioningMap.get(null);
                if (autoVersioning == null) {
                    autoVersioning = false;
                }
            }
        }

        Boolean suppressReload = false;
        if (mSuppressReloadMap != null) {
            suppressReload = mSuppressReloadMap.get(type.getName());
            if (suppressReload == null) {
                // No explicit setting, so check wildcard setting.
                suppressReload = mSuppressReloadMap.get(null);
                if (suppressReload == null) {
                    suppressReload = false;
                }
            }
        }

        return new JDBCStorage<S>(this, info, mIsMaster, autoVersioning, suppressReload);
    }

    @Override
    protected SequenceValueProducer createSequenceValueProducer(String name)
        throws RepositoryException
    {
        return mSupportStrategy.createSequenceValueProducer(name);
    }

    @Override
    protected final TransactionManager<JDBCTransaction> transactionManager() {
        return mTxnMgr;
    }

    @Override
    protected final TransactionScope<JDBCTransaction> localTransactionScope() {
        return mTxnMgr.localScope();
    }
}
