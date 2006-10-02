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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.IdentityHashMap;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import static com.amazon.carbonado.RepositoryBuilder.RepositoryReference;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.UnsupportedTypeException;

import com.amazon.carbonado.capability.Capability;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.capability.ShutdownCapability;
import com.amazon.carbonado.capability.StorableInfoCapability;

import com.amazon.carbonado.info.StorableProperty;

/**
 * Repository implementation backed by a JDBC accessible database.
 * JDBCRepository is not independent of the underlying database schema, and so
 * it requires matching tables and columns in the database. It will not alter
 * or create tables. Use the {@link com.amazon.carbonado.Alias Alias} annotation to
 * control precisely which tables and columns must be matched up.
 *
 * @author Brian S O'Neill
 * @see JDBCRepositoryBuilder
 */
// Note: this class must be public because auto-generated code needs access to it
public class JDBCRepository
    implements Repository,
               IndexInfoCapability,
               ShutdownCapability,
               StorableInfoCapability,
               JDBCConnectionCapability
{

    static IsolationLevel mapIsolationLevelFromJdbc(int jdbcLevel) {
        switch (jdbcLevel) {
        case Connection.TRANSACTION_READ_UNCOMMITTED: default:
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
        case READ_UNCOMMITTED: default:
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

    private final String mName;
    final boolean mIsMaster;
    private final RepositoryReference mRootRef;
    private final String mDatabaseProductName;
    private final DataSource mDataSource;
    private final String mCatalog;
    private final String mSchema;
    private final Map<Class<?>, JDBCStorage<?>> mStorages;

    // Track all open connections so that they can be closed when this
    // repository is closed.
    private Map<Connection, Object> mOpenConnections;

    private final ThreadLocal<JDBCTransactionManager> mCurrentTxnMgr;

    // Weakly tracks all JDBCTransactionManager instances for shutdown.
    private final Map<JDBCTransactionManager, ?> mAllTxnMgrs;

    private final boolean mSupportsSavepoints;
    private final boolean mSupportsSelectForUpdate;

    private final IsolationLevel mDefaultIsolationLevel;
    private final int mJdbcDefaultIsolationLevel;

    private final JDBCSupportStrategy mSupportStrategy;
    private JDBCExceptionTransformer mExceptionTransformer;

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
     * database independent
     */
    @SuppressWarnings("unchecked")
    JDBCRepository(RepositoryReference rootRef,
                   String name, boolean isMaster,
                   DataSource dataSource, String catalog, String schema)
        throws RepositoryException
    {
        if (name == null || dataSource == null) {
            throw new IllegalArgumentException();
        }
        mName = name;
        mIsMaster = isMaster;
        mRootRef = rootRef;
        mDataSource = dataSource;
        mCatalog = catalog;
        mSchema = schema;
        mStorages = new IdentityHashMap<Class<?>, JDBCStorage<?>>();
        mOpenConnections = new IdentityHashMap<Connection, Object>();
        mCurrentTxnMgr = new ThreadLocal<JDBCTransactionManager>();
        mAllTxnMgrs = new WeakIdentityMap();

        // Temporarily set to generic one, in case there's a problem during initialization.
        mExceptionTransformer = new JDBCExceptionTransformer();

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

            mJdbcDefaultIsolationLevel = md.getDefaultTransactionIsolation();
            mDefaultIsolationLevel = mapIsolationLevelFromJdbc(mJdbcDefaultIsolationLevel);

            mReadUncommittedLevel = selectIsolationLevel(md, IsolationLevel.READ_UNCOMMITTED);
            mReadCommittedLevel   = selectIsolationLevel(md, IsolationLevel.READ_COMMITTED);
            mRepeatableReadLevel  = selectIsolationLevel(md, IsolationLevel.REPEATABLE_READ);
            mSerializableLevel    = selectIsolationLevel(md, IsolationLevel.SERIALIZABLE);

        } catch (SQLException e) {
            throw toRepositoryException(e);
        } finally {
            forceYieldConnection(con);
        }

        mSupportStrategy = JDBCSupportStrategy.createStrategy(this);
        mExceptionTransformer = mSupportStrategy.createExceptionTransformer();
    }

    public DataSource getDataSource() {
        return mDataSource;
    }

    public String getName() {
        return mName;
    }

    @SuppressWarnings("unchecked")
    public <S extends Storable> Storage<S> storageFor(Class<S> type) throws RepositoryException {
        // Lock on mAllTxnMgrs to prevent databases from being opened during shutdown.
        synchronized (mAllTxnMgrs) {
            JDBCStorage<S> storage = (JDBCStorage<S>) mStorages.get(type);
            if (storage == null) {
                // Examine and throw exception early if there is a problem.
                JDBCStorableInfo<S> info = examineStorable(type);

                if (!info.isSupported()) {
                    throw new UnsupportedTypeException(type);
                }

                storage = new JDBCStorage<S>(this, info);
                mStorages.put(type, storage);
            }
            return storage;
        }
    }

    public Transaction enterTransaction() {
        return openTransactionManager().enter(null);
    }

    public Transaction enterTransaction(IsolationLevel level) {
        return openTransactionManager().enter(level);
    }

    public Transaction enterTopTransaction(IsolationLevel level) {
        return openTransactionManager().enterTop(level);
    }

    public IsolationLevel getTransactionIsolationLevel() {
        return openTransactionManager().getIsolationLevel();
    }

    /**
     * Returns true if a transaction is in progress and it is for update.
     */
    public boolean isTransactionForUpdate() {
        return openTransactionManager().isForUpdate();
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
            return JDBCStorableIntrospector.examine(type, mDataSource, mCatalog, mSchema);
        } catch (SQLException e) {
            throw toRepositoryException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <C extends Capability> C getCapability(Class<C> capabilityType) {
        if (capabilityType.isInstance(this)) {
            return (C) this;
        }
        return null;
    }

    public <S extends Storable> IndexInfo[] getIndexInfo(Class<S> storableType)
        throws RepositoryException
    {
        return ((JDBCStorage) storageFor(storableType)).getIndexInfo();
    }

    public String[] getUserStorableTypeNames() {
        // We don't register Storable types persistently, so just return what
        // we know right now.
        synchronized (mAllTxnMgrs) {
            String[] names = new String[mStorages.size()];
            int i = 0;
            for (Class<?> type : mStorages.keySet()) {
                names[i++] = type.getName();
            }
            return names;
        }
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

    /**
     * Returns the thread-local JDBCTransactionManager instance, creating it if
     * needed.
     */
    JDBCTransactionManager openTransactionManager() {
        JDBCTransactionManager txnMgr = mCurrentTxnMgr.get();
        if (txnMgr == null) {
            synchronized (mAllTxnMgrs) {
                txnMgr = new JDBCTransactionManager(this);
                mCurrentTxnMgr.set(txnMgr);
                mAllTxnMgrs.put(txnMgr, null);
            }
        }
        return txnMgr;
    }

    public void close() {
        shutdown(false);
    }

    public boolean isAutoShutdownEnabled() {
        return false;
    }

    public void setAutoShutdownEnabled(boolean enabled) {
    }

    public void shutdown() {
        shutdown(true);
    }

    private void shutdown(boolean suspendThreads) {
        synchronized (mAllTxnMgrs) {
            // Close transactions and cursors.
            for (JDBCTransactionManager txnMgr : mAllTxnMgrs.keySet()) {
                if (suspendThreads) {
                    // Lock transaction manager but don't release it. This
                    // prevents other threads from beginning work during
                    // shutdown, which will likely fail along the way.
                    txnMgr.getLock().lock();
                }
                try {
                    txnMgr.close();
                } catch (Throwable e) {
                    getLog().error(null, e);
                }
            }

            // Now close all open connections.
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
        }
    }

    protected Log getLog() {
        return mLog;
    }

    public String getDatabaseProductName() {
        return mDatabaseProductName;
    }

    /**
     * Any connection returned by this method must be closed by calling
     * yieldConnection on this repository.
     */
    // Note: This method must be public for auto-generated code to access it.
    public Connection getConnection() throws FetchException {
        try {
            if (mOpenConnections == null) {
                throw new FetchException("Repository is closed");
            }

            JDBCTransaction txn = openTransactionManager().getTxn();
            if (txn != null) {
                // Return the connection used by the current transaction.
                return txn.getConnection();
            }

            // Get connection outside synchronized section since it may block.
            Connection con = mDataSource.getConnection();
            con.setAutoCommit(true);

            synchronized (mAllTxnMgrs) {
                if (mOpenConnections == null) {
                    con.close();
                    throw new FetchException("Repository is closed");
                }
                mOpenConnections.put(con, null);
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

            // Get connection outside synchronized section since it may block.
            Connection con = mDataSource.getConnection();
            con.setAutoCommit(false);
            if (level != mDefaultIsolationLevel) {
                con.setTransactionIsolation(mapIsolationLevelToJdbc(level));
            }

            synchronized (mAllTxnMgrs) {
                if (mOpenConnections == null) {
                    con.close();
                    throw new FetchException("Repository is closed");
                }
                mOpenConnections.put(con, null);
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
    // Note: This method must be public for auto-generated code to access it.
    public void yieldConnection(Connection con) throws FetchException {
        try {
            if (con.getAutoCommit()) {
                synchronized (mAllTxnMgrs) {
                    if (mOpenConnections != null) {
                        mOpenConnections.remove(con);
                    }
                }
                // Close connection outside synchronized section since it may block.
                if (con.getTransactionIsolation() != mJdbcDefaultIsolationLevel) {
                    con.setTransactionIsolation(mJdbcDefaultIsolationLevel);
                }
                con.close();
            }

            // Connections which aren't auto-commit are in a transaction.
            // When transaction is finished, JDBCTransactionManager switches
            // connection back to auto-commit and calls yieldConnection.
        } catch (Exception e) {
            throw toFetchException(e);
        }
    }

    /**
     * Yields connection without attempting to restore isolation level. Ignores
     * any exceptions too.
     */
    private void forceYieldConnection(Connection con) {
        synchronized (mAllTxnMgrs) {
            if (mOpenConnections != null) {
                mOpenConnections.remove(con);
            }
        }
        // Close connection outside synchronized section since it may block.
        try {
            con.close();
        } catch (SQLException e) {
            // Don't care.
        }
    }

    boolean supportsSavepoints() {
        return mSupportsSavepoints;
    }

    boolean supportsSelectForUpdate() {
        return mSupportsSelectForUpdate;
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
}
