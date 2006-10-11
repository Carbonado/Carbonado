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

package com.amazon.carbonado.repo.sleepycat;

import java.lang.reflect.Constructor;

import java.io.File;
import java.io.IOException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.atomic.AtomicReference;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.repo.indexed.IndexedRepositoryBuilder;

import com.amazon.carbonado.raw.GenericStorableCodecFactory;
import com.amazon.carbonado.raw.StorableCodecFactory;

import com.amazon.carbonado.spi.AbstractRepositoryBuilder;

import com.amazon.carbonado.util.ThrowUnchecked;

import com.amazon.carbonado.ConfigurationException;

/**
 * Builder and configuration options for BDBRepository.
 *
 * <pre>
 * BDBRepositoryBuilder builder = new BDBRepositoryBuilder();
 *
 * builder.setProduct("JE");
 * builder.setName("test");
 * builder.setEnvironmentHome("/tmp/testRepo");
 * builder.setTransactionNoSync(true);
 *
 * Repository repo = builder.build();
 * </pre>
 *
 * <p>
 * The following extra capabilities are supported:
 * <ul>
 * <li>{@link com.amazon.carbonado.capability.IndexInfoCapability IndexInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.StorableInfoCapability StorableInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.ShutdownCapability ShutdownCapability}
 * <li>{@link com.amazon.carbonado.layout.LayoutCapability LayoutCapability}
 * <li>{@link CheckpointCapability CheckpointCapability}
 * <li>{@link EnvironmentCapability EnvironmentCapability}
 * </ul>
 *
 * @author Brian S O'Neill
 * @author Vidya Iyer
 * @author Nicole Deflaux
 */
public class BDBRepositoryBuilder extends AbstractRepositoryBuilder {

    private static final BDBProduct DEFAULT_PRODUCT = BDBProduct.JE;

    private static final int DEFAULT_CHECKPOINT_INTERVAL = 10000;

    private String mName;
    private boolean mIsMaster = true;
    private BDBProduct mProduct = DEFAULT_PRODUCT;
    private File mEnvHome;
    private String mMergeSortTempDir;
    private File mDataHome;
    private String mSingleFileName;
    private boolean mIndexSupport = true;
    private boolean mReadOnly;
    private Long mCacheSize;
    private double mLockTimeout = 0.5;
    private double mTxnTimeout = 300.0;
    private boolean mTxnNoSync;
    private Boolean mDatabasesTransactional = null;
    private Map<Class<?>, Integer> mDatabasePageSizes;
    private boolean mPrivate;
    private boolean mMultiversion;
    private boolean mRunCheckpointer = true;
    private int mCheckpointInterval = DEFAULT_CHECKPOINT_INTERVAL;
    private boolean mRunDeadlockDetector = true;
    private Object mInitialEnvConfig = null;
    private Object mInitialDBConfig = null;
    private StorableCodecFactory mStorableCodecFactory = new GenericStorableCodecFactory();
    private Runnable mPreShutdownHook;
    private Runnable mPostShutdownHook;
    private DatabaseHook mDatabaseHook;

    public BDBRepositoryBuilder() {
    }

    public Repository build(AtomicReference<Repository> rootRef) throws RepositoryException {
        if (mIndexSupport) {
            // Wrap BDBRepository with IndexedRepository.

            // Temporarily set to false to avoid infinite recursion.
            mIndexSupport = false;
            try {
                IndexedRepositoryBuilder ixBuilder = new IndexedRepositoryBuilder();
                ixBuilder.setWrappedRepository(this);
                ixBuilder.setMaster(isMaster());
                return ixBuilder.build(rootRef);
            } finally {
                mIndexSupport = true;
            }
        }

        assertReady();

        // Make environment directory if it doesn't exist.
        File homeFile = getEnvironmentHomeFile();
        if (!homeFile.exists()) {
            if (!homeFile.mkdirs()) {
                throw new RepositoryException
                    ("Unable to make environment home directory: " + homeFile);
            }
        }

        BDBRepository repo;

        try {
            repo = getRepositoryConstructor().newInstance(rootRef, this);
        } catch (Exception e) {
            ThrowUnchecked.fireFirstDeclaredCause(e, RepositoryException.class);
            // Not reached.
            return null;
        }

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
     * Sets the BDB product to use, which defaults to JE. Also supported is DB
     * and DB_HA. If not supported, an IllegalArgumentException is thrown.
     */
    public void setProduct(String product) {
        mProduct = product == null ? DEFAULT_PRODUCT : BDBProduct.forString(product);
    }

    /**
     * Returns the BDB product to use, which is JE by default.
     */
    public String getProduct() {
        return mProduct.toString();
    }

    /**
     * Sets the BDB product to use, which defaults to JE.
     */
    public void setBDBProduct(BDBProduct product) {
        mProduct = product == null ? DEFAULT_PRODUCT : product;
    }

    /**
     * Returns the BDB product to use, which is JE by default.
     */
    public BDBProduct getBDBProduct() {
        return mProduct;
    }

    /**
     * Sets the repository environment home directory, which is required.
     */
    public void setEnvironmentHomeFile(File envHome) {
        try {
            // Switch to canonical for more detailed error messages.
            envHome = envHome.getCanonicalFile();
        } catch (IOException e) {
        }
        mEnvHome = envHome;
    }

    /**
     * Returns the repository environment home directory.
     */
    public File getEnvironmentHomeFile() {
        return mEnvHome;
    }

    /**
     * Sets the repository environment home directory, which is required.
     *
     * @throws RepositoryException if environment home is not valid
     */
    public void setEnvironmentHome(String envHome) {
        setEnvironmentHomeFile(new File(envHome));
    }

    /**
     * Returns the repository environment home directory.
     */
    public String getEnvironmentHome() {
        return mEnvHome.getPath();
    }

    /**
     * By default, data files are stored relative to the environment home. Call
     * this method to override. For BDBRepositories that are log files only,
     * this configuration is ignored.
     */
    public void setDataHomeFile(File dir) {
        if (dir != null) {
            try {
                // Switch to canonical for more detailed error messages.
                dir = dir.getCanonicalFile();
            } catch (IOException e) {
            }
        }
        mDataHome = dir;
    }

    /**
     * Returns the optional directory to store data files. Returns null if data
     * files are expected to be relative to the environment home.
     */
    public File getDataHomeFile() {
        if (mDataHome == null) {
            return getEnvironmentHomeFile();
        }
        return mDataHome;
    }

    /**
     * By default, data files are stored relative to the environment home. Call
     * this method to override. For BDBRepositories that are log files only,
     * this configuration is ignored.
     */
    public void setDataHome(String dir) {
        if (dir == null) {
            mDataHome = null;
        } else {
            setDataHomeFile(new File(dir));
        }
    }

    /**
     * Returns the directory to store data files.
     */
    public String getDataHome() {
        return getDataHomeFile().getPath();
    }

    /**
     * Sets the directory to use for creating temporary files needed for merge
     * sorting. If null or not specified, the default temporary file directory is used.
     *
     * @param tempDir directory to store temp files for merge sorting, or null
     * for default
     */
    /* FIXME: use common config somehow, since indexed repo needs this too
    public void setMergeSortTempDirectory(String tempDir) {
        mMergeSortTempDir = tempDir;
    }
    */

    /**
     * Returns the directory to use for creating temporary files needed for
     * merge sorting. If null, the default temporary file directory is used.
     */
    /* FIXME: use common config somehow, since indexed repo needs this too
    public String getMergeSortTempDirectory() {
        return mMergeSortTempDir;
    }
    */

    /**
     * Specify that all BDB databases should reside in one file, except for log
     * files and caches. The filename is relative to the environment home,
     * unless data directories have been specified. For BDBRepositories that
     * are log files only, this configuration is ignored.
     *
     * <p>Note: When setting this option, the storable codec factory must also
     * be changed, since the default storable codec factory is unable to
     * distinguish storable types that reside in a single database file.
     */
    public void setSingleFileName(String filename) {
        mSingleFileName = filename;
    }

    /**
     * Returns the single file that all BDB databases should reside in.
     */
    public String getSingleFileName() {
        return mSingleFileName;
    }

    /**
     * By default, user specified indexes are supported. Pass false to disable
     * this, and no indexes will be built. Another consequence of this option
     * is that no unique constraint checks will be applied to alternate keys.
     */
    public void setIndexSupport(boolean indexSupport) {
        mIndexSupport = indexSupport;
    }

    /**
     * Returns true if indexes are supported, which is true by default.
     */
    public boolean getIndexSupport() {
        return mIndexSupport;
    }

    /**
     * Sets the repository to read-only mode. By default, repository is opened
     * for reads and writes.
     */
    public void setReadOnly(boolean readOnly) {
        mReadOnly = readOnly;
    }

    /**
     * Returns true if repository should be opened read-only.
     */
    public boolean getReadOnly() {
        return mReadOnly;
    }

    /**
     * Set the repository cache size, in bytes. Actual BDB implementation will
     * select a suitable default if this is not set.
     */
    public void setCacheSize(long cacheSize) {
        mCacheSize = cacheSize;
    }

    /**
     * Set the repository cache size, in bytes. Actual BDB implementation will
     * select a suitable default if this is not set.
     *
     * @param cacheSize cache size to use, or null for default
     */
    public void setCacheSize(Long cacheSize) {
        mCacheSize = cacheSize;
    }

    /**
     * Returns the repository cache size, or null if default should be
     * selected.
     */
    public Long getCacheSize() {
        return mCacheSize;
    }

    /**
     * Set the lock timeout, in seconds. Default value is 0.5 seconds.
     */
    public void setLockTimeout(double lockTimeout) {
        mLockTimeout = lockTimeout;
    }

    /**
     * Returns the lock timeout, in seconds.
     */
    public double getLockTimeout() {
        return mLockTimeout;
    }

    /**
     * Returns the lock timeout, in microseconds, limited to max long value.
     */
    public long getLockTimeoutInMicroseconds() {
        return inMicros(mLockTimeout);
    }

    /**
     * Set the transaction timeout, in seconds. Default value is 300 seconds.
     */
    public void setTransactionTimeout(double txnTimeout) {
        mTxnTimeout = txnTimeout;
    }

    /**
     * Returns the repository transaction timeout, in seconds.
     */
    public double getTransactionTimeout() {
        return mTxnTimeout;
    }

    /**
     * Returns the repository transaction timeout, in microseconds, limited to
     * max long value.
     */
    public long getTransactionTimeoutInMicroseconds() {
        return inMicros(mTxnTimeout);
    }

    /**
     * When true, commits are not forcibly flushed to disk. This improves
     * performance, but there is a chance of losing the most recent commits if
     * the machine crashes.
     */
    public void setTransactionNoSync(boolean noSync) {
        mTxnNoSync = noSync;
    }

    /**
     * Returns true if transactions are forcibly flushed to disk.
     */
    public boolean getTransactionNoSync() {
        return mTxnNoSync;
    }

    /**
     * When true, allows databases to be transactional. This setting affects
     * the databases, not the environment. If this is not explicitly set, the
     * environment getTransactional is used.
     */
    public void setDatabasesTransactional(Boolean transactional) {
        mDatabasesTransactional = transactional;
    }

    /**
     * Returns true if the databases are configured to be transactional,
     * false if configured to not be transactional, null if this override was never set
     */
    public Boolean getDatabasesTransactional() {
        return mDatabasesTransactional;
    }

    /**
     * Sets the desired page size for a given type. If not specified, the page
     * size applies to all types.
     */
    public void setDatabasePageSize(Integer bytes, Class<? extends Storable> type) {
        if (mDatabasePageSizes == null) {
            mDatabasePageSizes = new HashMap<Class<?>, Integer>();
        }
        mDatabasePageSizes.put(type, bytes);
    }

    Map<Class<?>, Integer> getDatabasePagesMap() {
        if (mDatabasePageSizes == null) {
            return null;
        }
        return new HashMap<Class<?>, Integer>(mDatabasePageSizes);
    }

    /**
     * When true, BDB environment cannot be shared by other processes, and
     * region files are not created. By default, environment is shared, if
     * supported.
     */
    public void setPrivate(boolean b) {
        mPrivate = b;
    }

    /**
     * Returns true if BDB environment is private. By default, environment is
     * shared, if supported.
     */
    public boolean isPrivate() {
        return mPrivate;
    }

    /**
     * Set true to enable multiversion concurrency control (MVCC) on BDB
     * environment. This enables snapshot isolation, and is it is not supported
     * by all BDB products and versions.
     */
    public void setMultiversion(boolean multiversion) {
        mMultiversion = multiversion;
    }

    /**
     * Returns false by default because multiversion concurrency control (MVCC)
     * is not enabled.
     */
    public boolean isMultiversion() {
        return mMultiversion;
    }

    /**
     * Disable automatic checkpointing of database if another process is
     * responsible for that. The false setting is implied for read-only
     * databases.
     */
    public void setRunCheckpointer(boolean runCheckpointer) {
        mRunCheckpointer = runCheckpointer;
    }

    /**
     * Returns true if checkpointer is run automatically.
     */
    public boolean getRunCheckpointer() {
        return mRunCheckpointer;
    }

    /**
     * Set the interval to run checkpoints. This setting is ignored if the
     * checkpointer is not configured to run.
     *
     * @param intervalMillis interval between checkpoints, in milliseconds
     */
    public void setCheckpointInterval(int intervalMillis) {
        mCheckpointInterval = intervalMillis;
    }

    /**
     * @return interval between checkpoints, in milliseconds
     */
    public int getCheckpointInterval() {
        return mCheckpointInterval;
    }

    /**
     * Disable automatic deadlock detection of database if another thread is
     * responsible for that.
     */
    public void setRunDeadlockDetector(boolean runDeadlockDetector) {
        mRunDeadlockDetector = runDeadlockDetector;
    }

    /**
     * Returns true if deadlock detector is configured to run.
     */
    public boolean getRunDeadlockDetector() {
        return mRunDeadlockDetector;
    }

    /**
     * Optionally set the BDB specific environment configuration to
     * use. The builder will verify that needed configuration values are set.
     */
    public void setInitialEnvironmentConfig(Object envConfig) {
        mInitialEnvConfig = envConfig;
    }

    /**
     * Returns the optional BDB specific environment configuration to use.
     */
    public Object getInitialEnvironmentConfig() {
        return mInitialEnvConfig;
    }

    /**
     * Optionally set the BDB specific database configuration to use
     * for all databases created. The storage will verify that needed
     * configuration values are set.
     */
    public void setInitialDatabaseConfig(Object dbConfig) {
        mInitialDBConfig = dbConfig;
    }

    /**
     * Returns the optional BDB specific database configuration to use
     * for all databases created.
     */
    public Object getInitialDatabaseConfig() {
        return mInitialDBConfig;
    }

    /**
     * Override the default storable codec factory.
     */
    public void setStorableCodecFactory(StorableCodecFactory factory) {
        mStorableCodecFactory = factory;
    }

    /**
     * Returns the storable codec factory used.
     */
    public StorableCodecFactory getStorableCodecFactory() {
        return mStorableCodecFactory;
    }

    /**
     * Sets a callback to be invoked before the repository has finished running
     * its own shutdown hooks. This method is also invoked when repository is
     * manually closed.
     */
    public void setPreShutdownHook(Runnable hook) {
        mPreShutdownHook = hook;
    }

    /**
     * Returns the custom shutdown hook that runs before the repository has
     * finished running its own shutdown hooks, or null if none.
     */
    public Runnable getPreShutdownHook() {
        return mPreShutdownHook;
    }

    /**
     * Sets a callback to be invoked after repository has finished running its
     * own shutdown hooks. This method is also invoked when repository is
     * manually closed.
     */
    public void setShutdownHook(Runnable hook) {
        mPostShutdownHook = hook;
    }

    /**
     * Returns the custom shutdown hook that runs after the repository has
     * finished running its own shutdown hooks, or null if none.
     */
    public Runnable getShutdownHook() {
        return mPostShutdownHook;
    }

    /**
     * Sets a hook to be called whenever a database is opened.
     */
    public void setDatabaseHook(DatabaseHook hook) {
        mDatabaseHook = hook;
    }

    /**
     * Returns the custom open database hook, or null if none.
     */
    public DatabaseHook getDatabaseHook() {
        return mDatabaseHook;
    }

    private long inMicros(double seconds) {
        if (seconds >= Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        if (seconds <= 0 || Double.isNaN(seconds)) {
            return 0L;
        }
        return (long) (seconds * 1000000);
    }

    public void errorCheck(Collection<String> messages) throws ConfigurationException {
        super.errorCheck(messages);

        checkClass: {
            Exception error;
            try {
                getRepositoryConstructor();
                break checkClass;
            } catch (ClassCastException e) {
                error = e;
            } catch (ClassNotFoundException e) {
                error = e;
            } catch (NoSuchMethodException e) {
                error = e;
            }
            messages.add("BDB product \"" + getProduct() + "\" not supported: " + error);
        }

        File envHome = getEnvironmentHomeFile();
        if (envHome == null) {
            messages.add("environmentHome missing");
        } else {
            if (envHome.exists() && !envHome.isDirectory()) {
                messages.add("environment home is not a directory: " + envHome);
            }
        }
    }

    /**
     * Looks up appropriate repository via reflection, whose name is derived
     * from the BDB product string.
     */
    @SuppressWarnings("unchecked")
    private Constructor<BDBRepository> getRepositoryConstructor()
        throws ClassCastException, ClassNotFoundException, NoSuchMethodException
    {
        String className = getClass().getPackage().getName() + '.' +
            getBDBProduct().name() + "_Repository";
        Class repoClass = Class.forName(className);
        if (BDBRepository.class.isAssignableFrom(repoClass)) {
            return repoClass.getDeclaredConstructor
                (AtomicReference.class, BDBRepositoryBuilder.class);
        }
        throw new ClassCastException("Not an instance of BDBRepository: " + repoClass.getName());
    }

    public static interface DatabaseHook {
        /**
         * Called right before database is opened.
         *
         * @param db reference to database or config - actual type depends on BDB
         * implementation.
         */
        void prepareForOpening(Object db) throws RepositoryException;
    }
}
