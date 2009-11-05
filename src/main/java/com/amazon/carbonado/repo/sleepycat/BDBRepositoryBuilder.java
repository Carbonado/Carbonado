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

import com.amazon.carbonado.raw.CompressionType;
import com.amazon.carbonado.raw.CompressedStorableCodecFactory;
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
 * builder.setTransactionWriteNoSync(true);
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
 * <li>{@link com.amazon.carbonado.sequence.SequenceCapability SequenceCapability}
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
    private File mDataHome;
    private String mSingleFileName;
    private Map<String, String> mFileNames;
    private boolean mIndexSupport = true;
    private boolean mIndexRepairEnabled = true;
    private double mIndexThrottle = 1.0;
    private boolean mReadOnly;
    private Long mCacheSize;
    private Integer mCachePercent;
    private double mLockTimeout = 0.5;
    private double mTxnTimeout = 300.0;
    private boolean mTxnNoSync;
    private boolean mTxnWriteNoSync;
    private Boolean mDatabasesTransactional = null;
    private Map<Class<?>, Integer> mDatabasePageSizes;
    private boolean mPrivate;
    private boolean mMultiversion;
    private boolean mLogInMemory;
    private Integer mLogFileMaxSize;
    private boolean mRunFullRecovery;
    private boolean mRunCheckpointer = true;
    private int mCheckpointInterval = DEFAULT_CHECKPOINT_INTERVAL;
    private int mCheckpointThresholdKB = 1024;
    private int mCheckpointThresholdMinutes = 5;
    private boolean mRunDeadlockDetector = true;
    private Boolean mChecksumEnabled;
    private Object mInitialEnvConfig = null;
    private Object mInitialDBConfig = null;
    private StorableCodecFactory mStorableCodecFactory;
    private Runnable mPreShutdownHook;
    private Runnable mPostShutdownHook;
    private DatabaseHook mDatabaseHook;
    private Map<String, CompressionType> mCompressionMap;

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
                ixBuilder.setIndexRepairEnabled(mIndexRepairEnabled);
                ixBuilder.setIndexRepairThrottle(mIndexThrottle);
                return ixBuilder.build(rootRef);
            } finally {
                mIndexSupport = true;
            }
        }

        if (mStorableCodecFactory == null) {
            mStorableCodecFactory = new CompressedStorableCodecFactory(mCompressionMap);
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
     * Specify that all BDB databases should reside in one file, except for log
     * files and caches. The filename is relative to the environment home,
     * unless data directories have been specified. For BDBRepositories that
     * are log files only, this configuration is ignored.
     *
     * <p>Note: When setting this option, the storable codec factory must also
     * be changed, since the default storable codec factory is unable to
     * distinguish storable types that reside in a single database file. Call
     * setFileName instead to use built-in BDB feature for supporting multiple
     * databases in one file.
     */
    public void setSingleFileName(String filename) {
        mSingleFileName = filename;
        mFileNames = null;
    }

    /**
     * Returns the single file that all BDB databases should reside in.
     */
    public String getSingleFileName() {
        return mSingleFileName;
    }

    /**
     * Specify the file that a BDB database should reside in, except for log
     * files and caches. The filename is relative to the environment home,
     * unless data directories have been specified. For BDBRepositories that
     * are log files only, this configuration is ignored.
     *
     * @param filename BDB database filename
     * @param typeName type to store in file; if null, the file is used by default
     * for all types
     */
    public void setFileName(String filename, String typeName) {
        mSingleFileName = null;
        if (mFileNames == null) {
            mFileNames = new HashMap<String, String>();
        }
        mFileNames.put(typeName, filename);
    }

    Map<String, String> getFileNameMap() {
        if (mFileNames == null) {
            return null;
        }
        return new HashMap<String, String>(mFileNames);
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
     * @see #setIndexRepairEnabled(boolean)
     *
     * @return true by default
     */
    public boolean isIndexRepairEnabled() {
        return mIndexRepairEnabled;
    }

    /**
     * By default, index repair is enabled. In this mode, the first time a
     * Storable type is used, new indexes are populated and old indexes are
     * removed. Until finished, access to the Storable is blocked.
     *
     * <p>When index repair is disabled, the Storable is immediately
     * available. This does have consequences, however. The set of indexes
     * available for queries is defined by the <i>intersection</i> of the old
     * and new index sets. The set of indexes that are kept up-to-date is
     * defined by the <i>union</i> of the old and new index sets.
     *
     * <p>While index repair is disabled, another process can safely repair the
     * indexes in the background. When it is complete, index repair can be
     * enabled for this repository too.
     */
    public void setIndexRepairEnabled(boolean enabled) {
        mIndexRepairEnabled = enabled;
    }

    /**
     * Returns the throttle parameter used when indexes are added, dropped or
     * bulk repaired. By default this value is 1.0, or maximum speed.
     */
    public double getIndexRepairThrottle() {
        return mIndexThrottle;
    }

    /**
     * Sets the throttle parameter used when indexes are added, dropped or bulk
     * repaired. By default this value is 1.0, or maximum speed.
     *
     * @param desiredSpeed 1.0 = perform work at full speed,
     * 0.5 = perform work at half speed, 0.0 = fully suspend work
     */
    public void setIndexRepairThrottle(double desiredSpeed) {
        mIndexThrottle = desiredSpeed;
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
     * Set the percent of JVM heap used by the repository cache. Actual
     * BDB implementation will select a suitable default if this is not
     * set. This is overridden by setting an explicit cacheSize.
     */
    public void setCachePercent(int cachePercent) {
        mCachePercent = cachePercent;
    }

    /**
     * Set the percent of JVM heap used by the repository cache. Actual
     * BDB implementation will select a suitable default if this is not
     * set. This is overridden by setting an explicit cacheSize.
     *
     * @param cachePercent percent of JVM heap to use, or null for default
     */
    public void setCachePercent(Integer cachePercent) {
        mCachePercent = cachePercent;
    }

    /**
     * Returns the percent of JVM heap used by the repository cache, or
     * null if default should be selected.
     */
    public Integer getCachePercent() {
        return mCachePercent;
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
     * When true, commits are not immediately written or flushed to disk. This
     * improves performance, but there is a chance of losing the most recent
     * commits if the process is killed or if the machine crashes.
     */
    public void setTransactionNoSync(boolean noSync) {
        mTxnNoSync = noSync;
    }

    /**
     * Returns true if transactions are not written or flushed to disk.
     */
    public boolean getTransactionNoSync() {
        return mTxnNoSync;
    }

    /**
     * When true, commits are written, but they are not flushed to disk. This
     * improves performance, but there is a chance of losing the most recent
     * commits if the machine crashes.
     */
    public void setTransactionWriteNoSync(boolean noSync) {
        mTxnWriteNoSync = noSync;
    }

    /**
     * Returns true if transactions are not flushed to disk.
     */
    public boolean getTransactionWriteNoSync() {
        return mTxnWriteNoSync;
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
     * Set true to store transaction logs in memory only instead of persistent
     * storage. For BDB products which are entirely log based, no records are
     * ever persisted.
     */
    public void setLogInMemory(boolean logInMemory) {
        mLogInMemory = logInMemory;
    }

    /**
     * Returns false by default, indicating that transaction logs are persisted.
     */
    public boolean getLogInMemory() {
        return mLogInMemory;
    }

    /**
     * Set the maximum transaction log file size for the BDB environment.
     */
    public void setLogFileMaxSize(Integer sizeInBytes) {
        mLogFileMaxSize = sizeInBytes;
    }

    /**
     * Returns null if default size will be used.
     */
    public Integer getLogFileMaxSize() {
        return mLogFileMaxSize;
    }

    /**
     * Pass true to override the default and run a full (catastrophic) recovery
     * when environment is opened. This setting has no effect for BDB-JE.
     */
    public void setRunFullRecovery(boolean runRecovery) {
        mRunFullRecovery = runRecovery;
    }

    /**
     * Returns true if a full (catastrophic) recovery should be performed when
     * environment is opened.
     */
    public boolean getRunFullRecovery() {
        return mRunFullRecovery;
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
     * Set the size threshold to run checkpoints. This setting is ignored if
     * the checkpointer is not configured to run. Default value is 1024 KB.
     *
     * <p>Checkpoint threshold is only used by Carbonado's built-in
     * checkpointer, and is ignored when using BDB-JE.
     *
     * @param thresholdKB run checkpoint if at least this many kilobytes in log
     */
    public void setCheckpointThresholdKB(int thresholdKB) {
        mCheckpointThresholdKB = thresholdKB;
    }

    /**
     * @return run checkpoint if at least this many kilobytes in log
     */
    public int getCheckpointThresholdKB() {
        return mCheckpointThresholdKB;
    }

    /**
     * Set the time threshold to run checkpoints. This setting is ignored if
     * the checkpointer is not configured to run. Default value is 5 minutes.
     *
     * <p>Checkpoint threshold is only used by Carbonado's built-in
     * checkpointer, and is ignored when using BDB-JE.
     *
     * @param thresholdMinutes run checkpoint if at least this many minutes
     * passed since last checkpoint
     */
    public void setCheckpointThresholdMinutes(int thresholdMinutes) {
        mCheckpointThresholdMinutes = thresholdMinutes;
    }

    /**
     * @return run checkpoint if at least this many minutes passed since last
     * checkpoint
     */
    public int getCheckpointThresholdMinutes() {
        return mCheckpointThresholdMinutes;
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
     * When true, enable checksum verification of pages read into the cache
     * from the backing filestore. By default checksum is enabled for BDB-JE,
     * and disabled for BDB-C.
     */
    public void setChecksumEnabled(Boolean checksumEnabled) {
        mChecksumEnabled = checksumEnabled;
    }

    /**
     * Returns true if checksum verification is enabled. Returns null if the
     * BDB default is used.
     */
    public Boolean getChecksumEnabled() {
        return mChecksumEnabled;
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

    /**
     * Set the compressor for the given class, overriding a custom StorableCodecFactory.

     * @param type Storable to compress. 
     * @param compressionType String representation of type of
     * compression. Available options are "NONE" for no compression or "GZIP"
     * for gzip compression
     */
    public void setCompressor(String type, String compressionType) {
        mStorableCodecFactory = null;
        compressionType = compressionType.toUpperCase();
        if (mCompressionMap == null) {
            mCompressionMap = new HashMap<String, CompressionType>();
        }
        CompressionType compressionEnum = CompressionType.valueOf(compressionType);
        if (compressionEnum != null) {
            mCompressionMap.put(type, compressionEnum);
        }
    }

    /**
     * Return the compressor used for the given storable.
     * @param type Storable to compress
     * @return String representation of the type of compression used. Available options are "NONE"
     * for no compression and "GZIP" for gzip compression.
     */
    public String getCompressor(String type) {
        if (mCompressionMap == null) {
            return null;
        }

        return mCompressionMap.get(type).toString();
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

    @Override
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
         * Returns an appropriate database name for the given type. Simply
         * return the type name as-is to support default behavior.
         */
        String databaseName(String typeName);

        /**
         * Called right before database is opened.
         *
         * @param db reference to database or config - actual type depends on BDB
         * implementation.
         */
        void prepareForOpening(Object db) throws RepositoryException;
    }
}
