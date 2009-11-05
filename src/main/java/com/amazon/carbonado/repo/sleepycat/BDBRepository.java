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

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.MalformedArgumentException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.capability.Capability;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.capability.ShutdownCapability;
import com.amazon.carbonado.capability.StorableInfoCapability;

import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutCapability;
import com.amazon.carbonado.layout.LayoutFactory;

import com.amazon.carbonado.qe.RepositoryAccess;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.raw.StorableCodecFactory;

import com.amazon.carbonado.sequence.SequenceCapability;
import com.amazon.carbonado.sequence.SequenceValueGenerator;
import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.spi.AbstractRepository;
import com.amazon.carbonado.spi.ExceptionTransformer;
import com.amazon.carbonado.spi.LobEngine;

import com.amazon.carbonado.txn.TransactionManager;
import com.amazon.carbonado.txn.TransactionScope;

/**
 * Repository implementation backed by a Berkeley DB. Data is encoded in the
 * BDB in a specialized format, and so this repository should not be used to
 * open arbitrary Berkeley databases. BDBRepository has total schema ownership,
 * and so it updates type definitions in the storage layer automatically.
 *
 * @author Brian S O'Neill
 * @author Vidya Iyer
 * @author Nicole Deflaux
 * @author bcastill
 */
abstract class BDBRepository<Txn> extends AbstractRepository<Txn>
    implements Repository,
               RepositoryAccess,
               IndexInfoCapability,
               HotBackupCapability,
               CheckpointCapability,
               EnvironmentCapability,
               ShutdownCapability,
               StorableInfoCapability,
               SequenceCapability,
               LayoutCapability
{
    private final Log mLog = LogFactory.getLog(getClass());

    private final boolean mIsMaster;
    final Iterable<TriggerFactory> mTriggerFactories;
    private final AtomicReference<Repository> mRootRef;
    private final StorableCodecFactory mStorableCodecFactory;
    private final ExceptionTransformer mExTransformer;
    private final BDBTransactionManager<Txn> mTxnMgr;

    Checkpointer mCheckpointer;
    DeadlockDetector mDeadlockDetector;

    private final Runnable mPreShutdownHook;
    private final Runnable mPostShutdownHook;

    private final Object mInitialDBConfig;
    private final BDBRepositoryBuilder.DatabaseHook mDatabaseHook;
    private final Map<Class<?>, Integer> mDatabasePageSizes;

    final boolean mRunCheckpointer;
    final boolean mRunDeadlockDetector;

    final File mDataHome;
    final File mEnvHome;
    final String mSingleFileName;
    final Map<String, String> mFileNameMap;

    final Object mBackupLock = new Object();
    int mBackupCount = 0;

    private LayoutFactory mLayoutFactory;

    private LobEngine mLobEngine;

    /**
     * Subclass must call protected start method to fully initialize
     * BDBRepository.
     *
     * @param builder repository configuration
     * @param exTransformer transformer for exceptions
     * @throws IllegalArgumentException if name or environment home is null
     */
    @SuppressWarnings("unchecked")
    BDBRepository(AtomicReference<Repository> rootRef,
                  BDBRepositoryBuilder builder,
                  ExceptionTransformer exTransformer)
        throws ConfigurationException
    {
        super(builder.getName());

        builder.assertReady();

        if (exTransformer == null) {
            throw new IllegalArgumentException("Exception transformer must not be null");
        }

        mIsMaster = builder.isMaster();
        mTriggerFactories = builder.getTriggerFactories();
        mRootRef = rootRef;
        mExTransformer = exTransformer;
        mTxnMgr = new BDBTransactionManager<Txn>(mExTransformer, this);

        mRunCheckpointer = !builder.getReadOnly() && builder.getRunCheckpointer();
        mRunDeadlockDetector = builder.getRunDeadlockDetector();
        mStorableCodecFactory = builder.getStorableCodecFactory();
        mPreShutdownHook = builder.getPreShutdownHook();
        mPostShutdownHook = builder.getShutdownHook();
        mInitialDBConfig = builder.getInitialDatabaseConfig();
        mDatabaseHook = builder.getDatabaseHook();
        mDatabasePageSizes = builder.getDatabasePagesMap();
        mDataHome = builder.getDataHomeFile();
        mEnvHome = builder.getEnvironmentHomeFile();
        mSingleFileName = builder.getSingleFileName();
        mFileNameMap = builder.getFileNameMap();

        getLog().info("Opening repository \"" + getName() + '"');
    }

    public <S extends Storable> IndexInfo[] getIndexInfo(Class<S> storableType)
        throws RepositoryException
    {
        return ((BDBStorage) storageFor(storableType)).getIndexInfo();
    }

    public String[] getUserStorableTypeNames() throws RepositoryException {
        Repository metaRepo = getRootRepository();

        Cursor<StoredDatabaseInfo> cursor =
            metaRepo.storageFor(StoredDatabaseInfo.class)
            .query().orderBy("databaseName").fetch();

        try {
            ArrayList<String> names = new ArrayList<String>();
            while (cursor.hasNext()) {
                StoredDatabaseInfo info = cursor.next();
                // Ordinary user types support evolution.
                if (info.getEvolutionStrategy() != StoredDatabaseInfo.EVOLUTION_NONE) {
                    names.add(info.getDatabaseName());
                }
            }

            return names.toArray(new String[names.size()]);
        } finally {
            cursor.close();
        }
    }

    public boolean isSupported(Class<Storable> type) {
        if (type == null) {
            return false;
        }
        StorableIntrospector.examine(type);
        return true;
    }

    public boolean isPropertySupported(Class<Storable> type, String name) {
        if (type == null || name == null) {
            return false;
        }
        return StorableIntrospector.examine(type).getAllProperties().get(name) != null;
    }

    @Override
    public Backup startBackup() throws RepositoryException {
        synchronized (mBackupLock) {
            int count = mBackupCount;
            if (count == 0) {
                try {
                    enterBackupMode();
                } catch (Exception e) {
                    throw mExTransformer.toRepositoryException(e);
                }
            }
            mBackupCount = count + 1;

            return new Backup() {
                private boolean mDone;

                @Override
                public void endBackup() throws RepositoryException {
                    synchronized (mBackupLock) {
                        if (mDone) {
                            return;
                        }
                        mDone = true;

                        int count = mBackupCount - 1;
                        try {
                            if (count == 0) {
                                try {
                                    exitBackupMode();
                                } catch (Exception e) {
                                    throw mExTransformer.toRepositoryException(e);
                                }
                            }
                        } finally {
                            mBackupCount = count;
                        }
                    }
                }

                @Override
                public File[] getFiles() throws RepositoryException {
                    synchronized (mBackupLock) {
                        if (mDone) {
                            throw new IllegalStateException("Backup has ended");
                        }

                        try {
                            return backupFiles();
                        } catch (Exception e) {
                            throw mExTransformer.toRepositoryException(e);
                        }
                    }
                }
            };
        }
    }

    /**
     * Suspend the checkpointer until the suspension time has expired or until
     * manually resumed. If a checkpoint is in progress, this method will block
     * until it is finished. If checkpointing is disabled, calling this method
     * has no effect.
     *
     * <p>Calling this method repeatedly resets the suspension time. This
     * technique should be used by hot backup processes to ensure that its
     * failure does not leave the checkpointer permanently suspended. Each
     * invocation of suspendCheckpointer is like a lease renewal or heartbeat.
     *
     * @param suspensionTime minimum length of suspension, in milliseconds,
     * unless checkpointer is manually resumed
     */
    public void suspendCheckpointer(long suspensionTime) {
        if (mCheckpointer != null) {
            mCheckpointer.suspendCheckpointer(suspensionTime);
        }
    }

    /**
     * Resumes the checkpointer if it was suspended. If checkpointing is
     * disabled or if not suspended, calling this method has no effect.
     */
    public void resumeCheckpointer() {
        if (mCheckpointer != null) {
            mCheckpointer.resumeCheckpointer();
        }
    }

    /**
     * Forces a checkpoint to run now, even if checkpointer is suspended or
     * disabled. If a checkpoint is in progress, then this method will block
     * until it is finished, and then run another checkpoint. This method does
     * not return until the requested checkpoint has finished.
     */
    public void forceCheckpoint() throws PersistException {
        if (mCheckpointer != null) {
            mCheckpointer.forceCheckpoint();
        } else {
            try {
                env_checkpoint();
            } catch (Exception e) {
                throw toPersistException(e);
            }
        }
    }

    public Repository getRootRepository() {
        return mRootRef.get();
    }

    public <S extends Storable> StorageAccess<S> storageAccessFor(Class<S> type)
        throws RepositoryException
    {
        return (BDBStorage<Txn, S>) storageFor(type);
    }

    @Override
    public Layout layoutFor(Class<? extends Storable> type)
        throws FetchException, PersistException
    {
        try {
            return ((BDBStorage) storageFor(type)).getLayout(mStorableCodecFactory);
        } catch (PersistException e) {
            throw e;
        } catch (RepositoryException e) {
            throw e.toFetchException();
        }
    }

    @Override
    public Layout layoutFor(Class<? extends Storable> type, int generation)
        throws FetchException
    {
        return mLayoutFactory.layoutFor(type, generation);
    }

    @Override
    protected void finalize() {
        close();
    }

    @Override
    protected void shutdownHook() {
        // Run any external shutdown logic that needs to happen before the
        // databases and the environment are actually closed
        if (mPreShutdownHook != null) {
            mPreShutdownHook.run();
        }

        // Close database handles.
        for (Storage storage : allStorage()) {
            try {
                if (storage instanceof BDBStorage) {
                    ((BDBStorage) storage).close();
                }
            } catch (Throwable e) {
                getLog().error(null, e);
            }
        }

        // Wait for checkpointer to finish.
        if (mCheckpointer != null) {
            mCheckpointer.interrupt();
            try {
                mCheckpointer.join();
            } catch (InterruptedException e) {
            }
        }

        // Wait for deadlock detector to finish.
        if (mDeadlockDetector != null) {
            mDeadlockDetector.interrupt();
            try {
                mDeadlockDetector.join();
            } catch (InterruptedException e) {
            }
        }

        // Close environment.
        try {
            env_close();
        } catch (Throwable e) {
            getLog().error(null, e);
        }

        if (mPostShutdownHook != null) {
            mPostShutdownHook.run();
        }
    }

    @Override
    protected Log getLog() {
        return mLog;
    }

    @Override
    protected <S extends Storable> Storage createStorage(Class<S> type)
        throws RepositoryException
    {
        try {
            return createBDBStorage(type);
        } catch (MalformedArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw toRepositoryException(e);
        }
    }

    @Override
    protected SequenceValueProducer createSequenceValueProducer(String name)
        throws RepositoryException
    {
        return new SequenceValueGenerator(BDBRepository.this, name);
    }

    /**
     * @see com.amazon.carbonado.spi.RepositoryBuilder#isMaster
     */
    boolean isMaster() {
        return mIsMaster;
    }

    String[] getAllDatabaseNames() throws RepositoryException {
        Repository metaRepo = getRootRepository();

        Cursor<StoredDatabaseInfo> cursor =
            metaRepo.storageFor(StoredDatabaseInfo.class)
            .query().orderBy("databaseName").fetch();

        ArrayList<String> names = new ArrayList<String>();
        // This one needs to manually added since it is the metadata db itself.
        names.add(StoredDatabaseInfo.class.getName());

        try {
            while (cursor.hasNext()) {
                names.add(cursor.next().getDatabaseName());
            }
        } finally {
            cursor.close();
        }

        return names.toArray(new String[names.size()]);
    }

    String getDatabaseFileName(final String dbName) {
        String singleFileName = mSingleFileName;
        if (singleFileName == null && mFileNameMap != null) {
            singleFileName = mFileNameMap.get(dbName);
            if (singleFileName == null && dbName != null) {
                singleFileName = mFileNameMap.get(null);
            }
        }

        String dbFileName = dbName;

        if (singleFileName == null) {
            if (mDatabaseHook != null) {
                dbFileName = mDatabaseHook.databaseName(dbName);
            }
        } else {
            dbFileName = singleFileName;
        }

        if (mDataHome != null && !mDataHome.equals(mEnvHome)) {
            dbFileName = new File(mDataHome, dbFileName).getPath();
        }

        return dbFileName;
    }

    /**
     * Returns null if name should not be used.
     */
    String getDatabaseName(String dbName) {
        if (mFileNameMap == null) {
            return null;
        }
        String name = mFileNameMap.get(dbName);
        if (name == null && dbName != null) {
            name = mFileNameMap.get(null);
        }
        if (name == null) {
            return null;
        }
        if (mDatabaseHook != null) {
            try {
                dbName = mDatabaseHook.databaseName(dbName);
            } catch (IncompatibleClassChangeError e) {
                // Method not implemented.
            }
        }
        return dbName;
    }

    StorableCodecFactory getStorableCodecFactory() {
        return mStorableCodecFactory;
    }

    LayoutFactory getLayoutFactory() throws RepositoryException {
        if (mLayoutFactory == null) {
            mLayoutFactory = new LayoutFactory(getRootRepository());
        }
        return mLayoutFactory;
    }

    LobEngine getLobEngine() throws RepositoryException {
        if (mLobEngine == null) {
            mLobEngine = new LobEngine(this, getRootRepository());
        }
        return mLobEngine;
    }

    /**
     * Returns the optional BDB specific database configuration to use
     * for all databases created.
     */
    public Object getInitialDatabaseConfig() {
        return mInitialDBConfig;
    }

    /**
     * Returns the desired page size for the given type, or null for default.
     */
    Integer getDatabasePageSize(Class<? extends Storable> type) {
        if (mDatabasePageSizes == null) {
            return null;
        }
        Integer size = mDatabasePageSizes.get(type);
        if (size == null && type != null) {
            size = mDatabasePageSizes.get(null);
        }
        return size;
    }

    void runDatabasePrepareForOpeningHook(Object database) throws RepositoryException {
        if (mDatabaseHook != null) {
            mDatabaseHook.prepareForOpening(database);
        }
    }

    /**
     * Start background tasks and enable auto shutdown.
     *
     * @param checkpointInterval how often to run checkpoints, in milliseconds,
     * or zero if never. Ignored if repository is read only or builder has
     * checkpoints disabled.
     * @param deadlockDetectorInterval how often to run deadlock detector, in
     * milliseconds, or zero if never. Ignored if builder has deadlock detector
     * disabled.
     *
     * @deprecated Overloaded for backwards compatiblity with older
     * CarbonadoSleepycat packages
     */
    void start(long checkpointInterval, long deadlockDetectorInterval) {
        getLog().info("Opened repository \"" + getName() + '"');

        if (mRunCheckpointer && checkpointInterval > 0) {
            mCheckpointer = new Checkpointer(this, checkpointInterval, 1024, 5);
            mCheckpointer.start();
        } else {
            mCheckpointer = null;
        }

        if (mRunDeadlockDetector && deadlockDetectorInterval > 0) {
            mDeadlockDetector = new DeadlockDetector(this, deadlockDetectorInterval);
            mDeadlockDetector.start();
        } else {
            mDeadlockDetector = null;
        }

        setAutoShutdownEnabled(true);
    }

    /**
     * Start background tasks and enable auto shutdown.
     *
     * @param checkpointInterval how often to run checkpoints, in milliseconds,
     * or zero if never. Ignored if repository is read only or builder has
     * checkpoints disabled.
     * @param deadlockDetectorInterval how often to run deadlock detector, in
     * milliseconds, or zero if never. Ignored if builder has deadlock detector
     * disabled.
     * @param builder containing additonal background task properties.
     */
    void start(long checkpointInterval, long deadlockDetectorInterval,
               BDBRepositoryBuilder builder) {
        getLog().info("Opened repository \"" + getName() + '"');

        if (mRunCheckpointer && checkpointInterval > 0) {
            mCheckpointer = new Checkpointer(this, checkpointInterval,
                                             builder.getCheckpointThresholdKB(),
                                             builder.getCheckpointThresholdMinutes());
            mCheckpointer.start();
        } else {
            mCheckpointer = null;
        }

        if (mRunDeadlockDetector && deadlockDetectorInterval > 0) {
            mDeadlockDetector = new DeadlockDetector(this, deadlockDetectorInterval);
            mDeadlockDetector.start();
        } else {
            mDeadlockDetector = null;
        }

        setAutoShutdownEnabled(true);
    }

    abstract IsolationLevel selectIsolationLevel(Transaction parent, IsolationLevel level);

    abstract Txn txn_begin(Txn parent, IsolationLevel level) throws Exception;

    // Subclass should override this method to actually apply the timeout
    Txn txn_begin(Txn parent, IsolationLevel level, int timeout, TimeUnit unit) throws Exception {
        return txn_begin(parent, level);
    }

    abstract Txn txn_begin_nowait(Txn parent, IsolationLevel level) throws Exception;

    abstract void txn_commit(Txn txn) throws Exception;

    abstract void txn_abort(Txn txn) throws Exception;

    /**
     * Force a checkpoint to run.
     */
    abstract void env_checkpoint() throws Exception;

    /**
     * @param kBytes run checkpoint if at least this many kilobytes in log
     * @param minutes run checkpoint if at least this many minutes passed since
     * last checkpoint
     */
    abstract void env_checkpoint(int kBytes, int minutes) throws Exception;

    /**
     * Run the deadlock detector.
     */
    abstract void env_detectDeadlocks() throws Exception;

    /**
     * Close the environment.
     */
    abstract void env_close() throws Exception;

    abstract <S extends Storable> BDBStorage<Txn, S> createBDBStorage(Class<S> type)
        throws Exception;

    /**
     * Called only the first time a backup is started.
     */
    abstract void enterBackupMode() throws Exception;

    /**
     * Called only after the last backup ends.
     */
    abstract void exitBackupMode() throws Exception;

    /**
     * Called only if in backup mode.
     */
    abstract File[] backupFiles() throws Exception;

    FetchException toFetchException(Throwable e) {
        return mExTransformer.toFetchException(e);
    }

    PersistException toPersistException(Throwable e) {
        return mExTransformer.toPersistException(e);
    }

    RepositoryException toRepositoryException(Throwable e) {
        return mExTransformer.toRepositoryException(e);
    }

    @Override
    protected final TransactionManager<Txn> transactionManager() {
        return mTxnMgr;
    }

    @Override
    protected final TransactionScope<Txn> localTransactionScope() {
        return mTxnMgr.localScope();
    }

    /**
     * Periodically runs checkpoints on the environment.
     */
    private static class Checkpointer extends Thread {
        private final WeakReference<BDBRepository> mRepository;
        private final long mSleepInterval;
        private final int mKBytes;
        private final int mMinutes;

        private boolean mInProgress;
        private long mSuspendUntil = Long.MIN_VALUE;

        /**
         *
         * @param repository outer class
         * @param sleepInterval milliseconds to sleep before running checkpoint
         * @param kBytes run checkpoint if at least this many kilobytes in log
         * @param minutes run checkpoint if at least this many minutes passed
         * since last checkpoint
         */
        Checkpointer(BDBRepository repository, long sleepInterval, int kBytes, int minutes) {
            super(repository.getClass().getSimpleName() + " checkpointer (" +
                  repository.getName() + ')');
            setDaemon(true);
            mRepository = new WeakReference<BDBRepository>(repository);
            mSleepInterval = sleepInterval;
            mKBytes = kBytes;
            mMinutes = minutes;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    synchronized (this) {
                        if (!mInProgress) {
                            try {
                                wait(mSleepInterval);
                            } catch (InterruptedException e) {
                                break;
                            }
                        }
                    }

                    BDBRepository repository = mRepository.get();
                    if (repository == null) {
                        break;
                    }

                    long suspendUntil;
                    synchronized (this) {
                        suspendUntil = mSuspendUntil;
                    }
                    if (suspendUntil != Long.MIN_VALUE) {
                        if (System.currentTimeMillis() < suspendUntil) {
                            continue;
                        }
                    }

                    Log log = repository.getLog();

                    if (log.isDebugEnabled()) {
                        log.debug("Running checkpoint on repository \"" +
                                  repository.getName() + '"');
                    }

                    try {
                        synchronized (this) {
                            mInProgress = true;
                        }
                        repository.env_checkpoint(mKBytes, mMinutes);
                        if (log.isDebugEnabled()) {
                            log.debug("Finished running checkpoint on repository \"" +
                                      repository.getName() + '"');
                        }
                    } catch (ThreadDeath e) {
                        break;
                    } catch (Throwable e) {
                        log.error("Checkpoint failed", e);
                    } finally {
                        synchronized (this) {
                            mInProgress = false;
                            // Only wait condition is mInProgress, so okay to not call notifyAll.
                            notify();
                        }
                        repository = null;
                    }
                }
            } finally {
                synchronized (this) {
                    mInProgress = false;
                    // Only wait condition is mInProgress, so okay to not call notifyAll.
                    notify();
                }
            }
        }

        /**
         * Blocks until checkpoint has finished.
         */
        synchronized void suspendCheckpointer(long suspensionTime) {
            while (mInProgress) {
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }

            if (suspensionTime <= 0) {
                return;
            }

            long now = System.currentTimeMillis();
            long suspendUntil = now + suspensionTime;
            if (now >= 0 && suspendUntil < 0) {
                // Overflow.
                suspendUntil = Long.MAX_VALUE;
            }
            mSuspendUntil = suspendUntil;
        }

        synchronized void resumeCheckpointer() {
            mSuspendUntil = Long.MIN_VALUE;
        }

        /**
         * Blocks until checkpoint has finished.
         */
        synchronized void forceCheckpoint() throws PersistException {
            while (mInProgress) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    return;
                }
            }

            BDBRepository repository = mRepository.get();
            if (repository != null) {
                try {
                    repository.env_checkpoint();
                } catch (Exception e) {
                    throw repository.toPersistException(e);
                }
            }
        }
    }

    /**
     * Periodically runs deadlock detection on the environment.
     */
    private static class DeadlockDetector extends Thread {
        private final WeakReference<BDBRepository> mRepository;
        private final long mSleepInterval;

        /**
         * @param repository outer class
         * @param sleepInterval milliseconds to sleep before running deadlock detection
         */
        DeadlockDetector(BDBRepository repository, long sleepInterval) {
            super(repository.getClass().getSimpleName() + " deadlock detector (" +
                  repository.getName() + ')');
            setDaemon(true);
            mRepository = new WeakReference<BDBRepository>(repository);
            mSleepInterval = sleepInterval;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(mSleepInterval);
                } catch (InterruptedException e) {
                    break;
                }

                BDBRepository repository = mRepository.get();
                if (repository == null) {
                    break;
                }

                try {
                    repository.env_detectDeadlocks();
                } catch (ThreadDeath e) {
                    break;
                } catch (Throwable e) {
                    repository.getLog().error("Deadlock detection failed", e);
                } finally {
                    repository = null;
                }
            }
        }
    }
}
