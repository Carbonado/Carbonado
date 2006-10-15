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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.MalformedTypeException;
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

import com.amazon.carbonado.layout.LayoutCapability;
import com.amazon.carbonado.layout.LayoutFactory;

import com.amazon.carbonado.qe.RepositoryAccess;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.raw.StorableCodecFactory;

import com.amazon.carbonado.spi.ExceptionTransformer;
import com.amazon.carbonado.spi.LobEngine;
import com.amazon.carbonado.spi.SequenceValueGenerator;
import com.amazon.carbonado.spi.SequenceValueProducer;
import com.amazon.carbonado.spi.StorageCollection;

/**
 * Repository implementation backed by a Berkeley DB. Data is encoded in the
 * BDB in a specialized format, and so this repository should not be used to
 * open arbitrary Berkeley databases. BDBRepository has total schema ownership,
 * and so it updates type definitions in the storage layer automatically.
 *
 * @author Brian S O'Neill
 * @author Vidya Iyer
 * @author Nicole Deflaux
 */
abstract class BDBRepository<Txn>
    implements Repository,
               RepositoryAccess,
               IndexInfoCapability,
               CheckpointCapability,
               EnvironmentCapability,
               ShutdownCapability,
               StorableInfoCapability
{
    private final Log mLog = LogFactory.getLog(getClass());

    private final String mName;
    private final boolean mIsMaster;
    final Iterable<TriggerFactory> mTriggerFactories;
    private final AtomicReference<Repository> mRootRef;
    private final StorableCodecFactory mStorableCodecFactory;
    private final ExceptionTransformer mExTransformer;
    private final StorageCollection mStorages;
    private final Map<String, SequenceValueGenerator> mSequences;
    private final ThreadLocal<BDBTransactionManager<Txn>> mCurrentTxnMgr;

    private final Lock mShutdownLock;
    private final Condition mShutdownCondition;
    private int mShutdownBlockerCount;

    // Weakly tracks all BDBTransactionManager instances for shutdown hook.
    private final Map<BDBTransactionManager<Txn>, ?> mAllTxnMgrs;

    Checkpointer mCheckpointer;
    DeadlockDetector mDeadlockDetector;

    private ShutdownHook mShutdownHook;
    final Runnable mPreShutdownHook;
    final Runnable mPostShutdownHook;
    volatile boolean mHasShutdown;

    private final Object mInitialDBConfig;
    private final BDBRepositoryBuilder.DatabaseHook mDatabaseHook;
    private final Map<Class<?>, Integer> mDatabasePageSizes;

    final boolean mRunCheckpointer;
    final boolean mRunDeadlockDetector;

    final File mDataHome;
    final File mEnvHome;
    final String mSingleFileName;

    private final String mMergeSortTempDir;

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
        builder.assertReady();

        if (exTransformer == null) {
            throw new IllegalArgumentException("Exception transformer must not be null");
        }

        mName = builder.getName();
        mIsMaster = builder.isMaster();
        mTriggerFactories = builder.getTriggerFactories();
        mRootRef = rootRef;
        mExTransformer = exTransformer;

        mStorages = new StorageCollection() {
            protected <S extends Storable> Storage<S> createStorage(Class<S> type)
                throws RepositoryException
            {
                lockoutShutdown();
                try {
                    try {
                        return BDBRepository.this.createStorage(type);
                    } catch (Exception e) {
                        throw toRepositoryException(e);
                    }
                } finally {
                    unlockoutShutdown();
                }
            }
        };

        mSequences = new ConcurrentHashMap<String, SequenceValueGenerator>();
        mCurrentTxnMgr = new ThreadLocal<BDBTransactionManager<Txn>>();
        mShutdownLock = new ReentrantLock();
        mShutdownCondition = mShutdownLock.newCondition();
        mAllTxnMgrs = new WeakIdentityMap();
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
        // FIXME: see comments in builder
        mMergeSortTempDir = null; //builder.getMergeSortTempDirectory();
    }

    public String getName() {
        return mName;
    }

    public <S extends Storable> BDBStorage<Txn, S> storageFor(Class<S> type)
        throws MalformedTypeException, RepositoryException
    {
        return (BDBStorage<Txn, S>) mStorages.storageFor(type);
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

    @SuppressWarnings("unchecked")
    public <C extends Capability> C getCapability(Class<C> capabilityType) {
        if (capabilityType.isInstance(this)) {
            return (C) this;
        }
        if (capabilityType == LayoutCapability.class) {
            return (C) mLayoutFactory;
        }
        return null;
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

        ArrayList<String> names = new ArrayList<String>();
        while (cursor.hasNext()) {
            StoredDatabaseInfo info = cursor.next();
            // Ordinary user types support evolution.
            if (info.getEvolutionStrategy() != StoredDatabaseInfo.EVOLUTION_NONE) {
                names.add(info.getDatabaseName());
            }
        }

        return names.toArray(new String[names.size()]);
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

    public void close() {
        shutdown(false);
    }

    public boolean isAutoShutdownEnabled() {
        return mShutdownHook != null;
    }

    public void setAutoShutdownEnabled(boolean enabled) {
        if (mShutdownHook == null) {
            if (enabled) {
                mShutdownHook = new ShutdownHook(this);
                try {
                    Runtime.getRuntime().addShutdownHook(mShutdownHook);
                } catch (IllegalStateException e) {
                    // Shutdown in progress, so immediately run hook.
                    mShutdownHook.run();
                }
            }
        } else {
            if (!enabled) {
                try {
                    Runtime.getRuntime().removeShutdownHook(mShutdownHook);
                } catch (IllegalStateException e) {
                    // Shutdown in progress, hook is running.
                }
                mShutdownHook = null;
            }
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    private void shutdown(boolean suspendThreads) {
        if (!mHasShutdown) {
            // Since this repository is being closed before system shutdown,
            // remove shutdown hook and run it now.
            ShutdownHook hook = mShutdownHook;
            if (hook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(hook);
                } catch (IllegalStateException e) {
                    // Shutdown in progress, hook is running.
                    hook = null;
                }
            } else {
                // If hook is null, auto-shutdown was disabled. Make a new
                // instance to use, but don't register it.
                hook = new ShutdownHook(this);
            }
            if (hook != null) {
                hook.run(suspendThreads);
            }
            mHasShutdown = true;
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
        return storageFor(type);
    }

    @Override
    protected void finalize() {
        close();
    }

    /**
     * @see com.amazon.carbonado.spi.RepositoryBuilder#isMaster
     */
    boolean isMaster() {
        return mIsMaster;
    }

    String getDatabaseFileName(String dbName) {
        if (mSingleFileName != null) {
            dbName = mSingleFileName;
        }

        if (mDataHome != null && !mDataHome.equals(mEnvHome)) {
            dbName = new File(mDataHome, dbName).getPath();
        }

        return dbName;
    }

    String getMergeSortTempDirectory() {
        if (mMergeSortTempDir != null) {
            new File(mMergeSortTempDir).mkdirs();
        }
        return mMergeSortTempDir;
    }

    SequenceValueProducer getSequenceValueProducer(String name) throws PersistException {
        SequenceValueGenerator producer = mSequences.get(name);
        if (producer == null) {
            lockoutShutdown();
            try {
                producer = mSequences.get(name);
                if (producer == null) {
                    Repository metaRepo = getRootRepository();
                    try {
                        producer = new SequenceValueGenerator(metaRepo, name);
                    } catch (RepositoryException e) {
                        throw toPersistException(e);
                    }
                    mSequences.put(name, producer);
                }
                return producer;
            } finally {
                unlockoutShutdown();
            }
        }
        return producer;
    }

    Log getLog() {
        return mLog;
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
        LobEngine engine = mLobEngine;
        if (engine == null) {
            lockoutShutdown();
            try {
                if ((engine = mLobEngine) == null) {
                    mLobEngine = engine = new LobEngine(this);
                }
                return engine;
            } finally {
                unlockoutShutdown();
            }
        }
        return engine;
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
     * @param checkpointInterval how often to run checkpoints, in milliseconds,
     * or zero if never. Ignored if builder has checkpoints disabled.
     * @param deadlockDetectorInterval how often to run deadlock detector, in
     * milliseconds, or zero if never.
     */
    void start(long checkpointInterval, long deadlockDetectorInterval) {
        getLog().info("Opening repository \"" + getName() + '"');

        if (mRunCheckpointer && checkpointInterval > 0) {
            mCheckpointer = new Checkpointer(this, checkpointInterval);
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

    abstract String getVersionMajor();

    abstract String getVersionMajorMinor();

    abstract String getVersionMajorMinorPatch();

    abstract IsolationLevel selectIsolationLevel(Transaction parent, IsolationLevel level);

    abstract Txn txn_begin(Txn parent, IsolationLevel level) throws Exception;

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

    abstract <S extends Storable> BDBStorage<Txn, S> createStorage(Class<S> type)
        throws Exception;

    FetchException toFetchException(Throwable e) {
        return mExTransformer.toFetchException(e);
    }

    PersistException toPersistException(Throwable e) {
        return mExTransformer.toPersistException(e);
    }

    RepositoryException toRepositoryException(Throwable e) {
        return mExTransformer.toRepositoryException(e);
    }

    /**
     * Returns the thread-local BDBTransactionManager instance, creating it if
     * needed.
     */
    BDBTransactionManager<Txn> openTransactionManager() {
        BDBTransactionManager<Txn> txnMgr = mCurrentTxnMgr.get();
        if (txnMgr == null) {
            lockoutShutdown();
            try {
                txnMgr = new BDBTransactionManager<Txn>(mExTransformer, this);
                mCurrentTxnMgr.set(txnMgr);
                mAllTxnMgrs.put(txnMgr, null);
            } finally {
                unlockoutShutdown();
            }
        }
        return txnMgr;
    }

    /**
     * Call to prevent shutdown hook from running. Be sure to call
     * unlockoutShutdown afterwards.
     */
    private void lockoutShutdown() {
        mShutdownLock.lock();
        try {
            mShutdownBlockerCount++;
        } finally {
            mShutdownLock.unlock();
        }
    }

    /**
     * Only call this to release lockoutShutdown.
     */
    private void unlockoutShutdown() {
        mShutdownLock.lock();
        try {
            if (--mShutdownBlockerCount == 0) {
                mShutdownCondition.signalAll();
            }
        } finally {
            mShutdownLock.unlock();
        }
    }

    /**
     * Only to be called by shutdown hook itself.
     */
    void lockForShutdown() {
        mShutdownLock.lock();
        while (mShutdownBlockerCount > 0) {
            try {
                mShutdownCondition.await();
            } catch (InterruptedException e) {
                mLog.warn("Ignoring interruption for shutdown");
            }
        }
    }

    /**
     * Only to be called by shutdown hook itself.
     */
    void unlockForShutdown() {
        mShutdownLock.unlock();
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
         */
        Checkpointer(BDBRepository repository, long sleepInterval) {
            this(repository, sleepInterval, 1024, 5);
        }

        /**
         *
         * @param repository outer class
         * @param sleepInterval milliseconds to sleep before running checkpoint
         * @param kBytes run checkpoint if at least this many kilobytes in log
         * @param minutes run checkpoint if at least this many minutes passed
         * since last checkpoint
         */
        Checkpointer(BDBRepository repository, long sleepInterval, int kBytes, int minutes) {
            super("BDBRepository checkpointer (" + repository.getName() + ')');
            setDaemon(true);
            mRepository = new WeakReference<BDBRepository>(repository);
            mSleepInterval = sleepInterval;
            mKBytes = kBytes;
            mMinutes = minutes;
        }

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

                    if (mSuspendUntil != Long.MIN_VALUE) {
                        if (System.currentTimeMillis() < mSuspendUntil) {
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
                            notify();
                        }
                        repository = null;
                    }
                }
            } finally {
                synchronized (this) {
                    mInProgress = false;
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
            super("BDBRepository deadlock detector (" + repository.getName() + ')');
            setDaemon(true);
            mRepository = new WeakReference<BDBRepository>(repository);
            mSleepInterval = sleepInterval;
        }

        public void run() {
            while (true) {
                synchronized (this) {
                    try {
                        wait(mSleepInterval);
                    } catch (InterruptedException e) {
                        break;
                    }
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

    private static class ShutdownHook extends Thread {
        private final WeakReference<BDBRepository<?>> mRepository;

        ShutdownHook(BDBRepository repository) {
            super("BDBRepository shutdown (" + repository.getName() + ')');
            mRepository = new WeakReference<BDBRepository<?>>(repository);
        }

        public void run() {
            run(true);
        }

        public void run(boolean suspendThreads) {
            BDBRepository<?> repository = mRepository.get();
            if (repository == null) {
                return;
            }

            repository.getLog().info("Closing repository \"" + repository.getName() + '"');

            try {
                doShutdown(repository, suspendThreads);
            } finally {
                repository.mHasShutdown = true;
                mRepository.clear();
                repository.getLog().info
                    ("Finished closing repository \"" + repository.getName() + '"');
            }
        }

        private void doShutdown(BDBRepository<?> repository, boolean suspendThreads) {
            repository.lockForShutdown();
            try {
                // Return unused sequence values.
                for (SequenceValueGenerator generator : repository.mSequences.values()) {
                    try {
                        generator.returnReservedValues();
                    } catch (RepositoryException e) {
                        repository.getLog().warn(null, e);
                    }
                }

                // Close transactions and cursors.
                for (BDBTransactionManager<?> txnMgr : repository.mAllTxnMgrs.keySet()) {
                    if (suspendThreads) {
                        // Lock transaction manager but don't release it. This
                        // prevents other threads from beginning work during
                        // shutdown, which will likely fail along the way.
                        txnMgr.getLock().lock();
                    }
                    try {
                        txnMgr.close();
                    } catch (Throwable e) {
                        repository.getLog().error(null, e);
                    }
                }

                // Run any external shutdown logic that needs to
                // happen before the databases and the environment are
                // actually closed
                if (repository.mPreShutdownHook != null) {
                    repository.mPreShutdownHook.run();
                }

                // Close database handles.
                for (Storage storage : repository.mStorages.allStorage()) {
                    try {
                        ((BDBStorage) storage).close();
                    } catch (Throwable e) {
                        repository.getLog().error(null, e);
                    }
                }

                // Wait for checkpointer to finish.
                if (repository.mCheckpointer != null) {
                    repository.mCheckpointer.interrupt();
                    try {
                        repository.mCheckpointer.join();
                    } catch (InterruptedException e) {
                    }
                }

                // Wait for deadlock detector to finish.
                if (repository.mDeadlockDetector != null) {
                    repository.mDeadlockDetector.interrupt();
                    try {
                        repository.mDeadlockDetector.join();
                    } catch (InterruptedException e) {
                    }
                }

                // Close environment.
                try {
                    repository.env_close();
                } catch (Throwable e) {
                    repository.getLog().error(null, e);
                }

                if (repository.mPostShutdownHook != null) {
                    repository.mPostShutdownHook.run();
                }
            } finally {
                repository.unlockForShutdown();
            }
        }
    }
}
