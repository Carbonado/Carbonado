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

package com.amazon.carbonado.spi;

import java.lang.ref.WeakReference;

import java.util.Collection;
import java.util.Map;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;

import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.capability.Capability;
import com.amazon.carbonado.capability.ShutdownCapability;

import com.amazon.carbonado.sequence.SequenceCapability;
import com.amazon.carbonado.sequence.SequenceValueProducer;
import com.amazon.carbonado.sequence.SequenceValueProducerPool;

/**
 * Implements basic functionality required by a core Repository.
 *
 * @param <Txn> Transaction type
 * @author Brian S O'Neill
 * @since 1.2
 */
public abstract class AbstractRepository<Txn>
    implements Repository, ShutdownCapability, SequenceCapability
{
    private final String mName;
    private final ReadWriteLock mShutdownLock;

    private final ThreadLocal<TransactionManager<Txn>> mCurrentTxnMgr;

    // Weakly tracks all TransactionManager instances for shutdown hook.
    private final Map<TransactionManager<Txn>, ?> mAllTxnMgrs;

    private final StoragePool mStoragePool;

    private final SequenceValueProducerPool mSequencePool;

    private ShutdownHook mShutdownHook;
    volatile boolean mHasShutdown;

    protected AbstractRepository(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Repository name cannot be null");
        }
        mName = name;
        mShutdownLock = new ReentrantReadWriteLock();
        mCurrentTxnMgr = new ThreadLocal<TransactionManager<Txn>>();
        mAllTxnMgrs = new WeakIdentityMap();

        mStoragePool = new StoragePool() {
            protected <S extends Storable> Storage<S> createStorage(Class<S> type)
                throws RepositoryException
            {
                AbstractRepository.this.lockoutShutdown();
                try {
                    return AbstractRepository.this.createStorage(type);
                } finally {
                    AbstractRepository.this.unlockoutShutdown();
                }
            }
        };

        mSequencePool = new SequenceValueProducerPool() {
            protected SequenceValueProducer createSequenceValueProducer(String name)
                throws RepositoryException
            {
                AbstractRepository.this.lockoutShutdown();
                try {
                    return AbstractRepository.this.createSequenceValueProducer(name);
                } finally {
                    AbstractRepository.this.unlockoutShutdown();
                }
            }
        };
    }

    public String getName() {
        return mName;
    }

    public <S extends Storable> Storage<S> storageFor(Class<S> type)
        throws SupportException, RepositoryException
    {
        return mStoragePool.get(type);
    }

    public Transaction enterTransaction() {
        return localTransactionManager().enter(null);
    }

    public Transaction enterTransaction(IsolationLevel level) {
        return localTransactionManager().enter(level);
    }

    public Transaction enterTopTransaction(IsolationLevel level) {
        return localTransactionManager().enterTop(level);
    }

    public IsolationLevel getTransactionIsolationLevel() {
        return localTransactionManager().getIsolationLevel();
    }

    /**
     * Default implementation checks if Repository implements Capability
     * interface, and if so, returns the Repository.
     */
    @SuppressWarnings("unchecked")
    public <C extends Capability> C getCapability(Class<C> capabilityType) {
        if (capabilityType.isInstance(this)) {
            return (C) this;
        }
        return null;
    }

    public void close() {
        shutdown(false);
    }

    // Required by ShutdownCapability.
    public boolean isAutoShutdownEnabled() {
        return mShutdownHook != null;
    }

    // Required by ShutdownCapability.
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

    // Required by ShutdownCapability.
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

    // Required by SequenceCapability.
    public SequenceValueProducer getSequenceValueProducer(String name) throws RepositoryException {
        return mSequencePool.get(name);
    }

    /**
     * Returns the thread-local TransactionManager, creating it if needed.
     */
    protected TransactionManager<Txn> localTransactionManager() {
        TransactionManager<Txn> txnMgr = mCurrentTxnMgr.get();
        if (txnMgr == null) {
            lockoutShutdown();
            try {
                txnMgr = createTransactionManager();
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
    protected void lockoutShutdown() {
        mShutdownLock.readLock().lock();
    }

    /**
     * Only call this to release lockoutShutdown.
     */
    protected void unlockoutShutdown() {
        mShutdownLock.readLock().unlock();
    }

    /**
     * Only to be called by shutdown hook itself.
     */
    void lockForShutdown() {
        mShutdownLock.writeLock().lock();
    }

    /**
     * Only to be called by shutdown hook itself.
     */
    void unlockForShutdown() {
        mShutdownLock.writeLock().unlock();
    }

    /**
     * Returns all available Storage instances.
     */
    protected Collection<Storage> allStorage() {
        return mStoragePool.values();
    }

    /**
     * Install custom shutdown logic by overriding this method. By default it
     * does nothing.
     */
    protected void shutdownHook() {
    }

    /**
     * Return the main Log object for this Repository. If none provided, then
     * no messages are logged by AbstractRepository.
     */
    protected abstract Log getLog();

    /**
     * Called upon to create a new thread-local TransactionManager instance.
     */
    protected abstract TransactionManager<Txn> createTransactionManager();

    /**
     * Called upon to create a new Storage instance.
     */
    protected abstract <S extends Storable> Storage<S> createStorage(Class<S> type)
        throws RepositoryException;

    /**
     * Called upon to create a new SequenceValueProducer instance.
     */
    protected abstract SequenceValueProducer createSequenceValueProducer(String name)
        throws RepositoryException;

    void info(String message) {
        Log log = getLog();
        if (log != null) {
            log.info(message);
        }
    }

    void error(String message, Throwable e) {
        Log log = getLog();
        if (log != null) {
            log.error(message, e);
        }
    }

    private static class ShutdownHook extends Thread {
        private final WeakReference<AbstractRepository<?>> mRepository;

        ShutdownHook(AbstractRepository repository) {
            super(repository.getClass().getSimpleName() + " shutdown (" +
                  repository.getName() + ')');
            mRepository = new WeakReference<AbstractRepository<?>>(repository);
        }

        public void run() {
            run(true);
        }

        public void run(boolean suspendThreads) {
            AbstractRepository<?> repository = mRepository.get();
            if (repository == null) {
                return;
            }

            repository.info("Closing repository \"" + repository.getName() + '"');

            try {
                doShutdown(repository, suspendThreads);
            } finally {
                repository.mHasShutdown = true;
                mRepository.clear();
                repository.info("Finished closing repository \"" + repository.getName() + '"');
            }
        }

        private void doShutdown(AbstractRepository<?> repository, boolean suspendThreads) {
            repository.lockForShutdown();
            try {
                // Return unused sequence values.
                repository.mSequencePool.returnReservedValues(null);

                // Close transactions and cursors.
                for (TransactionManager<?> txnMgr : repository.mAllTxnMgrs.keySet()) {
                    if (suspendThreads) {
                        // Lock transaction manager but don't release it. This
                        // prevents other threads from beginning work during
                        // shutdown, which will likely fail along the way.
                        txnMgr.getLock().lock();
                    }
                    try {
                        txnMgr.close();
                    } catch (Throwable e) {
                        repository.error("Failed to close TransactionManager", e);
                    }
                }

                repository.shutdownHook();
            } finally {
                repository.unlockForShutdown();
            }
        }
    }
}
