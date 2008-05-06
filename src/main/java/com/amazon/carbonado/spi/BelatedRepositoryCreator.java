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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.capability.Capability;

import com.amazon.carbonado.util.BelatedCreator;

/**
 * Generic one-shot Repository builder which supports late object creation. If
 * the Repository building results in an exception or is taking too long, the
 * Repository produced instead is a bogus one. Many operations result in an
 * IllegalStateException. After retrying, if the real Repository is created,
 * then the bogus Repository turns into a wrapper to the real Repository.
 *
 * @author Brian S O'Neill
 * @see BelatedStorageCreator
 */
public class BelatedRepositoryCreator extends BelatedCreator<Repository, SupportException> {
    final Log mLog;
    final RepositoryBuilder mBuilder;
    final AtomicReference<Repository> mRootRef;

    /**
     * @param log error reporting log
     * @param builder builds real Repository
     * @param minRetryDelayMillis minimum milliseconds to wait before retrying
     * to create object after failure; if negative, never retry
     */
    public BelatedRepositoryCreator(Log log, RepositoryBuilder builder, int minRetryDelayMillis) {
        this(log, builder, new AtomicReference<Repository>(), minRetryDelayMillis);
    }

    /**
     * @param log error reporting log
     * @param builder builds real Repository
     * @param rootRef reference to root repository
     * @param minRetryDelayMillis minimum milliseconds to wait before retrying
     * to create object after failure; if negative, never retry
     */
    public BelatedRepositoryCreator(Log log,
                                    RepositoryBuilder builder,
                                    AtomicReference<Repository> rootRef,
                                    int minRetryDelayMillis)
    {
        super(Repository.class, minRetryDelayMillis);
        mLog = log;
        mBuilder = builder;
        mRootRef = rootRef;
    }

    @Override
    protected Repository createReal() throws SupportException {
        Exception error;
        try {
            return mBuilder.build(mRootRef);
        } catch (SupportException e) {
            // Cannot recover from this.
            throw e;
        } catch (RepositoryException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ClassNotFoundException) {
                // If a class cannot be loaded, then I don't expect this to be
                // a recoverable situation.
                throw new SupportException(cause);
            }
            error = e;
        } catch (Exception e) {
            error = e;
        }
        mLog.error("Error building Repository \"" + mBuilder.getName() + '"', error);
        return null;
    }

    @Override
    protected Repository createBogus() {
        return new BogusRepository();
    }

    @Override
    protected void timedOutNotification(long timedOutMillis) {
        mLog.error("Timed out waiting for Repository \"" + mBuilder.getName() +
                   "\" to build after waiting " + timedOutMillis +
                   " milliseconds");
    }

    private class BogusRepository implements Repository {
        public String getName() {
            return mBuilder.getName();
        }

        public synchronized <S extends Storable> Storage<S> storageFor(Class<S> type) {
            throw error();
        }

        public Transaction enterTransaction() {
            throw error();
        }

        public Transaction enterTransaction(IsolationLevel level) {
            throw error();
        }

        public Transaction enterTopTransaction(IsolationLevel level) {
            throw error();
        }

        public IsolationLevel getTransactionIsolationLevel() {
            return null;
        }

        public <C extends Capability> C getCapability(Class<C> capabilityType) {
            throw error();
        }

        public void close() {
        }

        private IllegalStateException error() {
            return new IllegalStateException
                ("Creation of Repository \"" + mBuilder.getName() + "\" is delayed");
        }
    }
}
