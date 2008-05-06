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

import org.apache.commons.logging.Log;

import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.util.BelatedCreator;

/**
 * Generic one-shot Storage creator which supports late object creation. If
 * getting the Storage results in an exception or is taking too long, the
 * Storage produced instead is a bogus one. Many operations result in an
 * IllegalStateException. After retrying, if the real Storage is accessed, then
 * the bogus Storage turns into a wrapper to the real Storage.
 *
 * @author Brian S O'Neill
 * @see BelatedRepositoryCreator
 */
public class BelatedStorageCreator<S extends Storable>
    extends BelatedCreator<Storage<S>, SupportException>
{
    final Log mLog;
    final Repository mRepo;
    final Class<S> mStorableType;

    /**
     * @param log error reporting log
     * @param repo Repository to get Storage from
     * @param storableType type of Storable to get Storage for
     * @param minRetryDelayMillis minimum milliseconds to wait before retrying
     * to create object after failure; if negative, never retry
     */
    public BelatedStorageCreator(Log log, Repository repo, Class<S> storableType,
                                 int minRetryDelayMillis) {
        // Nice double cast hack, eh?
        super((Class<Storage<S>>) ((Class) Storage.class), minRetryDelayMillis);
        mLog = log;
        mRepo = repo;
        mStorableType = storableType;
    }

    @Override
    protected Storage<S> createReal() throws SupportException {
        Exception error;
        try {
            return mRepo.storageFor(mStorableType);
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
        mLog.error("Error getting Storage of type \"" + mStorableType.getName() + '"', error);
        return null;
    }

    @Override
    protected Storage<S> createBogus() {
        return new BogusStorage();
    }

    @Override
    protected void timedOutNotification(long timedOutMillis) {
        mLog.error("Timed out waiting to get Storage of type \"" + mStorableType.getName() +
                   "\" after waiting " + timedOutMillis + " milliseconds");
    }

    private class BogusStorage implements Storage<S> {
        public Class<S> getStorableType() {
            return mStorableType;
        }

        public S prepare() {
            throw error();
        }

        public Query<S> query() {
            throw error();
        }

        public Query<S> query(String filter) {
            throw error();
        }

        public Query<S> query(Filter<S> filter) {
            throw error();
        }

        public Repository getRepository() {
            return mRepo;
        }

        public void truncate() {
            throw error();
        }

        public boolean addTrigger(Trigger<? super S> trigger) {
            throw error();
        }

        public boolean removeTrigger(Trigger<? super S> trigger) {
            throw error();
        }

        private IllegalStateException error() {
            return new IllegalStateException
                ("Creation of Storage for type \"" + mStorableType.getName() + "\" is delayed");
        }
    }
}
