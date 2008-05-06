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

package com.amazon.carbonado.repo.logging;

import java.util.Collection;

import java.util.concurrent.atomic.AtomicReference;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.spi.AbstractRepositoryBuilder;

/**
 * Repository implementation which logs activity against it. By default, all
 * logged messages are at the debug level.
 *
 * <p>
 * The following extra capabilities are supported:
 * <ul>
 * <li>{@link LogAccessCapability}
 * </ul>
 *
 * Example:
 *
 * <pre>
 * LoggingRepositoryBuilder loggingBuilder = new LoggingRepositoryBuilder();
 * loggingBuilder.setActualRepositoryBuilder(...);
 * Repository repo = loggingBuilder.build();
 * </pre>
 *
 * @author Brian S O'Neill
 */
public class LoggingRepositoryBuilder extends AbstractRepositoryBuilder {
    private String mName;
    private Boolean mMaster;
    private Log mLog;
    private RepositoryBuilder mRepoBuilder;

    public LoggingRepositoryBuilder() {
    }

    public Repository build(AtomicReference<Repository> rootRef) throws RepositoryException {
        if (mName == null) {
            if (mRepoBuilder != null) {
                mName = mRepoBuilder.getName();
            }
        }

        assertReady();

        if (mLog == null) {
            mLog = new CommonsLog(LoggingRepository.class);
        }

        String originalName = mRepoBuilder.getName();
        boolean originalIsMaster = mRepoBuilder.isMaster();

        boolean enabled = mLog.isEnabled();
        boolean master = mMaster != null ? mMaster : originalIsMaster;

        Repository actual;
        try {
            if (enabled) {
                mRepoBuilder.setName("Logging " + mName);
            }
            mRepoBuilder.setMaster(master);
            for (TriggerFactory factory : getTriggerFactories()) {
                mRepoBuilder.addTriggerFactory(factory);
            }
            actual = mRepoBuilder.build(rootRef);
        } finally {
            mRepoBuilder.setName(originalName);
            mRepoBuilder.setMaster(originalIsMaster);
        }

        if (!enabled) {
            return actual;
        }

        Repository repo = new LoggingRepository(rootRef, actual, mLog);
        rootRef.set(repo);
        return repo;
    }

    public void setName(String name) {
        mName = name;
    }

    public String getName() {
        return mName;
    }

    public void setMaster(boolean master) {
        mMaster = master;
    }

    public boolean isMaster() {
        return mMaster != null ? mMaster
            : (mRepoBuilder != null ? mRepoBuilder.isMaster() : false);
    }

    /**
     * Set the Log to use. If null, use default. Log must be enabled when build
     * is called, or else no logging is ever performed.
     */
    public void setLog(Log log) {
        mLog = log;
    }

    /**
     * Return the Log to use. If null, use default.
     */
    public Log getLog() {
        return mLog;
    }

    /**
     * Set the Repository to wrap all calls to.
     */
    public void setActualRepositoryBuilder(RepositoryBuilder builder) {
        mRepoBuilder = builder;
    }

    /**
     * Returns the Repository that all calls are wrapped to.
     */
    public RepositoryBuilder getActualRepositoryBuilder() {
        return mRepoBuilder;
    }

    @Override
    public void errorCheck(Collection<String> messages) throws ConfigurationException {
        super.errorCheck(messages);
        if (mRepoBuilder == null) {
            messages.add("Actual repository builder must be set");
        }
    }
}
