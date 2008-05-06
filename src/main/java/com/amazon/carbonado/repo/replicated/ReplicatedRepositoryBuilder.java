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
package com.amazon.carbonado.repo.replicated;

import java.util.Collection;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.spi.AbstractRepositoryBuilder;
import com.amazon.carbonado.spi.BelatedRepositoryCreator;

/**
 * Repository builder for the replicated repository.
 * <p>
 * The following extra capabilities are supported:
 * <ul>
 * <li>{@link com.amazon.carbonado.capability.ResyncCapability ResyncCapability}
 * </ul>
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public class ReplicatedRepositoryBuilder extends AbstractRepositoryBuilder {
    static final int DEFAULT_MASTER_TIMEOUT_MILLIS = 15000;
    static final int DEFAULT_RETRY_MILLIS = 30000;

    private String mName;
    private boolean mIsMaster = true;
    private RepositoryBuilder mReplicaRepositoryBuilder;
    private RepositoryBuilder mMasterRepositoryBuilder;

    public ReplicatedRepositoryBuilder() {
    }

    public Repository build(AtomicReference<Repository> rootRef) throws RepositoryException {
        assertReady();

        Repository replica, master;

        {
            boolean originalOption = mReplicaRepositoryBuilder.isMaster();
            try {
                mReplicaRepositoryBuilder.setMaster(false);
                for (TriggerFactory factory : getTriggerFactories()) {
                    mReplicaRepositoryBuilder.addTriggerFactory(factory);
                }
                replica = mReplicaRepositoryBuilder.build(rootRef);
            } finally {
                mReplicaRepositoryBuilder.setMaster(originalOption);
            }
        }

        {
            // Create master using BelatedRepositoryCreator such that we can
            // start up and read from replica even if master is down.

            final boolean originalOption = mMasterRepositoryBuilder.isMaster();
            mMasterRepositoryBuilder.setMaster(mIsMaster);

            Log log = LogFactory.getLog(ReplicatedRepositoryBuilder.class);
            BelatedRepositoryCreator creator = new BelatedRepositoryCreator
                (log, mMasterRepositoryBuilder, rootRef, DEFAULT_RETRY_MILLIS) {

                @Override
                protected void createdNotification(Repository repo) {
                    // Don't need builder any more so restore it.
                    mMasterRepositoryBuilder.setMaster(originalOption);
                }
            };

            master = creator.get(DEFAULT_MASTER_TIMEOUT_MILLIS);
        }

        Repository repo = new ReplicatedRepository(getName(), replica, master);
        rootRef.set(repo);
        return repo;
    }

    public String getName() {
        String name = mName;
        if (name == null) {
            if (mReplicaRepositoryBuilder != null && mReplicaRepositoryBuilder.getName() != null) {
                name = mReplicaRepositoryBuilder.getName();
            } else if (mMasterRepositoryBuilder != null) {
                name = mMasterRepositoryBuilder.getName();
            }
        }
        return name;
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
     * @return "replica" respository to replicate to.
     */
    public RepositoryBuilder getReplicaRepositoryBuilder() {
        return mReplicaRepositoryBuilder;
    }

    /**
     * Set "replica" respository to replicate to, which is required. This builder
     * automatically sets the master option of the given repository builder to
     * false.
     */
    public void setReplicaRepositoryBuilder(RepositoryBuilder replicaRepositoryBuilder) {
        mReplicaRepositoryBuilder = replicaRepositoryBuilder;
    }

    /**
     * @return "master" respository to replicate from.
     */
    public RepositoryBuilder getMasterRepositoryBuilder() {
        return mMasterRepositoryBuilder;
    }

    /**
     * Set "master" respository to replicate from, which is required. This
     * builder automatically sets the master option of the given repository to
     * true.
     */
    public void setMasterRepositoryBuilder(RepositoryBuilder masterRepositoryBuilder) {
        mMasterRepositoryBuilder = masterRepositoryBuilder;
    }

    @Override
    public void errorCheck(Collection<String> messages) throws ConfigurationException {
        super.errorCheck(messages);
        if (null == getReplicaRepositoryBuilder()) {
            messages.add("replicaRepositoryBuilder missing");
        }
        if (null == getMasterRepositoryBuilder()) {
            messages.add("masterRepositoryBuilder missing");
        }
    }
}
