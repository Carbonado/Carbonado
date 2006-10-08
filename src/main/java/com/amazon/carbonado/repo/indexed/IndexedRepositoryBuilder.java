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

package com.amazon.carbonado.repo.indexed;

import java.util.Collection;

import java.util.concurrent.atomic.AtomicReference;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.spi.AbstractRepositoryBuilder;

/**
 * Repository builder for the indexed repository.
 * <p>
 * In addition to supporting the capabilities of the wrapped repository, the
 * following extra capabilities are supported:
 * <ul>
 * <li>{@link com.amazon.carbonado.capability.IndexInfoCapability IndexInfoCapability}
 * <li>{@link com.amazon.carbonado.capability.StorableInfoCapability StorableInfoCapability}
 * <li>{@link IndexEntryAccessCapability IndexEntryAccessCapability}
 * </ul>
 *
 * @author Brian S O'Neill
 */
public class IndexedRepositoryBuilder extends AbstractRepositoryBuilder {
    private String mName;
    private boolean mIsMaster = true;
    private RepositoryBuilder mRepoBuilder;

    public IndexedRepositoryBuilder() {
    }

    public Repository build(AtomicReference<Repository> rootRef) throws RepositoryException {
        assertReady();

        Repository wrapped;

        boolean originalOption = mRepoBuilder.isMaster();
        try {
            mRepoBuilder.setMaster(mIsMaster);
            for (TriggerFactory factory : getTriggerFactories()) {
                mRepoBuilder.addTriggerFactory(factory);
            }
            wrapped = mRepoBuilder.build(rootRef);
        } finally {
            mRepoBuilder.setMaster(originalOption);
        }

        if (wrapped instanceof IndexedRepository) {
            return wrapped;
        }

        Repository repo = new IndexedRepository(rootRef, getName(), wrapped);
        rootRef.set(repo);
        return repo;
    }

    public String getName() {
        String name = mName;
        if (name == null && mRepoBuilder != null) {
            name = mRepoBuilder.getName();
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
     * @return wrapped respository
     */
    public RepositoryBuilder getWrappedRepository() {
        return mRepoBuilder;
    }

    /**
     * Set the required wrapped respository, which must support the
     * {@link com.amazon.carbonado.capability.IndexInfoCapability IndexInfoCapability}.
     */
    public void setWrappedRepository(RepositoryBuilder repoBuilder) {
        mRepoBuilder = repoBuilder;
    }

    public void errorCheck(Collection<String> messages) throws ConfigurationException {
        super.errorCheck(messages);
        if (null == getWrappedRepository()) {
            messages.add("wrapped repository missing");
        }
    }
}
