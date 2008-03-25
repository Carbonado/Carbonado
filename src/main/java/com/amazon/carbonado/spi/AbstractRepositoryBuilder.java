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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.TriggerFactory;

/**
 * Abstract builder class for opening repositories.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public abstract class AbstractRepositoryBuilder implements RepositoryBuilder {
    private final Set<TriggerFactory> mTriggerFactories;

    protected AbstractRepositoryBuilder() {
        mTriggerFactories = new LinkedHashSet<TriggerFactory>(2);
    }

    public Repository build() throws ConfigurationException, RepositoryException {
        return build(new AtomicReference<Repository>());
    }

    public boolean addTriggerFactory(TriggerFactory factory) {
        synchronized (mTriggerFactories) {
            return mTriggerFactories.add(factory);
        }
    }

    public boolean removeTriggerFactory(TriggerFactory factory) {
        synchronized (mTriggerFactories) {
            return mTriggerFactories.remove(factory);
        }
    }

    public Iterable<TriggerFactory> getTriggerFactories() {
        synchronized (mTriggerFactories) {
            if (mTriggerFactories.size() == 0) {
                return Collections.emptyList();
            } else {
                return new ArrayList<TriggerFactory>(mTriggerFactories);
            }
        }
    }

    /**
     * Throw a configuration exception if the configuration is not filled out
     * sufficiently and correctly such that a repository could be instantiated
     * from it.
     */
    public final void assertReady() throws ConfigurationException {
        ArrayList<String> messages = new ArrayList<String>();
        errorCheck(messages);
        int size = messages.size();
        if (size == 0) {
            return;
        }
        StringBuilder b = new StringBuilder();
        if (size > 1) {
            b.append("Multiple problems: ");
        }
        for (int i=0; i<size; i++) {
            if (i > 0) {
                b.append("; ");
            }
            b.append(messages.get(i));
        }
        throw new ConfigurationException(b.toString());
    }

    /**
     * This method is called by assertReady, and subclasses must override to
     * perform custom checks. Be sure to call {@code super.errorCheck} as well.
     *
     * @param messages add any error messages to this list
     * @throws ConfigurationException if error checking indirectly caused
     * another exception
     */
    public void errorCheck(Collection<String> messages) throws ConfigurationException {
        if (getName() == null) {
            messages.add("name missing");
        }
    }
}
