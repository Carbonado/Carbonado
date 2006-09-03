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

package com.amazon.carbonado.repo.toy;

import java.util.HashMap;
import java.util.Map;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.capability.Capability;

import com.amazon.carbonado.spi.SequenceValueGenerator;
import com.amazon.carbonado.spi.SequenceValueProducer;

/**
 *
 * @author Brian S O'Neill
 */
public class ToyRepository implements Repository {
    private final String mName;
    private final Map<Class, Storage> mStorages;
    private final Map<String, SequenceValueProducer> mSequences;

    public ToyRepository() {
        this("toy");
    }

    public ToyRepository(String name) {
        mName = name;
        mStorages = new HashMap<Class, Storage>();
        mSequences = new HashMap<String, SequenceValueProducer>();
    }

    public String getName() {
        return mName;
    }

    public <S extends Storable> Storage<S> storageFor(Class<S> type)
        throws SupportException, RepositoryException
    {
        synchronized (mStorages) {
            Storage<S> storage = (Storage<S>) mStorages.get(type);
            if (storage == null) {
                storage = new ToyStorage<S>(this, type);
                mStorages.put(type, storage);
            }
            return storage;
        }
    }

    public Transaction enterTransaction() {
        return new ToyTransaction();
    }

    public Transaction enterTransaction(IsolationLevel level) {
        return enterTransaction();
    }

    public Transaction enterTopTransaction(IsolationLevel level) {
        return enterTransaction(level);
    }

    public IsolationLevel getTransactionIsolationLevel() {
        return null;
    }

    public <C extends Capability> C getCapability(Class<C> capabilityType) {
        return null;
    }

    public void close() {
    }

    SequenceValueProducer getSequenceValueProducer(String name) throws RepositoryException {
        synchronized (mSequences) {
            SequenceValueProducer producer = mSequences.get(name);
            if (producer == null) {
                producer = new SequenceValueGenerator(this, name);
                mSequences.put(name, producer);
            }
            return producer;
        }
    }
}
