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

import java.util.IdentityHashMap;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.StorableIntrospector;

/**
 * Thread-safe container of Storage instances which creates Storage
 * on-demand. If multiple threads are requesting the same Storage concurrently,
 * only one thread actually creates the Storage. The other waits for it to
 * become available.
 *
 * @author Brian S O'Neill
 */
public abstract class StorageCollection {
    private final ConcurrentMap<Class<?>, Storage> mStorageMap;
    private final ConcurrentMap<Class<?>, Object> mStorableTypeLockMap;

    public StorageCollection() {
        mStorageMap = new ConcurrentHashMap<Class<?>, Storage>();
        mStorableTypeLockMap = new ConcurrentHashMap<Class<?>, Object>();
    }

    public <S extends Storable> Storage<S> storageFor(Class<S> type)
        throws MalformedTypeException, SupportException, RepositoryException
    {
        Storage storage = mStorageMap.get(type);
        if (storage != null) {
            return storage;
        }

        getLock: while (true) {
            Object lock;
            boolean doCreate;

            {
                Object newLock = new Object();
                Object existingLock = mStorableTypeLockMap.putIfAbsent(type, newLock);

                if (existingLock == null) {
                    lock = newLock;
                    doCreate = true;
                } else {
                    lock = existingLock;
                    doCreate = false;
                }
            }

            if (Thread.holdsLock(lock)) {
                throw new IllegalStateException
                    ("Recursively trying to create storage for type: " + type);
            }

            synchronized (lock) {
                try {
                    // Check storage map again before creating new storage.
                    checkLock: while (true) {
                        storage = mStorageMap.get(type);
                        if (storage != null) {
                            return storage;
                        }
                        if (mStorableTypeLockMap.get(type) != lock) {
                            // Lock has changed, so get a new lock.
                            continue getLock;
                        }
                        if (doCreate) {
                            break checkLock;
                        }
                        try {
                            // Wait with a timeout to handle rare race condition.
                            lock.wait(100);
                        } catch (InterruptedException e) {
                            throw new RepositoryException("Interrupted");
                        }
                    }

                    // Examine and throw exception early if there is a problem.
                    StorableIntrospector.examine(type);

                    storage = createStorage(type);

                    mStorageMap.put(type, storage);
                    break getLock;
                } finally {
                    // Storable type lock no longer needed.
                    mStorableTypeLockMap.remove(type, lock);
                    lock.notifyAll();
                }
            }
        }

        return storage;
    }

    public Iterable<Storage> allStorage() {
        return mStorageMap.values();
    }

    protected abstract <S extends Storable> Storage<S> createStorage(Class<S> type)
        throws SupportException, RepositoryException;
}
