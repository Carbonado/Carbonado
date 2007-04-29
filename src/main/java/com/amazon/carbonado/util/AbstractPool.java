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

package com.amazon.carbonado.util;

import java.util.Collection;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 * A concurrent pool of strongly referenced values mapped by key. Values are
 * lazily created and pooled.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public abstract class AbstractPool<K, V, E extends Exception> {
    private final ConcurrentMap<K, V> mValues;
    private final WeakReentrantLockPool<K> mLockPool;

    protected AbstractPool() {
        mValues = new ConcurrentHashMap<K, V>();
        mLockPool = new WeakReentrantLockPool<K>();
    }

    /**
     * Returns a value for the given key, which is lazily created and
     * pooled. If multiple threads are requesting upon the same key
     * concurrently, at most one thread attempts to lazily create the
     * value. The others wait for it to become available.
     */
    public V get(K key) throws E {
        // Quick check without locking.
        V value = mValues.get(key);
        if (value != null) {
            return value;
        }

        // Check again with key lock held.
        Lock lock = mLockPool.get(key);
        lock.lock();
        try {
            value = mValues.get(key);
            if (value == null) {
                try {
                    value = create(key);
                    mValues.put(key, value);
                } catch (Exception e) {
                    // Workaround compiler bug.
                    ThrowUnchecked.fire(e);
                }
            }
        } finally {
            lock.unlock();
        }

        return value;
    }

    /**
     * Remove a value, returning the old value.
     */
    public V remove(Object key) {
        return mValues.remove(key);
    }

    /**
     * Returns the pool values, which may be concurrently modified.
     */
    public Collection<V> values() {
        return mValues.values();
    }

    /**
     * Return a new value instance.
     */
    protected abstract V create(K key) throws E;
}
