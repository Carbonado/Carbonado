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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A concurrent pool of weakly referenced values mapped by key. Values are
 * created (and recreated) as needed.
 *
 * @author Brian S O'Neill
 * @see AbstractPool
 * @since 1.2
 */
abstract class AbstractWeakPool<K, V, E extends Exception> {
    private final ConcurrentMap<K, ValueRef<K, V>> mValues;
    private final ReferenceQueue<V> mValueRefQueue;

    protected AbstractWeakPool() {
        mValues = new ConcurrentHashMap<K, ValueRef<K, V>>();
        mValueRefQueue = new ReferenceQueue<V>();
    }

    /**
     * Returns a value for the given key. Unused values are automatically
     * cleared to free up memory, even if they are still held. Repeat calls are
     * guaranteed to return the same value instance only if the value is
     * strongly reachable. The following idiom should be used when using the
     * pool for maintaining locks:
     *
     * <pre>
     * // Store lock in local variable to be strongly reachable.
     * Lock lock = lockPool.get(key);
     * lock.lock();
     * try {
     *     // access the resource protected by this lock
     *     ...
     * } finally {
     *     lock.unlock();
     * }
     * </pre>
     */
    public V get(K key) throws E {
        clean();

        ValueRef<K, V> valueRef = mValues.get(key);
        V value;

        if (valueRef == null || (value = valueRef.get()) == null) {
            try {
                value = create(key);
            } catch (Exception e) {
                // Workaround compiler bug.
                ThrowUnchecked.fire(e);
                return null;
            }
            valueRef = new ValueRef<K, V>(value, mValueRefQueue, key);
            while (true) {
                ValueRef<K, V> existingRef = mValues.putIfAbsent(key, valueRef);
                if (existingRef == null) {
                    // Newly created value is now the official value.
                    break;
                }
                V existing = existingRef.get();
                if (existing != null) {
                    // Someone else just created value before us. Use that
                    // instead and chuck the new value object.
                    value = existing;
                    valueRef.clear();
                    break;
                }
                // Reference just got cleared. Try again. Explicitly remove it
                // to prevent an infinite loop. Note that the two argument
                // remove method is called to ensure that what is being removed
                // is not a new value.
                mValues.remove(((ValueRef<K, V>) existingRef).mKey, existingRef);
            }
        }

        return value;
    }

    /**
     * Manually remove a value, returning the old value.
     */
    public V remove(Object key) {
        clean();

        ValueRef<K, V> valueRef = mValues.remove(key);
        V value;

        if (valueRef != null && (value = valueRef.get()) != null) {
            valueRef.clear();
            return value;
        }

        return null;
    }

    /**
     * Return a new value instance.
     */
    protected abstract V create(K key) throws E;

    private void clean() {
        // Clean out cleared values.
        Reference<? extends V> ref;
        while ((ref = mValueRefQueue.poll()) != null) {
            // Note that the two argument remove method is called to ensure
            // that what is being removed is not a new value.
            mValues.remove(((ValueRef<K, V>) ref).mKey, ref);
        }
    }

    private static class ValueRef<K, V> extends WeakReference<V> {
        final K mKey;

        ValueRef(V value, ReferenceQueue<V> queue, K key) {
            super(value, queue);
            mKey = key;
        }
    }
}
