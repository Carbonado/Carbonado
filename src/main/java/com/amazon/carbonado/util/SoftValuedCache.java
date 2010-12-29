/*
 * Copyright 2010 Amazon Technologies, Inc. or its affiliates.
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
import java.lang.ref.SoftReference;

/**
 * Simple thread-safe cache which evicts entries via a shared background
 * thread. Cache permits null keys, but not null values.
 *
 * @author Brian S O'Neill
 * @deprecated use Cojen {@link org.cojen.util.Cache} interface
 */
@Deprecated
public abstract class SoftValuedCache<K, V> {
    public static <K, V> SoftValuedCache<K, V> newCache(int capacity) {
        try {
            return new Wrapper<K, V>(capacity);
        } catch (NoClassDefFoundError e) {
            // Use older implementation for compatibility.
            return new Impl<K, V>(capacity);
        }
    }

    public abstract int size();

    public abstract boolean isEmpty();

    public abstract V get(K key);

    public abstract V put(K key, V value);

    public abstract V putIfAbsent(K key, V value);

    public abstract V remove(K key);

    public abstract boolean remove(K key, V value);

    public abstract boolean replace(K key, V oldValue, V newValue);

    public abstract V replace(K key, V value);

    public abstract void clear();

    public abstract String toString();

    private static class Wrapper<K, V> extends SoftValuedCache<K, V> {
        private final org.cojen.util.Cache<K, V> mCache;

        Wrapper(int capacity) {
            mCache = new org.cojen.util.SoftValueCache<K, V>(capacity);
        }

        @Override
        public int size() {
            return mCache.size();
        }

        @Override
        public boolean isEmpty() {
            return mCache.isEmpty();
        }

        @Override
        public V get(K key) {
            return mCache.get(key);
        }

        @Override
        public V put(K key, V value) {
            return mCache.put(key, value);
        }

        @Override
        public V putIfAbsent(K key, V value) {
            return mCache.putIfAbsent(key, value);
        }

        @Override
        public V remove(K key) {
            return mCache.remove(key);
        }

        @Override
        public boolean remove(K key, V value) {
            return mCache.remove(key, value);
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            return mCache.replace(key, oldValue, newValue);
        }

        @Override
        public V replace(K key, V value) {
            return mCache.replace(key, value);
        }

        @Override
        public void clear() {
            mCache.clear();
        }

        @Override
        public String toString() {
            return mCache.toString();
        }
    }

    private static class Impl<K, V> extends SoftValuedCache<K, V> {
        private static final float LOAD_FACTOR = 0.75f;

        final static Evictor cEvictor;

        static {
            Evictor evictor = new Evictor();
            evictor.setName("SoftValuedCache Evictor");
            evictor.setDaemon(true);
            evictor.setPriority(Thread.MAX_PRIORITY);
            evictor.start();
            cEvictor = evictor;
        }

        private Entry<K, V>[] mEntries;
        private int mSize;
        private int mThreshold;

        Impl(int capacity) {
            mEntries = new Entry[capacity];
            mThreshold = (int) (capacity * LOAD_FACTOR);
        }

        @Override
        public synchronized int size() {
            return mSize;
        }

        @Override
        public synchronized boolean isEmpty() {
            return mSize == 0;
        }

        @Override
        public synchronized V get(K key) {
            int hash = key == null ? 0 : key.hashCode();
            Entry<K, V>[] entries = mEntries;
            int index = (hash & 0x7fffffff) % entries.length;
            for (Entry<K, V> e = entries[index]; e != null; e = e.mNext) {
                if (e.matches(key, hash)) {
                    return e.get();
                }
            }
            return null;
        }

        @Override
        public synchronized V put(K key, V value) {
            int hash = key == null ? 0 : key.hashCode();
            Entry<K, V>[] entries = mEntries;
            int index = (hash & 0x7fffffff) % entries.length;
            for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
                if (e.matches(key, hash)) {
                    V old = e.get();
                    e.clear();
                    Entry<K, V> newEntry;
                    if (prev == null) {
                        newEntry = new Entry<K, V>(this, hash, key, value, e.mNext);
                    } else {
                        prev.mNext = e.mNext;
                        newEntry = new Entry<K, V>(this, hash, key, value, entries[index]);
                    }
                    entries[index] = newEntry;
                    return old;
                } else {
                    prev = e;
                }
            }

            if (mSize >= mThreshold) {
                cleanup();
                if (mSize >= mThreshold) {
                    rehash();
                    entries = mEntries;
                    index = (hash & 0x7fffffff) % entries.length;
                }
            }

            entries[index] = new Entry<K, V>(this, hash, key, value, entries[index]);
            mSize++;
            return null;
        }

        @Override
        public synchronized V putIfAbsent(K key, V value) {
            V existing = get(key);
            return existing == null ? put(key, value) : existing;
        }

        @Override
        public synchronized V remove(K key) {
            int hash = key == null ? 0 : key.hashCode();
            Entry<K, V>[] entries = mEntries;
            int index = (hash & 0x7fffffff) % entries.length;
            for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
                if (e.matches(key, hash)) {
                    V old = e.get();
                    e.clear();
                    if (prev == null) {
                        entries[index] = e.mNext;
                    } else {
                        prev.mNext = e.mNext;
                    }
                    mSize--;
                    return old;
                } else {
                    prev = e;
                }
            }
            return null;
        }

        @Override
        public synchronized boolean remove(K key, V value) {
            V existing = get(key);
            if (existing != null && existing.equals(value)) {
                remove(key);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public synchronized boolean replace(K key, V oldValue, V newValue) {
            V existing = get(key);
            if (existing != null && existing.equals(oldValue)) {
                put(key, newValue);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public synchronized V replace(K key, V value) {
            return get(key) == null ? null : put(key, value);
        }

        @Override
        public synchronized void clear() {
            Entry[] entries = mEntries;
            for (int i=entries.length; --i>=0; ) {
                Entry e = entries[i];
                if (e != null) {
                    e.clear();
                    entries[i] = null;
                }
            }
            mSize = 0;
        }

        @Override
        public synchronized String toString() {
            if (isEmpty()) {
                return "{}";
            }

            StringBuilder b = new StringBuilder();
            b.append('{');

            Entry<K, V>[] entries = mEntries;
            int removed = 0;
            boolean any = false;

            for (int i=entries.length; --i>=0 ;) {
                for (Entry<K, V> e = entries[i], prev = null; e != null; e = e.mNext) {
                    V value = e.get();
                    if (value == null) {
                        // Clean up after a cleared Reference.
                        if (prev == null) {
                            entries[i] = e.mNext;
                        } else {
                            prev.mNext = e.mNext;
                        }
                        removed++;
                    } else {
                        prev = e;
                        if (any) {
                            b.append(',').append(' ');
                        }
                        K key = e.mKey;
                        b.append(key).append('=').append(value);
                        any = true;
                    }
                }
            }

            mSize -= removed;

            b.append('}');
            return b.toString();
        }

        synchronized void removeCleared(Entry<K, V> cleared) {
            Entry<K, V>[] entries = mEntries;
            int index = (cleared.mHash & 0x7fffffff) % entries.length;
            for (Entry<K, V> e = entries[index], prev = null; e != null; e = e.mNext) {
                if (e == cleared) {
                    if (prev == null) {
                        entries[index] = e.mNext;
                    } else {
                        prev.mNext = e.mNext;
                    }
                    mSize--;
                    return;
                } else {
                    prev = e;
                }
            }
        }

        private synchronized void cleanup() {
            Entry<K, V>[] entries = mEntries;
            int removed = 0;

            for (int i=entries.length; --i>=0 ;) {
                for (Entry<K, V> e = entries[i], prev = null; e != null; e = e.mNext) {
                    if (e.get() == null) {
                        // Clean up after a cleared Reference.
                        if (prev == null) {
                            entries[i] = e.mNext;
                        } else {
                            prev.mNext = e.mNext;
                        }
                        removed++;
                    } else {
                        prev = e;
                    }
                }
            }

            mSize -= removed;
        }

        private synchronized void rehash() {
            Entry<K, V>[] oldEntries = mEntries;
            int newCapacity = oldEntries.length * 2 + 1;
            Entry<K, V>[] newEntries = new Entry[newCapacity];
            int removed = 0;

            for (int i=oldEntries.length; --i>=0 ;) {
                for (Entry<K, V> old = oldEntries[i]; old != null; ) {
                    Entry<K, V> e = old;
                    old = old.mNext;
                    // Only copy entry if its value hasn't been cleared.
                    if (e.get() == null) {
                        removed++;
                    } else {
                        int index = (e.mHash & 0x7fffffff) % newCapacity;
                        e.mNext = newEntries[index];
                        newEntries[index] = e;
                    }
                }
            }

            mEntries = newEntries;
            mSize -= removed;
            mThreshold = (int) (newCapacity * LOAD_FACTOR);
        }
    }

    private static class Entry<K, V> extends Ref<V> {
        final Impl<K, V> mCache;
        final int mHash;
        final K mKey;
        Entry mNext;

        Entry(Impl<K, V> cache, int hash, K key, V value, Entry next) {
            super(value);
            mCache = cache;
            mHash = hash;
            mKey = key;
            mNext = next;
        }

        @Override
        void remove() {
            mCache.removeCleared(this);
        }

        boolean matches(K key, int hash) {
            return hash == mHash && (key == null ? mKey == null : key.equals(mKey));
        }
    }

    private static abstract class Ref<T> extends SoftReference<T> {
        Ref(T referent) {
            super(referent, Impl.cEvictor.mQueue);
        }

        abstract void remove();
    }

    private static class Evictor extends Thread {
        final ReferenceQueue<Object> mQueue;

        Evictor() {
            mQueue = new ReferenceQueue<Object>();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ((Ref) mQueue.remove()).remove();
                }
            } catch (InterruptedException e) {
            }
        }
    }
}
