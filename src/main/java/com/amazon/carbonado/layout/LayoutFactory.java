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

package com.amazon.carbonado.layout;

import java.lang.annotation.Annotation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.util.Arrays;
import java.util.Map;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistDeadlockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.capability.ResyncCapability;

import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;
import com.amazon.carbonado.info.StorablePropertyAnnotation;

/**
 * Factory for obtaining references to storable layouts.
 *
 * @author Brian S O'Neill
 */
public class LayoutFactory implements LayoutCapability {
    // The first entry is the primary hash multiplier. Subsequent ones are
    // rehash multipliers.
    private static final int[] HASH_MULTIPLIERS = {31, 63};

    final Repository mRepository;
    final Storage<StoredLayout> mLayoutStorage;
    final Storage<StoredLayoutProperty> mPropertyStorage;

    private Map<Class<? extends Storable>, Layout> mReconstructed;

    /**
     * @throws com.amazon.carbonado.SupportException if underlying repository
     * does not support the storables for persisting storable layouts
     */
    public LayoutFactory(Repository repo) throws RepositoryException {
        mRepository = repo;
        mLayoutStorage = repo.storageFor(StoredLayout.class);
        mPropertyStorage = repo.storageFor(StoredLayoutProperty.class);
    }

    /**
     * Returns the layout matching the current definition of the given type.
     *
     * @throws PersistException if type represents a new generation, but
     * persisting this information failed
     */
    public Layout layoutFor(Class<? extends Storable> type)
        throws FetchException, PersistException
    {
        return layoutFor(type, null);
    }

    /**
     * Returns the layout matching the current definition of the given type.
     *
     * @throws PersistException if type represents a new generation, but
     * persisting this information failed
     */
    public Layout layoutFor(Class<? extends Storable> type, LayoutOptions options)
        throws FetchException, PersistException
    {
        if (options != null) {
            // Make side-effect consistently applied.
            options.readOnly();
        }

        synchronized (this) {
            if (mReconstructed != null) {
                Layout layout = mReconstructed.get(type);
                if (layout != null) {
                    return layout;
                }
            }
        }

        StorableInfo<?> info = StorableIntrospector.examine(type);

        Layout layout;
        ResyncCapability resyncCap = null;

        // Try to insert metadata up to three times.
        boolean top = true;
        loadLayout: for (int retryCount = 3;;) {
            try {
                Transaction txn;
                if (top) {
                    txn = mRepository.enterTopTransaction(IsolationLevel.READ_COMMITTED);
                } else {
                    txn = mRepository.enterTransaction(IsolationLevel.READ_COMMITTED);
                }

                txn.setForUpdate(true);
                try {
                    // If type represents a new generation, then a new layout needs to
                    // be inserted.
                    Layout newLayout = null;

                    for (int i=0; i<HASH_MULTIPLIERS.length; i++) {
                        // Generate an identifier which has a high likelyhood of being unique.
                        long layoutID = mixInHash(0L, info, options, HASH_MULTIPLIERS[i]);

                        // Initially use for comparison purposes.
                        newLayout = new Layout(this, info, options, layoutID);

                        StoredLayout storedLayout = mLayoutStorage.prepare();
                        storedLayout.setLayoutID(layoutID);

                        if (!storedLayout.tryLoad()) {
                            // Not found, so break out and insert.
                            break;
                        }

                        Layout knownLayout = new Layout(this, storedLayout);
                        if (knownLayout.equalLayouts(newLayout)) {
                            // Type does not represent a new generation. Return
                            // existing layout.
                            layout = knownLayout;
                            break loadLayout;
                        }

                        if (knownLayout.getAllProperties().size() == 0) {
                            // This is clearly wrong. All Storables must have
                            // at least one property. Assume that layout record
                            // is corrupt so rebuild it.
                            break;
                        }

                        // If this point is reached, then there was a hash collision in
                        // the generated layout ID. This should be extremely rare.
                        // Rehash and try again.

                        if (i >= HASH_MULTIPLIERS.length - 1) {
                            // No more rehashes to attempt. This should be extremely,
                            // extremely rare, unless there is a bug somewhere.
                            throw new FetchException
                                ("Unable to generate unique layout identifier for " +
                                 type.getName());
                        }
                    }

                    // If this point is reached, then type represents a new
                    // generation. Calculate next generation value and insert.

                    assert(newLayout != null);
                    int generation = 0;

                    Cursor<StoredLayout> cursor = mLayoutStorage
                        .query("storableTypeName = ?")
                        .with(info.getStorableType().getName())
                        .orderBy("-generation")
                        .fetch();

                    try {
                        if (cursor.hasNext()) {
                            generation = cursor.next().getGeneration() + 1;
                        }
                    } finally {
                        cursor.close();
                    }

                    newLayout.insert(generation);
                    layout = newLayout;

                    txn.commit();
                } finally {
                    txn.exit();
                }

                break;
            } catch (UniqueConstraintException e) {
                // This might be caused by a transient replication error. Retry
                // a few times before throwing exception. Wait up to a second
                // before each retry.
                retryCount = e.backoff(e, retryCount, 1000);
                resyncCap = mRepository.getCapability(ResyncCapability.class);
            } catch (FetchException e) {
                if (e instanceof FetchDeadlockException || e instanceof FetchTimeoutException) {
                    // Might be caused by coarse locks. Switch to nested
                    // transaction to share the locks.
                    if (top) {
                        top = false;
                        retryCount = e.backoff(e, retryCount, 100);
                        continue;
                    }
                }
                throw e;
            } catch (PersistException e) {
                if (e instanceof PersistDeadlockException || e instanceof PersistTimeoutException){
                    // Might be caused by coarse locks. Switch to nested
                    // transaction to share the locks.
                    if (top) {
                        top = false;
                        retryCount = e.backoff(e, retryCount, 100);
                        continue;
                    }
                }
                throw e;
            }
        }

        if (resyncCap != null) {
            // Make sure that all layout records are sync'd.
            try {
                resyncCap.resync(StoredLayoutProperty.class, 1.0, null);
            } catch (RepositoryException e) {
                throw e.toPersistException();
            }
        }

        return layout;
    }

    /**
     * Returns the layout for a particular generation of the given type.
     *
     * @param generation desired generation
     * @throws FetchNoneException if generation not found
     */
    public Layout layoutFor(Class<? extends Storable> type, int generation)
        throws FetchException, FetchNoneException
    {
        StoredLayout storedLayout =
            mLayoutStorage.query("storableTypeName = ? & generation = ?")
            .with(type.getName()).with(generation)
            .loadOne();
        return new Layout(this, storedLayout);
    }

    synchronized void registerReconstructed
        (Class<? extends Storable> reconstructed, Layout layout)
    {
        if (mReconstructed == null) {
            mReconstructed = new SoftValuedHashMap();
        }
        mReconstructed.put(reconstructed, layout);
    }

    /**
     * Creates a long hash code that attempts to mix in all relevant layout
     * elements.
     */
    private long mixInHash(long hash, StorableInfo<?> info, LayoutOptions options, int multiplier)
    {
        hash = mixInHash(hash, info.getStorableType().getName(), multiplier);
        hash = mixInHash(hash, options, multiplier);

        for (StorableProperty<?> property : info.getAllProperties().values()) {
            if (!property.isJoin()) {
                hash = mixInHash(hash, property, multiplier);
            }
        }

        return hash;
    }

    /**
     * Creates a long hash code that attempts to mix in all relevant layout
     * elements.
     */
    private long mixInHash(long hash, StorableProperty<?> property, int multiplier) {
        hash = mixInHash(hash, property.getName(), multiplier);
        hash = mixInHash(hash, property.getType().getName(), multiplier);
        hash = hash * multiplier + (property.isNullable() ? 1 : 2);
        hash = hash * multiplier + (property.isPrimaryKeyMember() ? 1 : 2);

        // Keep this in for compatibility with prior versions of hash code.
        hash = hash * multiplier + 1;

        if (property.getAdapter() != null) {
            // Keep this in for compatibility with prior versions of hash code.
            hash += 1;

            StorablePropertyAdapter adapter = property.getAdapter();
            StorablePropertyAnnotation annotation = adapter.getAnnotation();

            hash = mixInHash(hash, annotation.getAnnotationType().getName(), multiplier);

            // Annotation may contain parameters which affect how property
            // value is stored. So mix that in too.
            Annotation ann = annotation.getAnnotation();
            if (ann != null) {
                hash = hash * multiplier + annHashCode(ann);
            }
        }

        return hash;
    }

    private long mixInHash(long hash, CharSequence value, int multiplier) {
        for (int i=value.length(); --i>=0; ) {
            hash = hash * multiplier + value.charAt(i);
        }
        return hash;
    }

    private long mixInHash(long hash, LayoutOptions options, int multiplier) {
        if (options != null) {
            byte[] data = options.encode();
            if (data != null) {
                for (int b : data) {
                    hash = hash * multiplier + (b & 0xff);
                }
            }
        }
        return hash;
    }

    /**
     * Returns an annotation hash code using a algorithm similar to the
     * default. The difference is in the handling of class and enum values. The
     * name is chosen for the hash code component instead of the instance
     * because it is stable between invocations of the JVM.
     */
    private static int annHashCode(Annotation ann) {
        int hash = 0;

        Method[] methods = ann.getClass().getDeclaredMethods();
        for (Method m : methods) {
            if (m.getReturnType() == null || m.getReturnType() == void.class) {
                continue;
            }
            if (m.getParameterTypes().length != 0) {
                continue;
            }

            String name = m.getName();
            if (name.equals("hashCode") ||
                name.equals("toString") ||
                name.equals("annotationType"))
            {
                continue;
            }

            Object value;
            try {
                value = m.invoke(ann);
            } catch (InvocationTargetException e) {
                continue;
            } catch (IllegalAccessException e) {
                continue;
            }

            hash += (127 * name.hashCode()) ^ annValueHashCode(value);
        }

        return hash;
    }

    private static int annValueHashCode(Object value) {
        Class type = value.getClass();
        if (!type.isArray()) {
            if (value instanceof String || type.isPrimitive()) {
                return value.hashCode();
            } else if (value instanceof Class) {
                // Use name for stable hash code.
                return ((Class) value).getName().hashCode();
            } else if (value instanceof Enum) {
                // Use name for stable hash code.
                return ((Enum) value).name().hashCode();
            } else if (value instanceof Annotation) {
                return annHashCode((Annotation) value);
            } else {
                return value.hashCode();
            }
        } else if (type == byte[].class) {
            return Arrays.hashCode((byte[]) value);
        } else if (type == char[].class) {
            return Arrays.hashCode((char[]) value);
        } else if (type == double[].class) {
            return Arrays.hashCode((double[]) value);
        } else if (type == float[].class) {
            return Arrays.hashCode((float[]) value);
        } else if (type == int[].class) {
            return Arrays.hashCode((int[]) value);
        } else if (type == long[].class) {
            return Arrays.hashCode((long[]) value);
        } else if (type == short[].class) {
            return Arrays.hashCode((short[]) value);
        } else if (type == boolean[].class) {
            return Arrays.hashCode((boolean[]) value);
        } else {
            return Arrays.hashCode((Object[]) value);
        }
    }
}
