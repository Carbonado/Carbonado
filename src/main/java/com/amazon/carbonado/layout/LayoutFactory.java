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
import java.util.Map;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;

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
        synchronized (this) {
            if (mReconstructed != null) {
                Layout layout = mReconstructed.get(type);
                if (layout != null) {
                    return layout;
                }
            }
        }

        StorableInfo<?> info = StorableIntrospector.examine(type);

        Transaction txn = mRepository.enterTransaction();
        try {
            // If type represents a new generation, then a new layout needs to
            // be inserted.
            Layout newLayout = null;

            for (int i=0; i<HASH_MULTIPLIERS.length; i++) {
                // Generate an identifier which has a high likelyhood of being unique.
                long layoutID = mixInHash(0L, info, HASH_MULTIPLIERS[i]);

                // Initially use for comparison purposes.
                newLayout = new Layout(this, info, layoutID);

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
                    return knownLayout;
                }

                // If this point is reached, then there was a hash collision in
                // the generated layout ID. This should be extremely rare.
                // Rehash and try again.

                if (i >= HASH_MULTIPLIERS.length - 1) {
                    // No more rehashes to attempt. This should be extremely,
                    // extremely rare, unless there is a bug somewhere.
                    throw new FetchException("Unable to generate unique layout identifier");
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
            txn.commit();

            return newLayout;
        } finally {
            txn.exit();
        }
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
    private long mixInHash(long hash, StorableInfo<?> info, int multiplier) {
        hash = mixInHash(hash, info.getStorableType().getName(), multiplier);

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
                // Okay to mix in annotation hash code since Annotation
                // documentation defines the implementation. It should remain
                // stable between releases, but it is not critical that the
                // hash code always comes out the same. The result would be a
                // duplicate stored layout, but with a different generation.
                // Stored entries will be converted from the "different"
                // generation, causing a very slight performance degradation.
                hash = hash * multiplier + ann.hashCode();
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
}
