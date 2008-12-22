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

import java.util.Comparator;
import java.util.Map;
import java.util.WeakHashMap;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.synthetic.SyntheticStorableReferenceAccess;
import com.amazon.carbonado.synthetic.SyntheticStorableReferenceBuilder;

/**
 * IndexEntryGenerator creates new kinds of Storables suitable for indexing a
 * master Storable.
 *
 * @author Brian S O'Neill
 * @author Don Schneider
 *
 */
class IndexEntryGenerator <S extends Storable> {

    // cache for access classes
    private static Map<StorableIndex, Reference<SyntheticStorableReferenceAccess>> cCache =
        new WeakHashMap<StorableIndex, Reference<SyntheticStorableReferenceAccess>>();

    /**
     * Returns a new or cached index access instance. The caching of accessors
     * is soft, so if no references remain to a given instance it may be
     * garbage collected. A subsequent call will return a newly created
     * instance.
     *
     * <p>In addition to generating an index entry storable, the accessor
     * contains methods to operate on it. Care must be taken to ensure that the
     * index entry instances are of the same type that the accessor expects.
     * Since the accessor may be garbage collected freely of the generated
     * index entry class, it is possible for index entries to be passed to a
     * accessor instance that does not understand it. For example:
     *
     * <pre>
     * StorableIndex index = ...
     * Class indexEntryClass = IndexEntryGenerator.getIndexAccess(index).getReferenceClass();
     * ...
     * garbage collection
     * ...
     * Storable indexEntry = instance of indexEntryClass
     * // Might fail because generator instance is new
     * IndexEntryGenerator.getIndexAccess(index).copyFromMaster(indexEntry, master);
     * </pre>
     *
     * The above code can be fixed by saving a local reference to the accessor:
     *
     * <pre>
     * StorableIndex index = ...
     * SyntheticStorableReferenceAccess access = IndexEntryGenerator.getIndexAccess(index);
     * Class indexEntryClass = access.getReferenceClass();
     * ...
     * Storable indexEntry = instance of indexEntryClass
     * access.copyFromMaster(indexEntry, master);
     * </pre>
     *
     * @throws SupportException if any non-primary key property doesn't have a
     * public read method.
     */
    public static <S extends Storable>
        SyntheticStorableReferenceAccess<S> getIndexAccess(StorableIndex<S> index)
        throws SupportException
    {
        synchronized (cCache) {
            SyntheticStorableReferenceAccess<S> access;
            Reference<SyntheticStorableReferenceAccess> ref = cCache.get(index);
            if (ref != null) {
                access = ref.get();
                if (access != null) {
                    return access;
                }
            }

            // Need to try to find the base type.  This is an awkward way to do
            // it, but we have nothing better available to us
            Class<S> type = index.getProperty(0).getEnclosingType();

            SyntheticStorableReferenceBuilder<S> builder =
                new SyntheticStorableReferenceBuilder<S>(type, index.isUnique());

            for (int i=0; i<index.getPropertyCount(); i++) {
                StorableProperty<S> source = index.getProperty(i);
                builder.addKeyProperty(source.getName(), index.getPropertyDirection(i));
            }

            builder.build();
            access = builder.getReferenceAccess();

            cCache.put(index, new SoftReference<SyntheticStorableReferenceAccess>(access));

            return access;
        }
    }

    private IndexEntryGenerator() {
    }
}
