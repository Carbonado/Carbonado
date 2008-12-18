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

    // cache for generators
    private static Map<StorableIndex, Reference<IndexEntryGenerator>> cCache =
            new WeakHashMap<StorableIndex, Reference<IndexEntryGenerator>>();


    /**
     * Returns a new or cached generator instance. The caching of generators is
     * soft, so if no references remain to a given instance it may be garbage
     * collected. A subsequent call will return a newly created instance.
     *
     * <p>In addition to generating an index entry storable, this class
     * contains methods to operate on it. Care must be taken to ensure that the
     * index entry instances are of the same type that the generator expects.
     * Since the generator may be garbage collected freely of the generated
     * index entry class, it is possible for index entries to be passed to a
     * generator instance that does not understand it. For example:
     *
     * <pre>
     * StorableIndex index = ...
     * Class indexEntryClass = IndexEntryGenerator.getInstance(index).getIndexEntryClass();
     * ...
     * garbage collection
     * ...
     * Storable indexEntry = instance of indexEntryClass
     * // Might fail because generator instance is new
     * IndexEntryGenerator.getInstance(index).setAllProperties(indexEntry, source);
     * </pre>
     *
     * The above code can be fixed by saving a local reference to the generator:
     *
     * <pre>
     * StorableIndex index = ...
     * IndexEntryGenerator generator = IndexEntryGenerator.getInstance(index);
     * Class indexEntryClass = generator.getIndexEntryClass();
     * ...
     * Storable indexEntry = instance of indexEntryClass
     * generator.setAllProperties(indexEntry, source);
     * </pre>
     *
     * @throws SupportException if any non-primary key property doesn't have a
     * public read method.
     */
    public static <S extends Storable> IndexEntryGenerator<S>
            getInstance(StorableIndex<S> index) throws SupportException
    {
        synchronized(cCache) {
            IndexEntryGenerator<S> generator;
            Reference<IndexEntryGenerator> ref = cCache.get(index);
            if (ref != null) {
                generator = ref.get();
                if (generator != null) {
                    return generator;
                }
            }
            generator = new IndexEntryGenerator<S>(index);
            cCache.put(index, new SoftReference<IndexEntryGenerator>(generator));
            return generator;
        }
    }

    private SyntheticStorableReferenceAccess<S> mIndexAccess;

    /**
     * Convenience class for gluing new "builder" style synthetics to the traditional
     * generator style.
     * @param index Generator style index specification
     */
    public IndexEntryGenerator(StorableIndex<S> index) throws SupportException {
        // Need to try to find the base type.  This is an awkward way to do it,
        // but we have nothing better available to us
        Class<S> type = index.getProperty(0).getEnclosingType();

        SyntheticStorableReferenceBuilder<S> builder =
            new SyntheticStorableReferenceBuilder<S>(type, index.isUnique());

        for (int i=0; i<index.getPropertyCount();  i++) {
            StorableProperty source = index.getProperty(i);
            builder.addKeyProperty(source.getName(), index.getPropertyDirection(i));
        }

        builder.build();

        mIndexAccess = builder.getReferenceAccess();
    }

    /**
     * Returns generated index entry class, which is abstract.
     *
     * @return class of index entry, which is a custom Storable
     */
    public Class<? extends Storable> getIndexEntryClass() {
        return mIndexAccess.getReferenceClass();
    }

    /**
     * Sets all the primary key properties of the given master, using the
     * applicable properties of the given index entry.
     *
     * @param indexEntry source of property values
     * @param master master whose primary key properties will be set
     */
    public void copyToMasterPrimaryKey(Storable indexEntry, S master) {
        mIndexAccess.copyToMasterPrimaryKey(indexEntry, master);
    }

    /**
     * Sets all the properties of the given index entry, using the applicable
     * properties of the given master.
     *
     * @param indexEntry index entry whose properties will be set
     * @param master source of property values
     */
    public void copyFromMaster(Storable indexEntry, S master) {
        mIndexAccess.copyFromMaster(indexEntry, master);
    }

    /**
     * Returns true if the properties of the given index entry match those
     * contained in the master. This will always return true after a call to
     * setAllProperties.
     *
     * @param indexEntry index entry whose properties will be tested
     * @param master source of property values
     */
    public boolean isConsistent(Storable indexEntry, S master) {
        return mIndexAccess.isConsistent(indexEntry, master);
    }

    /**
     * Returns a comparator for ordering index entries.
     */
    public Comparator<? extends Storable> getComparator() {
        return mIndexAccess.getComparator();
    }
}
