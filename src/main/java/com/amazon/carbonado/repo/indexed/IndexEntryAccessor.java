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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.capability.IndexInfo;

/**
 * Provides low-level access to index data, which can be used for manual
 * inspection and repair.
 *
 * @author Brian S O'Neill
 * @see IndexEntryAccessCapability
 */
public interface IndexEntryAccessor<S extends Storable> extends IndexInfo {
    /**
     * Returns the index entry storage.
     */
    Storage<?> getIndexEntryStorage();

    /**
     * Sets all the primary key properties of the given master, using the
     * applicable properties of the given index entry.
     *
     * @param indexEntry source of property values
     * @param master master whose primary key properties will be set
     */
    void copyToMasterPrimaryKey(Storable indexEntry, S master) throws FetchException;

    /**
     * Sets all the properties of the given index entry, using the applicable
     * properties of the given master.
     *
     * @param indexEntry index entry whose properties will be set
     * @param master source of property values
     */
    void copyFromMaster(Storable indexEntry, S master) throws FetchException;

    /**
     * Returns true if the properties of the given index entry match those
     * contained in the master, exluding any version property. This will always
     * return true after a call to copyFromMaster.
     *
     * @param indexEntry index entry whose properties will be tested
     * @param master source of property values
     */
    boolean isConsistent(Storable indexEntry, S master) throws FetchException;

    /**
     * Repairs the index by inserting missing entries and fixing
     * inconsistencies.
     *
     * @param desiredSpeed throttling parameter - 1.0 = full speed, 0.5 = half
     * speed, 0.1 = one-tenth speed, etc
     */
    void repair(double desiredSpeed) throws RepositoryException;

    /**
     * Returns a comparator for ordering index entries.
     */
    Comparator<? extends Storable> getComparator();
}
