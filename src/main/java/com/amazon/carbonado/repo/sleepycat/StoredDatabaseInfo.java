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

package com.amazon.carbonado.repo.sleepycat;

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Version;

import com.amazon.carbonado.layout.Unevolvable;
import com.amazon.carbonado.repo.indexed.Unindexed;

/**
 * Stores basic information about the BDB databases managed by BDBRepository.
 *
 * <p>Note: This storable cannot have indexes defined, since it is used to
 * discover information about indexes. It would create a cyclic dependency.
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("databaseName")
@Independent
@Alias("CARBONADO_DATABASE_INFO")
public abstract class StoredDatabaseInfo implements Storable, Unevolvable, Unindexed {
    /** Evolution strategy code */
    public static final int EVOLUTION_NONE = 0, EVOLUTION_STANDARD = 1;

    public StoredDatabaseInfo() {
    }

    public abstract String getDatabaseName();

    public abstract void setDatabaseName(String name);

    /**
     * Returns the index name descriptor for the keys of this database. This
     * descriptor is defined by {@link com.amazon.carbonado.info.StorableIndex}, and
     * it does not contain type information.
     */
    @Nullable
    public abstract String getIndexNameDescriptor();

    public abstract void setIndexNameDescriptor(String descriptor);

    /**
     * Returns the types of the index properties. This descriptor is defined by
     * {@link com.amazon.carbonado.info.StorableIndex}.
     */
    @Nullable
    public abstract String getIndexTypeDescriptor();

    public abstract void setIndexTypeDescriptor(String descriptor);

    /**
     * Returns EVOLUTION_NONE if evolution of records is not supported.
     */
    public abstract int getEvolutionStrategy();

    public abstract void setEvolutionStrategy(int strategy);

    /**
     * Returns the milliseconds from 1970-01-01T00:00:00Z when this record was
     * created.
     */
    public abstract long getCreationTimestamp();

    public abstract void setCreationTimestamp(long timestamp);

    /**
     * Record version number for this StoredDatabaseInfo instance. Some
     * encoding strategies require a version number.
     */
    @Version
    public abstract int getVersionNumber();

    public abstract void setVersionNumber(int version);

    /**
     * Since this record cannot evolve, this property allows it to be extended
     * without conflicting with existing records. This record cannot evolve
     * because an evolution strategy likely depends on this interface remaining
     * stable, avoiding a cyclic dependency.
     */
    @Nullable
    public abstract byte[] getExtraData();

    public abstract void setExtraData(byte[] data);
}
