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

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.AlternateKeys;
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.Key;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Version;

/**
 * Stored information regarding the layout of a Storable type, which is used
 * internally by {@link Layout}. This interface is public only because
 * Carbonado requires storable type definitions to be public.
 *
 * @author Brian S O'Neill
 */
@AlternateKeys({
    @Key({"storableTypeName", "generation"})
})
@PrimaryKey("layoutID")
@Independent
@Alias("CARBONADO_LAYOUT")
public interface StoredLayout extends Storable<StoredLayout>, Unevolvable {
    long getLayoutID();
    void setLayoutID(long typeID);

    /**
     * Storable type name is a fully qualified Java class name.
     */
    String getStorableTypeName();
    void setStorableTypeName(String typeName);

    /**
     * Generation of storable, where 0 represents the first generation.
     */
    int getGeneration();
    void setGeneration(int generation);

    /**
     * Returns the milliseconds from 1970-01-01T00:00:00Z when this record was
     * created.
     */
    long getCreationTimestamp();
    void setCreationTimestamp(long timestamp);

    /**
     * Returns the user that created this generation.
     */
    @Nullable
    String getCreationUser();
    void setCreationUser(String user);

    /**
     * Returns the host machine that created this generation.
     */
    @Nullable
    String getCreationHost();
    void setCreationHost(String host);

    /**
     * Record version number for this StoredTypeLayout instance. Some encoding
     * strategies require a version number.
     */
    @Version
    int getVersionNumber();
    void setVersionNumber(int version);

    /**
     * Since this record cannot evolve, this property allows it to be extended
     * without conflicting with existing records. This record cannot evolve
     * because an evolution strategy likely depends on this interface remaining
     * stable, avoiding a cyclic dependency.
     */
    @Nullable
    byte[] getExtraData();
    void setExtraData(byte[] data);
}
