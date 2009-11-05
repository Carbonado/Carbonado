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
 * Stored property joined to a {@link StoredLayout}, which is used internally
 * by {@link LayoutProperty}. This interface is public only because Carbonado
 * requires storable type definitions to be public.
 *
 * @author Brian S O'Neill
 */
@AlternateKeys({
    @Key({"layoutID", "propertyName"})
})
@PrimaryKey({"layoutID", "ordinal"})
@Independent
@Alias("CARBONADO_LAYOUT_PROPERTY")
public interface StoredLayoutProperty extends Storable<StoredLayoutProperty>, Unevolvable {
    long getLayoutID();
    void setLayoutID(long typeID);

    /**
     * Ordinal defines the order in which this property appears in it enclosing
     * layout.
     */
    int getOrdinal();
    void setOrdinal(int ordinal);

    String getPropertyName();
    void setPropertyName(String name);

    /**
     * Property type descriptor is a Java type descriptor.
     */
    String getPropertyTypeDescriptor();
    void setPropertyTypeDescriptor(String type);

    /**
     * Returns true of property value can be set to null.
     */
    boolean isNullable();
    void setNullable(boolean nullable);

    /**
     * Returns true if property is a member of the primary key.
     */
    boolean isPrimaryKeyMember();
    void setPrimaryKeyMember(boolean pk);

    /**
     * Returns true if this property is the designated version number for the
     * Storable.
     */
    boolean isVersion();
    void setVersion(boolean version);

    /**
     * Adapter type name is a fully qualified Java class name. If property has
     * no adapter, then null is returned.
     */
    @Nullable
    String getAdapterTypeName();
    void setAdapterTypeName(String name);

    /**
     * Parameters for adapter, or null if property has no explicit adapter.
     */
    @Nullable
    String getAdapterParams();
    void setAdapterParams(String params);

    /**
     * Record version number for this StoredPropertyLayout instance. Some
     * encoding strategies require a version number.
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
