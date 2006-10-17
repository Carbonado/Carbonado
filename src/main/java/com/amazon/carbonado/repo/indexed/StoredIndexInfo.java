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

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Version;

import com.amazon.carbonado.layout.Unevolvable;

/**
 * Stores basic information about the indexes managed by IndexedRepository.
 *
 * <p>Note: This storable cannot have indexes defined, since it is used to
 * discover information about indexes. It would create a cyclic dependency.
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("indexName")
@Independent
@Alias("CARBONADO_INDEX_INFO")
public interface StoredIndexInfo extends Storable, Unevolvable, Unindexed {
    /**
     * Returns the index name, which is also a valid index name
     * descriptor. This descriptor is defined by {@link
     * com.amazon.carbonado.info.StorableIndex}. The name descriptor does not
     * contain type information.
     */
    String getIndexName();

    void setIndexName(String name);

    /**
     * Returns the types of the index properties. This descriptor is defined by
     * {@link com.amazon.carbonado.info.StorableIndex}.
     */
    @Nullable
    String getIndexTypeDescriptor();

    void setIndexTypeDescriptor(String descriptor);

    /**
     * Returns the milliseconds from 1970-01-01T00:00:00Z when this record was
     * created.
     */
    long getCreationTimestamp();

    void setCreationTimestamp(long timestamp);

    /**
     * Record version number for this StoredIndexInfo instance. Some encoding
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
