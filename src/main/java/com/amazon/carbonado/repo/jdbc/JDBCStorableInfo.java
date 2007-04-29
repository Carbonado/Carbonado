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

package com.amazon.carbonado.repo.jdbc;

import java.util.Map;

import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.StorableInfo;

/**
 * Contains all the metadata describing a specific {@link Storable} type as
 * needed by JDBCRepository. It extends the regular {@link StorableInfo} with
 * information gathered from the database.
 *
 * @author Brian S O'Neill
 * @see JDBCStorableIntrospector
 */
public interface JDBCStorableInfo<S extends Storable> extends StorableInfo<S> {
    /**
     * Returns false only if storable type is {@link com.amazon.carbonado.Independent independent}
     * and no matching table was found.
     */
    boolean isSupported();

    /**
     * Returns the optional catalog name for the Storable. Some databases use a
     * catalog name to fully qualify the table name.
     */
    String getCatalogName();

    /**
     * Returns the optional schema name for the Storable. Some databases use a
     * schema name to fully qualify the table name.
     */
    String getSchemaName();

    /**
     * Returns the table name for the Storable or null if unsupported.
     */
    String getTableName();

    /**
     * Returns the qualified table name for the Storable or null if
     * unsupported. Is used by SQL statements.
     */
    String getQualifiedTableName();

    IndexInfo[] getIndexInfo();

    Map<String, JDBCStorableProperty<S>> getAllProperties();

    Map<String, JDBCStorableProperty<S>> getPrimaryKeyProperties();

    Map<String, JDBCStorableProperty<S>> getDataProperties();

    /**
     * Returns auto-increment properties which are primary key members. The map
     * should almost always be empty or contain one property.
     *
     * @since 1.2
     */
    Map<String, JDBCStorableProperty<S>> getIdentityProperties();

    JDBCStorableProperty<S> getVersionProperty();
}
