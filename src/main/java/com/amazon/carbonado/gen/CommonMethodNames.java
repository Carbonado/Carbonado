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

package com.amazon.carbonado.gen;

/**
 * Collection of constant method names for the public API.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public class CommonMethodNames {
    /** Storable API method name */
    public static final String
        LOAD_METHOD_NAME               = "load",
        INSERT_METHOD_NAME             = "insert",
        UPDATE_METHOD_NAME             = "update",
        DELETE_METHOD_NAME             = "delete",
        TRY_LOAD_METHOD_NAME           = "tryLoad",
        TRY_INSERT_METHOD_NAME         = "tryInsert",
        TRY_UPDATE_METHOD_NAME         = "tryUpdate",
        TRY_DELETE_METHOD_NAME         = "tryDelete",
        STORABLE_TYPE_METHOD_NAME      = "storableType",
        COPY_METHOD_NAME               = "copy",
        CLONE_METHOD_NAME              = "clone",
        COPY_ALL_PROPERTIES            = "copyAllProperties",
        COPY_PRIMARY_KEY_PROPERTIES    = "copyPrimaryKeyProperties",
        COPY_VERSION_PROPERTY          = "copyVersionProperty",
        COPY_UNEQUAL_PROPERTIES        = "copyUnequalProperties",
        COPY_DIRTY_PROPERTIES          = "copyDirtyProperties",
        HAS_DIRTY_PROPERTIES           = "hasDirtyProperties",
        MARK_PROPERTIES_CLEAN          = "markPropertiesClean",
        MARK_ALL_PROPERTIES_CLEAN      = "markAllPropertiesClean",
        MARK_PROPERTIES_DIRTY          = "markPropertiesDirty",
        MARK_ALL_PROPERTIES_DIRTY      = "markAllPropertiesDirty",
        IS_PROPERTY_UNINITIALIZED      = "isPropertyUninitialized",
        IS_PROPERTY_DIRTY              = "isPropertyDirty",
        IS_PROPERTY_CLEAN              = "isPropertyClean",
        IS_PROPERTY_SUPPORTED          = "isPropertySupported",
        GET_PROPERTY_VALUE             = "getPropertyValue",
        SET_PROPERTY_VALUE             = "setPropertyValue",
        PROPERTY_MAP                   = "propertyMap",
        WRITE_TO                       = "writeTo",
        READ_FROM                      = "readFrom",
        TO_STRING_KEY_ONLY_METHOD_NAME = "toStringKeyOnly",
        TO_STRING_METHOD_NAME          = "toString",
        HASHCODE_METHOD_NAME           = "hashCode",
        EQUALS_METHOD_NAME             = "equals",
        EQUAL_PRIMARY_KEYS_METHOD_NAME = "equalPrimaryKeys",
        EQUAL_PROPERTIES_METHOD_NAME   = "equalProperties";

    /** Storage API method name */
    public static final String
        QUERY_METHOD_NAME    = "query",
        PREPARE_METHOD_NAME  = "prepare";

    /** Query API method name */
    public static final String
        LOAD_ONE_METHOD_NAME     = "loadOne",
        TRY_LOAD_ONE_METHOD_NAME = "tryLoadOne",
        WITH_METHOD_NAME         = "with",
        FETCH_METHOD_NAME        = "fetch";

    /** Repository API method name */
    public static final String
        STORAGE_FOR_METHOD_NAME      = "storageFor",
        ENTER_TRANSACTION_METHOD_NAME = "enterTransaction",
        GET_TRANSACTION_ISOLATION_LEVEL_METHOD_NAME = "getTransactionIsolationLevel";

    /** Transaction API method name */
    public static final String
        SET_FOR_UPDATE_METHOD_NAME = "setForUpdate",
        COMMIT_METHOD_NAME = "commit",
        EXIT_METHOD_NAME   = "exit";
}
