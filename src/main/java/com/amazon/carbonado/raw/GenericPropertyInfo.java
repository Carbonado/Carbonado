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

package com.amazon.carbonado.raw;

import java.lang.reflect.Method;

import org.cojen.classfile.TypeDesc;

/**
 * Minimal information required by {@link GenericEncodingStrategy} to encode
 * and decode a storable property.
 *
 * @author Brian S O'Neill
 */
public interface GenericPropertyInfo {
    String getPropertyName();

    /**
     * Returns the user specified property type.
     */
    TypeDesc getPropertyType();

    /**
     * Returns the storage supported type. If it differs from the property
     * type, then adapter methods must also exist.
     */
    TypeDesc getStorageType();

    boolean isNullable();

    boolean isLob();

    boolean isDerived();

    /**
     * Returns the optional method used to adapt the property from the
     * storage supported type to the user visible type.
     */
    Method getFromStorageAdapter();

    /**
     * Returns the optional method used to adapt the property from the user
     * visible type to the storage supported type.
     */
    Method getToStorageAdapter();
}
