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

import com.amazon.carbonado.layout.LayoutProperty;
import com.amazon.carbonado.lob.Lob;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class LayoutPropertyInfo implements GenericPropertyInfo {
    private final LayoutProperty mProp;
    private final TypeDesc mPropertyType;
    private final TypeDesc mStorageType;
    private final Method mFromStorage;
    private final Method mToStorage;

    LayoutPropertyInfo(LayoutProperty property) {
        this(property, null, null, null);
    }

    LayoutPropertyInfo(LayoutProperty property,
                       Class<?> storageType, Method fromStorage, Method toStorage)
    {
        mProp = property;
        mPropertyType = TypeDesc.forDescriptor(property.getPropertyTypeDescriptor());
        if (storageType == null) {
            mStorageType = mPropertyType;
        } else {
            mStorageType = TypeDesc.forClass(storageType);
        }
        mFromStorage = fromStorage;
        mToStorage = toStorage;
    }

    public String getPropertyName() {
        return mProp.getPropertyName();
    }

    public TypeDesc getPropertyType() {
        return mPropertyType;
    }

    public TypeDesc getStorageType() {
        return mStorageType;
    }

    public boolean isNullable() {
        return mProp.isNullable();
    }

    public boolean isLob() {
        Class clazz = mPropertyType.toClass();
        return clazz != null && Lob.class.isAssignableFrom(clazz);
    }

    public Method getFromStorageAdapter() {
        return mFromStorage;
    }

    public Method getToStorageAdapter() {
        return mToStorage;
    }
}