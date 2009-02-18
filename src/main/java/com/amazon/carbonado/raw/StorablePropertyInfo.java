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

import org.cojen.classfile.CodeAssembler;
import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.lob.Lob;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class StorablePropertyInfo implements GenericPropertyInfo {
    private final StorableProperty<?> mProp;
    private final TypeDesc mPropertyType;
    private final TypeDesc mStorageType;
    private final Method mFromStorage;
    private final Method mToStorage;

    StorablePropertyInfo(StorableProperty<?> property) {
        this(property, null, null, null);
    }

    StorablePropertyInfo(StorableProperty<?> property,
                         Class<?> storageType, Method fromStorage, Method toStorage) {
        mProp = property;
        mPropertyType = TypeDesc.forClass(property.getType());
        if (storageType == null) {
            mStorageType = mPropertyType;
        } else {
            mStorageType = TypeDesc.forClass(storageType);
        }
        mFromStorage = fromStorage;
        mToStorage = toStorage;
    }

    public String getPropertyName() {
        return mProp.getName();
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

    public boolean isDerived() {
        return mProp.isDerived();
    }

    public Method getFromStorageAdapter() {
        return mFromStorage;
    }

    public Method getToStorageAdapter() {
        return mToStorage;
    }

    public String getReadMethodName() {
        return mProp.getReadMethodName();
    }

    public void addInvokeReadMethod(CodeAssembler a) {
        a.invoke(mProp.getReadMethod());
    }

    public void addInvokeReadMethod(CodeAssembler a, TypeDesc instanceType) {
        Class clazz = instanceType.toClass();
        if (clazz == null) {
            // Can't know if instance should be invoked as an interface or as a
            // virtual method.
            throw new IllegalArgumentException("Instance type has no known class");
        }
        if (clazz.isInterface()) {
            a.invokeInterface(instanceType, getReadMethodName(), getPropertyType(), null);
        } else {
            a.invokeVirtual(instanceType, getReadMethodName(), getPropertyType(), null);
        }
    }

    public String getWriteMethodName() {
        return mProp.getWriteMethodName();
    }

    public void addInvokeWriteMethod(CodeAssembler a) {
        a.invoke(mProp.getWriteMethod());
    }

    public void addInvokeWriteMethod(CodeAssembler a, TypeDesc instanceType) {
        Class clazz = instanceType.toClass();
        if (clazz == null) {
            // Can't know if instance should be invoked as an interface or as a
            // virtual method.
            throw new IllegalArgumentException("Instance type has no known class");
        }
        if (clazz.isInterface()) {
            a.invokeInterface(instanceType,
                              getWriteMethodName(), null, new TypeDesc[] {getPropertyType()});
        } else {
            a.invokeVirtual(instanceType,
                            getWriteMethodName(), null, new TypeDesc[] {getPropertyType()});
        }
    }

    @Override
    public String toString() {
        return mProp.toString();
    }
}
