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
package com.amazon.carbonado.synthetic;

import org.cojen.util.BeanPropertyAccessor;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

/**
 * Simple wrapper to allow a storable to be accessed via property names rather than
 * an explicit interface definition.  This is generally expected to be used for accessing
 * synthetic storables, but could certainly be used with standard storables if the need
 * arises.
 *
 * @author Don Schneider
 */
public class StorableBean<S extends Storable<S>> implements Storable<S> {
    private final S mProxy;
    private BeanPropertyAccessor mAccessor;

    /**
     * Wrap a storable
     * @param s
     */
    public StorableBean(S s) {
        mProxy = s;
    }

    /**
     * Retrieve a value from the storable by property name
     * @param propName name of the property to retrieve
     */
    public Object getValue(String propName) {
        return getAccessor().getPropertyValue(mProxy, propName);
    }

    /**
     * Set a value into the storable by property name
     * @param propName name of the property to set
     * @param value new value for the property
     */
    public void setValue(String propName, Object value) {
        getAccessor().setPropertyValue(mProxy, propName, value);
    }

    /**
     * @return the unwrapped storable
     */
    public S getStorable() {
        return mProxy;
    }

    public void load() throws FetchException {
        mProxy.load();
    }

    public boolean tryLoad() throws FetchException {
        return mProxy.tryLoad();
    }

    public void insert() throws PersistException {
        mProxy.insert();
    }

    public boolean tryInsert() throws PersistException {
        return mProxy.tryInsert();
    }

    public void update() throws PersistException {
        mProxy.update();
    }

    public boolean tryUpdate() throws PersistException {
        return mProxy.tryUpdate();
    }

    public void delete() throws PersistException {
        mProxy.delete();
    }

    public boolean tryDelete() throws PersistException {
        return mProxy.tryDelete();
    }

    public Class<S> storableType() {
        return mProxy.storableType();
    }

    public void copyAllProperties(S target) {
        mProxy.copyAllProperties(target);
    }

    public void copyDirtyProperties(S target) {
        mProxy.copyDirtyProperties(target);
    }

    public void copyPrimaryKeyProperties(S target) {
        mProxy.copyPrimaryKeyProperties(target);
    }

    public void copyVersionProperty(S target) {
        mProxy.copyVersionProperty(target);
    }

    public void copyUnequalProperties(S target) {
        mProxy.copyUnequalProperties(target);
    }

    public boolean hasDirtyProperties() {
        return mProxy.hasDirtyProperties();
    }

    public void markPropertiesClean() {
        mProxy.markPropertiesClean();
    }

    public void markAllPropertiesClean() {
        mProxy.markAllPropertiesClean();
    }

    public void markPropertiesDirty() {
        mProxy.markPropertiesDirty();
    }

    public void markAllPropertiesDirty() {
        mProxy.markAllPropertiesDirty();
    }

    public boolean isPropertyUninitialized(String propertyName) {
        return mProxy.isPropertyUninitialized(propertyName);
    }

    public boolean isPropertyDirty(String propertyName) {
        return mProxy.isPropertyDirty(propertyName);
    }

    public boolean isPropertyClean(String propertyName) {
        return mProxy.isPropertyClean(propertyName);
    }

    public boolean isPropertySupported(String propertyName) {
        return mProxy.isPropertySupported(propertyName);
    }

    public int hashCode() {
        return mProxy.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof Storable) {
            return mProxy.equalProperties(obj);
        }
        return false;
    }

    public boolean equalPrimaryKeys(Object obj) {
        return mProxy.equalPrimaryKeys(obj);
    }

    public boolean equalProperties(Object obj) {
        return mProxy.equalProperties(obj);
    }

    public String toString() {
        return mProxy.toString();
    }

    public String toStringKeyOnly() {
        return mProxy.toStringKeyOnly();
    }

    public S copy() {
        return mProxy.copy();
    }

    private BeanPropertyAccessor getAccessor() {
        if (mAccessor == null) {
            mAccessor = BeanPropertyAccessor.forClass(mProxy.storableType());
        }
        return mAccessor;
    }
}
