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

package com.amazon.carbonado.stored;

import com.amazon.carbonado.*;

/**
 * Implements all the Storable methods, but each throws
 * UnsupportedOperationException. Methods defined in Object are left alone.
 *
 * @author Brian S O'Neill
 */
public class Dummy implements Storable {
    public void load() throws FetchException {
        throw error();
    }

    public boolean tryLoad() throws FetchException {
        throw error();
    }

    public void insert() throws PersistException {
        throw error();
    }

    public boolean tryInsert() throws PersistException {
        throw error();
    }

    public void update() throws PersistException {
        throw error();
    }

    public boolean tryUpdate() throws PersistException {
        throw error();
    }

    public void delete() throws PersistException {
        throw error();
    }

    public boolean tryDelete() throws PersistException {
        throw error();
    }

    public Storage storage() {
        throw error();
    }

    public Class storableType() {
        throw error();
    }

    public void copyAllProperties(Storable target) {
        throw error();
    }

    public void copyPrimaryKeyProperties(Storable target) {
        throw error();
    }

    public void copyVersionProperty(Storable target) {
        throw error();
    }

    public void copyUnequalProperties(Storable target) {
        throw error();
    }

    public void copyDirtyProperties(Storable target) {
        throw error();
    }

    public boolean hasDirtyProperties() {
        throw error();
    }

    public void markPropertiesClean() {
        throw error();
    }

    public void markAllPropertiesClean() {
        throw error();
    }

    public void markPropertiesDirty() {
        throw error();
    }

    public void markAllPropertiesDirty() {
        throw error();
    }

    public boolean isPropertyUninitialized(String propertyName) {
        throw error();
    }

    public boolean isPropertyDirty(String propertyName) {
        throw error();
    }

    public boolean isPropertyClean(String propertyName) {
        throw error();
    }

    public boolean isPropertySupported(String propertyName) {
        throw error();
    }

    public Storable copy() {
        throw error();
    }

    public boolean equalPrimaryKeys(Object obj) {
        throw error();
    }

    public boolean equalProperties(Object obj) {
        throw error();
    }

    public String toStringKeyOnly() {
        throw error();
    }

    protected UnsupportedOperationException error() {
        return new UnsupportedOperationException();
    }
}
