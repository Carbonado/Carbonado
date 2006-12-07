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

package com.amazon.carbonado.info;

import java.util.Map;

import com.amazon.carbonado.Storable;

/**
 * Contains all the metadata describing a specific {@link Storable} type.
 *
 * @author Brian S O'Neill
 * @see StorableIntrospector
 */
public interface StorableInfo<S extends Storable> {
    /**
     * Returns the name of the Storable described by this StorableInfo,
     * which is an abbreviated form of the type's class name.
     */
    String getName();

    /**
     * Returns the type of Storable described by this StorableInfo.
     */
    Class<S> getStorableType();

    /**
     * Returns all the storable properties in an unmodifiable map. Properties
     * are always ordered, case-sensitive, by name. Primary key properties are
     * grouped first.
     *
     * @return maps property names to property objects
     */
    Map<String, ? extends StorableProperty<S>> getAllProperties();

    /**
     * Returns a subset of the storable properties in an unmodifiable map
     * that define the primary key. Properties are always ordered,
     * case-sensitive, by name.
     *
     * @return maps property names to property objects
     */
    Map<String, ? extends StorableProperty<S>> getPrimaryKeyProperties();

    /**
     * Returns a subset of the storable properties in an unmodifiable map
     * that define the basic data properties. Primary keys and joins are
     * excluded. Properties are always ordered, case-sensitive, by name.
     *
     * @return maps property names to property objects
     */
    Map<String, ? extends StorableProperty<S>> getDataProperties();

    /**
     * Returns the designated version property, or null if none.
     */
    StorableProperty<S> getVersionProperty();

    /**
     * Returns the primary key for the Storable, never null.
     */
    StorableKey<S> getPrimaryKey();

    /**
     * Returns the count of alternate keys for the Storable.
     */
    int getAlternateKeyCount();

    /**
     * Returns a specific alternate key for the Storable.
     */
    StorableKey<S> getAlternateKey(int index);

    /**
     * Returns a new array with all the alternate keys in it.
     */
    StorableKey<S>[] getAlternateKeys();

    /**
     * Returns the count of aliases for the Storable.
     */
    int getAliasCount();

    /**
     * Returns a specific alias for the Storable.
     */
    String getAlias(int index) throws IndexOutOfBoundsException;

    /**
     * Returns a new array with all the alias names in it.
     */
    String[] getAliases();

    /**
     * Returns the count of indexes defined for the Storable.
     */
    int getIndexCount();

    /**
     * Returns a specific index for the Storable.
     */
    StorableIndex<S> getIndex(int index) throws IndexOutOfBoundsException;

    /**
     * Returns a new array with all the indexes in it.
     */
    StorableIndex<S>[] getIndexes();

    /**
     * @see com.amazon.carbonado.Independent
     */
    boolean isIndependent();

    /**
     * @see com.amazon.carbonado.Authoritative
     */
    boolean isAuthoritative();
}
