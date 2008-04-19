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

package com.amazon.carbonado;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Map;

/**
 * A data access object in a {@link Repository}. User defined storables must
 * either extend or implement this interface via an interface or abstract
 * class. Abstract bean properties defined in the storable are persisted into
 * the repository. At least one property must be annotated as the {@link
 * PrimaryKey}. At most one property may be annotated as being the {@link
 * Version} property.
 *
 * <p>Storable instances are mutable, but they must be thread-safe. Although
 * race conditions are possible if multiple threads are mutating the Storable,
 * the Storable instance will not get into a corrupt state.
 *
 * @author Brian S O'Neill
 * @author Don Schneider
 *
 * @see com.amazon.carbonado.Alias
 * @see com.amazon.carbonado.Indexes
 * @see com.amazon.carbonado.Join
 * @see com.amazon.carbonado.Nullable
 * @see com.amazon.carbonado.PrimaryKey
 * @see com.amazon.carbonado.Version
 */
public interface Storable<S extends Storable<S>> {
    /**
     * Loads or reloads this object from the storage layer by a primary or
     * alternate key. All properties of a key must be initialized for it to be
     * chosen. The primary key is examined first, and if not fully initialized,
     * alternate keys are examined in turn.
     *
     * <p>If load is successful, altering the primary key is no longer allowed
     * unless a call to delete succeeds. Attempting to alter the primary key in
     * this state results in an {@link IllegalStateException}. Alternate keys
     * may always be modified, however.
     *
     * <p>Note: This method differs from {@link #tryLoad} only in that it
     * throws an exception if no matching record was found, instead of returning
     * false. This may indicate that the underlying record was deleted between
     * a load and reload. When a FetchNoneException is thrown, this object's
     * state will be the same as if the delete method was called on it.
     *
     * @throws FetchNoneException if no matching record found
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified
     */
    void load() throws FetchNoneException, FetchException;

    /**
     * Loads or reloads this object from the storage layer by a primary or
     * alternate key. All properties of a key must be initialized for it to be
     * chosen. The primary key is examined first, and if not fully initialized,
     * alternate keys are examined in turn.
     *
     * <p>If load is successful, altering the primary key is no longer allowed
     * unless a call to delete succeeds. Attempting to alter the primary key in
     * this state results in an {@link IllegalStateException}. Alternate keys
     * may always be modified, however.
     *
     * <p>Note: This method differs from {@link #load} only in that it returns
     * false if no matching record was found, instead of throwing an exception.
     * This may indicate that the underlying record was deleted between a load
     * and reload. When false is returned, this object's state will be the same
     * as if the delete method was called on it.
     *
     * @return true if found and loaded, false otherwise
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified
     */
    boolean tryLoad() throws FetchException;

    /**
     * Inserts a new persistent value for this object. If successful, altering
     * the primary key is no longer allowed unless a call to delete succeeds.
     * Attempting to alter the primary key in this state results in an {@link
     * IllegalStateException}. Alternate keys may always be modified, however.
     *
     * <p>Insert requires that all primary key properties be specified. If not,
     * an {@link IllegalStateException} is thrown. Also, repository
     * implementations usually require that properties which are not {@link
     * Nullable} also be specified. Otherwise, a {@link ConstraintException}
     * may be thrown.
     *
     * <p>Note: This method differs from {@link #tryInsert} only in that it may
     * throw a UniqueConstraintException, instead of returning false.
     *
     * @throws UniqueConstraintException if it is absolutely known that a key
     * of inserted object matches an existing one
     * @throws ConstraintException if any required properties are unspecified
     * @throws PersistException if storage layer throws an exception
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified
     */
    void insert() throws PersistException;

    /**
     * Inserts a new persistent value for this object. If successful, altering
     * the primary key is no longer allowed unless a call to delete succeeds.
     * Attempting to alter the primary key in this state results in an {@link
     * IllegalStateException}. Alternate keys may always be modified, however.
     *
     * <p>Insert requires that all primary key properties be specified. If not,
     * an {@link IllegalStateException} is thrown. Also, repository
     * implementations usually require that properties which are not {@link
     * Nullable} also be specified. Otherwise, a {@link ConstraintException}
     * may be thrown.
     *
     * <p>Note: This method differs from {@link #insert} only in that it
     * returns false, instead of throwing a UniqueConstraintException.
     *
     * @return false if it is absolutely known that a key of inserted object
     * matches an existing one
     * @throws ConstraintException if any required properties are unspecified
     * @throws PersistException if storage layer throws an exception
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified
     */
    boolean tryInsert() throws PersistException;

    /**
     * Updates the persistent value of this object, regardless of whether this
     * object has actually been loaded or not. If successful, altering the
     * primary key is no longer allowed unless a call to delete succeeds.
     * Attempting to alter the primary key in this state results in an {@link
     * IllegalStateException}. Alternate keys may always be modified, however.
     *
     * <p>If this object has a {@link Version version} property defined, then
     * the update logic is a bit more strict. Updates of any storable require
     * that the primary keys be specified; if a version is present, the version
     * must be specified as well. If any of the primary key or version
     * properties are unspecified, an {@link IllegalStateException} will be
     * thrown; if they are fully specified and the version doesn't match the
     * current record, an {@link OptimisticLockException} is thrown.
     *
     * <p>Not all properties need to be set on this object when calling
     * update. Setting a subset results in a partial update. After a successful
     * update, all properties are set to the actual values in the storage
     * layer. Put another way, the object is automatically reloaded after a
     * successful update.
     *
     * <p>If PersistNoneException is thrown, this indicates that the underlying
     * record was deleted. When this happens, this object's state will be the
     * same as if the delete method was called on it.
     *
     * @throws PersistNoneException if record is missing and no update occurred
     * @throws PersistException if storage layer throws an exception
     * @throws OptimisticLockException if a version property exists and the
     * optimistic lock failed
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified, or if a version property is unspecified
     */
    void update() throws PersistException;

    /**
     * Updates the persistent value of this object, regardless of whether this
     * object has actually been loaded or not. If successful, altering the
     * primary key is no longer allowed unless a call to delete succeeds.
     * Attempting to alter the primary key in this state results in an {@link
     * IllegalStateException}. Alternate keys may always be modified, however.
     *
     * <p>If this object has a {@link Version version} property defined, then
     * the update logic is a bit more strict. Updates of any storable require
     * that the primary keys be specified; if a version is present, the version
     * must be specified as well. If any of the primary key or version
     * properties are unspecified, an {@link IllegalStateException} will be
     * thrown; if they are fully specified and the version doesn't match the
     * current record, an {@link OptimisticLockException} is thrown.
     *
     * <p>Not all properties need to be set on this object when calling
     * update. Setting a subset results in a partial update. After a successful
     * update, all properties are set to the actual values in the storage
     * layer. Put another way, the object is automatically reloaded after a
     * successful update.
     *
     * <p>A return value of false indicates that the underlying record was
     * deleted. When this happens, this object's state will be the same as if
     * the delete method was called on it.
     *
     * @return true if record likely exists and was updated, or false if record
     * absolutely no longer exists and no update occurred
     * @throws PersistException if storage layer throws an exception
     * @throws OptimisticLockException if a version property exists and the
     * optimistic lock failed
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified, or if a version property is unspecified
     */
    boolean tryUpdate() throws PersistException;

    /**
     * Deletes this object from the storage layer by its primary key,
     * regardless of whether this object has actually been loaded or not.
     * Calling delete does not prevent this object from being used again. All
     * property values are still valid, including the primary key. Once
     * deleted, the insert operation is permitted again.
     *
     * <p>Note: This method differs from {@link #tryDelete} only in that it may
     * throw a PersistNoneException, instead of returning false.
     *
     * @throws PersistNoneException if record is missing and nothing was
     * deleted
     * @throws PersistException if storage layer throws an exception
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified
     */
    void delete() throws PersistException;

    /**
     * Deletes this object from the storage layer by its primary key,
     * regardless of whether this object has actually been loaded or not.
     * Calling delete does not prevent this object from being used again. All
     * property values are still valid, including the primary key. Once
     * deleted, the insert operation is permitted again.
     *
     * <p>Note: This method differs from {@link #delete} only in that it
     * returns false, instead of throwing a PersistNoneException.
     *
     * @return true if record likely existed and was deleted, or false if record
     * absolutely no longer exists and no delete was necessary
     * @throws PersistException if storage layer throws an exception
     * @throws IllegalStateException if the state of this instance suggests
     * that any primary keys are unspecified
     */
    boolean tryDelete() throws PersistException;

    /**
     * Returns the class or interface from which this storable was
     * generated. This represents the data class for the storable.
     *
     * <p><i>Design note: the name "getStorableType" is avoided, so as not to
     * conflict with a user defined property of "storableType"</i>
     */
    Class<S> storableType();

    /**
     * Copies all supported properties, skipping any that are uninitialized.
     * Specifically, calls {@literal "target.set<property>"} for all supported
     * properties in this storable, passing the value of the property from this
     * object. Unsupported {@link Independent independent} properties in this
     * or the target are not copied.
     *
     * @param target storable on which to call {@literal set<property>} methods
     * @throws IllegalStateException if any primary key properties of target
     * cannot be altered
     */
    void copyAllProperties(S target);

    /**
     * Copies all supported primary key properties, skipping any that are
     * uninitialized. Specifically, calls {@literal "target.set<property>"} for all
     * supported properties which participate in the primary key, passing the
     * value of the property from this object. Unsupported {@link Independent
     * independent} properties in this or the target are not copied.
     *
     * @param target storable on which to call {@literal set<property>} methods
     * @throws IllegalStateException if any primary key properties of target
     * cannot be altered
     */
    void copyPrimaryKeyProperties(S target);

    /**
     * Copies the optional version property, unless it is uninitialized.
     * Specifically, calls {@literal "target.set<property>"} for the version
     * property (if supported), passing the value of the property from this
     * object. If no version property is defined, then this method does
     * nothing. Unsupported {@link Independent independent} properties in this
     * or the target are not copied.
     *
     * @param target storable on which to call {@literal set<property>} method
     */
    void copyVersionProperty(S target);

    /**
     * Copies all supported non-primary key properties which are unequal,
     * skipping any that are uninitialized. Specifically, calls
     * {@literal "target.get<property>"}, and if the value thus retrieved differs
     * from the local value, {@literal "target.set<property>"} is called for that
     * property. Unsupported {@link Independent independent} properties in this
     * or the target are not copied.
     *
     * @param target storable on which to call {@literal set<property>} methods
     */
    void copyUnequalProperties(S target);

    /**
     * Copies all supported non-primary key properties which are
     * dirty. Specifically, calls {@literal "target.set<property>"} for any
     * non-primary key property which is dirty, passing the value of the
     * property from this object. A property is considered dirty when set
     * before a load or persist operation is called. Unsupported {@link
     * Independent independent} properties in this or the target are not
     * copied.
     *
     * @param target storable on which to call {@literal set<property>} methods
     */
    void copyDirtyProperties(S target);

    /**
     * Returns true if any non-primary key properties in this object are
     * dirty. A property is considered dirty when set before a load or persist
     * operation is called. A property becomes clean after a successful load,
     * insert, or update operation.
     */
    boolean hasDirtyProperties();

    /**
     * Marks all dirty properties as clean. Uninitialized properties remain so.
     * As a side-effect, initialized primary keys may no longer be altered.
     */
    void markPropertiesClean();

    /**
     * Marks all properties as clean, including uninitialized properties.
     * As a side-effect, primary keys may no longer be altered.
     */
    void markAllPropertiesClean();

    /**
     * Marks all clean properties as dirty. Uninitialized properties remain so.
     * As a side-effect, primary keys can be altered.
     */
    void markPropertiesDirty();

    /**
     * Marks all properties as dirty, including uninitialized properties.
     * As a side-effect, primary keys can be altered.
     */
    void markAllPropertiesDirty();

    /**
     * Returns true if the given property of this Storable has never been
     * loaded or set.
     *
     * @param propertyName name of property to interrogate
     * @throws IllegalArgumentException if property is unknown, is a join or is derived
     */
    boolean isPropertyUninitialized(String propertyName);

    /**
     * Returns true if the given property of this Storable has been set, but no
     * load or store operation has been performed yet.
     *
     * @param propertyName name of property to interrogate
     * @throws IllegalArgumentException if property is unknown, is a join or is derived
     */
    boolean isPropertyDirty(String propertyName);

    /**
     * Returns true if the given property of this Storable is clean. All
     * properties are clean after a successful load or store operation.
     *
     * @param propertyName name of property to interrogate
     * @throws IllegalArgumentException if property is unknown, is a join or is derived
     */
    boolean isPropertyClean(String propertyName);

    /**
     * Returns true if the given property exists and is supported. If a
     * Storable has an {@link Independent} property which is not supported by
     * the repository, then this method returns false.
     *
     * @param propertyName name of property to check
     */
    boolean isPropertySupported(String propertyName);

    /**
     * Returns a Storable property value by name.
     *
     * @param propertyName name of property to get value of
     * @return property value, which is boxed if property type is primitive
     * @throws IllegalArgumentException if property is unknown or if accessor
     * method declares throwing any checked exceptions
     * @throws UnsupportedOperationException if property is independent but unsupported
     * @throws NullPointerException if property name is null
     * @since 1.2
     */
    Object getPropertyValue(String propertyName);

    /**
     * Sets a Storable property value by name. Call insert or update to persist
     * the change.
     *
     * @param propertyName name of property to set value to
     * @param value new value for property
     * @throws IllegalArgumentException if property is unknown, or if value is
     * unsupported due to a constraint, or if mutator method declares throwing
     * any checked exceptions
     * @throws UnsupportedOperationException if property is independent but unsupported
     * @throws ClassCastException if value is of wrong type
     * @throws NullPointerException if property name is null or if primitive
     * value is required but value is null
     * @since 1.2
     */
    void setPropertyValue(String propertyName, Object value);

    /**
     * Returns a fixed-size map view of this Storable's properties. Properties
     * which declare throwing any checked exceptions are excluded from the
     * map. Removing and adding of map entries is unsupported.
     *
     * @return map of property name to property value; primitive property
     * values are boxed
     * @since 1.2
     */
    Map<String, Object> propertyMap();

    /**
     * Returns an exact shallow copy of this object, including the state.
     */
    S copy();

    /**
     * Prepares a new object for loading, inserting, updating, or deleting.
     *
     * @see Storage#prepare
     * @since 1.2
     */
    S prepare();

    /**
     * Serializes property values and states for temporary storage or for
     * network transfer. Call {@link #readFrom} to restore. Derived and join
     * properties are not serialized.
     *
     * <p>The encoding used by this method is much simpler than what is
     * provided by standard object serialization. It does not encode class info
     * or property names, which is why it is not suitable for long term
     * storage.
     *
     * @throws IOException if exception from stream
     * @throws SupportException if Storable cannot be serialized
     * @since 1.2
     */
    void writeTo(OutputStream out) throws IOException, SupportException;

    /**
     * Restores property values and states as encoded by {@link #writeTo}.
     * Derived properties are not directly modified, but all other properties
     * not restored are reset to their initial state.
     *
     * @throws IOException if exception from stream
     * @throws SupportException if Storable cannot be serialized
     * @since 1.2
     */
    void readFrom(InputStream in) throws IOException, SupportException;

    int hashCode();

    /**
     * True if all properties and fields are equal, but ignoring the state.
     *
     * @param obj object to compare to for equality
     */
    boolean equals(Object obj);

    /**
     * True if the supported properties which participate in the primary key
     * are equal. This is useful to cheaply investigate if two storables refer
     * to the same entity, regardless of the state of object (specifically the
     * non-key properties). Unsupported {@link Independent independent}
     * properties in this or the target are not compared.
     *
     * @param obj object to compare to for equality
     */
    boolean equalPrimaryKeys(Object obj);

    /**
     * True if all supported properties for this object are equal. Unsupported
     * {@link Independent independent} properties in this or the target are not
     * compared.
     *
     * @param obj object to compare to for equality
     */
    boolean equalProperties(Object obj);

    /**
     * Returns a string for debugging purposes that contains all supported
     * property names and values for this object. Uninitialized and unsupported
     * {@link Independent independent} properties are not included.
     */
    String toString();

    /**
     * Returns a string for debugging purposes that contains supported key
     * property names and values for this object. Uninitialized and unsupported
     * {@link Independent independent} properties are not included.
     */
    String toStringKeyOnly();
}
