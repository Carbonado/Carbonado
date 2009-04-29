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

import java.io.Serializable;
import java.lang.reflect.Method;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.util.Appender;

/**
 * Contains all the metadata describing a property of a specific {@link Storable} type.
 *
 * @author Brian S O'Neill
 * @author Tobias Holgers
 * @see StorableIntrospector
 */
public interface StorableProperty<S extends Storable> extends Serializable, Appender {
    /**
     * Returns the name of this property, which is the same as the bean name
     * unless it has been {@link com.amazon.carbonado.Name renamed}.
     */
    String getName();

    /**
     * Returns the bean name of this property, which is derived from the read
     * and write method names.
     *
     * @since 1.2
     */
    String getBeanName();

    /**
     * Returns the primary type of this property.
     */
    Class<?> getType();

    /**
     * Returns additional types of this property, all of which are assignable
     * by the primary type.
     *
     * @since 1.2.1
     */
    Class<?>[] getCovariantTypes();

    /**
     * Returns the zero-based numerical position of this property within its
     * enclosing type.
     *
     * @since 1.2
     */
    int getNumber();

    /**
     * Returns the enclosing type of this property.
     */
    Class<S> getEnclosingType();

    /**
     * Returns a no-arg method used to read the property value, or null if
     * reading is not allowed. The return type matches the type of this
     * property.
     */
    Method getReadMethod();

    /**
     * Returns the name of the read method, even if no read method was actually
     * declared. That is, this method always returns a method name, but
     * getReadMethod may still return null.
     */
    String getReadMethodName();

    /**
     * Returns a one argument method used to write the property value, or null
     * if writing is not allowed. The first argument is the value to set, which
     * is the type of this property.
     */
    Method getWriteMethod();

    /**
     * Returns the name of the write method, even if no write method was
     * actually declared. That is, this method always returns a method name,
     * but getWriteMethod may still return null.
     */
    String getWriteMethodName();

    /**
     * Returns true if this property can be null.
     *
     * @see com.amazon.carbonado.Nullable
     */
    boolean isNullable();

    /**
     * Returns true if this property is a member of a primary key.
     *
     * @see com.amazon.carbonado.PrimaryKey
     */
    boolean isPrimaryKeyMember();

    /**
     * Returns true if this property is a member of an alternate key.
     *
     * @see com.amazon.carbonado.AlternateKeys
     */
    boolean isAlternateKeyMember();

   /**
     * Returns the count of aliases for this property.
     *
     * @see com.amazon.carbonado.Alias
     */
    int getAliasCount();

    /**
     * Returns a specific alias for this property.
     *
     * @see com.amazon.carbonado.Alias
     */
    String getAlias(int index) throws IndexOutOfBoundsException;

    /**
     * Returns a new array with all the alias names in it.
     *
     * @see com.amazon.carbonado.Alias
     */
    String[] getAliases();

    /**
     * Returns true if this property is joined in any way to another Storable.
     *
     * @see com.amazon.carbonado.Join
     */
    boolean isJoin();

    /**
     * Returns true if this property is one-to-one joined to another Storable.
     *
     * @see com.amazon.carbonado.Join
     * @since 1.2
     */
    boolean isOneToOneJoin();

    /**
     * Returns the type of property this is joined to, or null if not joined.
     */
    Class<? extends Storable> getJoinedType();

    /**
     * Returns the count of properties that participate in this property's
     * join. If this property is not a join, then zero is returned.
     */
    int getJoinElementCount();

    /**
     * Returns a specific property in this property's class that participates
     * in the join.
     */
    StorableProperty<S> getInternalJoinElement(int index) throws IndexOutOfBoundsException;

    /**
     * Returns a new array with all the internal join elements in it.
     */
    StorableProperty<S>[] getInternalJoinElements();

    /**
     * Returns a specific property in the joined class that participates in the
     * join.
     */
    StorableProperty<?> getExternalJoinElement(int index) throws IndexOutOfBoundsException;

    /**
     * Returns a new array with all the external join elements in it.
     */
    StorableProperty<?>[] getExternalJoinElements();

    /**
     * Returns true if this property is a query, which also implies that it is
     * a join property.
     *
     * @see com.amazon.carbonado.Query
     */
    boolean isQuery();

    /**
     * Returns the count of constraints for this property.
     */
    int getConstraintCount();

    /**
     * Returns a specific constraint for this property.
     */
    StorablePropertyConstraint getConstraint(int index) throws IndexOutOfBoundsException;

    /**
     * Returns a new array with all the constraints in it.
     */
    StorablePropertyConstraint[] getConstraints();

    /**
     * Returns this property's adapter, or null if none.
     */
    StorablePropertyAdapter getAdapter();

    /**
     * Returns the property's sequence name, or null if none.
     *
     * @see com.amazon.carbonado.Sequence
     */
    String getSequenceName();

    /**
     * Returns true of this property is given an automatic value upon insert.
     *
     * @see com.amazon.carbonado.Automatic
     * @since 1.2
     */
    boolean isAutomatic();

    /**
     * Returns true if this property is the designated version number for the
     * Storable.
     *
     * @see com.amazon.carbonado.Version
     */
    boolean isVersion();

    /**
     * Returns true if this property has been designated independent.
     *
     * @see com.amazon.carbonado.Independent
     */
    boolean isIndependent();

    /**
     * Returns true if this property is derived.
     *
     * @see com.amazon.carbonado.Derived
     * @since 1.2
     */
    boolean isDerived();

    /**
     * Returns a new array with all the derived-from properties, which is empty
     * if this is not a derived property. Otherwise, the set is the transitive
     * closure of all dependent properties. This set may include joins and
     * other derived properties.
     *
     * @since 1.2
     */
    ChainedProperty<S>[] getDerivedFromProperties();

    /**
     * Returns a new array with all the properties which are derived from this
     * one. The set is the transitive closure of all derived properties which
     * depend on this one.
     *
     * <p>Each property in the set is represented as a chain, where the prime
     * property is the actual dependent property, and the tail is the path to
     * reach this property's enclosing type. If a derived property resides in
     * the same enclosing type as this one, the chain count is zero.
     *
     * @since 1.2
     */
    ChainedProperty<?>[] getDerivedToProperties();

    /**
     * Returns true if this derived property should be included when copying a
     * storable. Copying of a derived property uses the "get" and "set" methods
     * and requires the "set" method to be defined.
     *
     * @since 1.2
     */
    boolean shouldCopyDerived();

    String toString();
}
