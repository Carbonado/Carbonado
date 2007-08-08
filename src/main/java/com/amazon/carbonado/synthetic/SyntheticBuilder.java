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

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

/**
 * A synthetic builder provides a mechanism for creating a user storable from scratch.
 * The client creates the builder, decorates with properties and indexes on those
 * properties, then builds.
 *
 * If additional, ad hoc decoration is desired, the partially constructed classfile
 * can be retrieved and operated on directly via the ClassFileBuilder
 * returned by {@link #prepare}.
 *
 * @author Don Schneider
 * @author David Rosenstrauch
 */
public interface SyntheticBuilder {


    /**
     * @return {@link ClassFileBuilder} ready for further decoration or building
     * @throws SupportException
     */
    public ClassFileBuilder prepare() throws SupportException;

    /**
     * @return the generated class file for this builder.  Note that
     * proper operation requires that {@link #prepare()} already have been called
     * prior to calling this method.
     * @throws IllegalStateException if build has not yet been called.
     */
    public Class<? extends Storable> getStorableClass() throws IllegalStateException;

    /**
     * Convenience method to generate the class.
     * Build will always call {@link #prepare()} and return the result of
     * generating the class from that classfile.  If the caller does not
     * wish to regenerate the class from scratch, use {@link #getStorableClass()} instead.
     */
    public Class<? extends Storable> build() throws SupportException;

    /**
     * Add a property to the set managed by this builder.
     * @param name of the property
     * @param type of the property
     * @return property specification which can be further refined
     */
    public SyntheticProperty addProperty(String name, Class type);

    /**
     * Add an externally defined synthetic property to the list
     * @param prop to add
     * @return original synthetic property as a convenience
     */
    public SyntheticProperty addProperty(SyntheticProperty prop);

    /**
     * Check to see if a particular property has already been added to the list of
     * properties to generate
     * @param name
     */
    public boolean hasProperty(String name);

    /**
     * Add a primary key to be built.
     * @return key to be decorated with property values defining the primary key
     */
    public SyntheticKey addPrimaryKey();

    /**
     * Add an alternate key to be built.
     * @return key to be decorated with property values defining the alternate key
     * @since 1.2
     */
    public SyntheticKey addAlternateKey();

    /**
     * Add an index to the set managed by this builder.  All indexes added this
     * way will be in addition to the primary and alternate key indexes.
     * @return index to be decorated with property values defining the index
     */
    public SyntheticIndex addIndex();


    /**
     * Returns true if a property with the version attribute has been addded
     */
    public boolean isVersioned();

    /**
     * Interface used to get the name for the class to generate. This allows the
     * client to apply different rules for classname generation.
     */
    public interface ClassNameProvider {
        public String getName();

        /**
         * SyntheticBuilder may choose to alter the class name to prevent a
         * class name collision. When explicit is true, the class name must not
         * be altered.
         */
        public boolean isExplicit();
    }
}
