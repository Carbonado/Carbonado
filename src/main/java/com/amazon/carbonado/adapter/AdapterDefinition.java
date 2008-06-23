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

package com.amazon.carbonado.adapter;

import java.lang.annotation.*;

/**
 * Allows annotations to be defined for supporting property types which are not
 * natively supported by the underlying storage layer. Repositories must always
 * attempt to match property types to the best matching native type, but they
 * may have to rely on an adapter to make a conversion.
 *
 * <p>The annotation is just a pointer to an adapter implementation class. If
 * the adapter class is not explicitly provided, it defaults to a static inner
 * class named "Adapter" in the annotation itself.
 *
 * <p>The adapter class must have a public constructor that accepts the
 * annotation that has the AdapterDefinition annotation. It must also define
 * several adapt methods which convert property values. An adapt method needs
 * to start with "adapt", accept one parameter and return something.
 * <p>
 * Example true/false adapter for booleans:
 * <pre>
 * &#64;Documented
 * <b>&#64;Retention(RetentionPolicy.RUNTIME)</b>
 * <b>&#64;Target(ElementType.METHOD)</b>
 * <b>&#64;AdapterDefinition</b>
 * public &#64;interface TrueFalseAdapter {
 *
 *     public static class Adapter {
 *         private final String propertyName;
 *
 *         // Constructor may throw a MalformedTypeException if
 *         // params supplied by annotation are illegal.
 *
 *         /**
 *          * @param type optional type of object that contains the adapted property
 *          * @param propertyName name of property with adapter
 *          * @param annotation specific annotation that binds to this adapter class
 *          *&#47;
 *         public Adapter(Class type, String propertyName, TrueFalseAdapter annotation) {
 *             this.propertyName = propertyName;
 *         }
 *
 *         // Define at least two adapt methods for each supported property type.
 *
 *         /**
 *          * @param propertyValue value to convert from
 *          *&#47;
 *         public char adaptToChar(boolean propertyValue) {
 *             return value ? 'T' : 'F';
 *         }
 *
 *         /**
 *          * @param propertyValue value to convert from
 *          *&#47;
 *         public boolean adaptToBoolean(char propertyValue) {
 *             if (propertyValue == 'T') { return true; };
 *             if (propertyValue == 'F') { return false; };
 *             throw new IllegalArgumentException
 *                 ("Cannot adapt '" + value + "' into boolean for property \"" +
 *                   propertyName + '"');
 *         }
 *     }
 * }
 * </pre>
 *
 * The newly defined adapter can be applied to property accessors.
 *
 * <pre>
 * public interface UserInfo extends Storable {
 *     <b>&#64;TrueFalseAdapter</b>
 *     boolean isAdministrator();
 *     void setAdministrator(boolean admin);
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface AdapterDefinition {
    /**
     * Specify class which will perform property adaptation. Must have a public
     * constructor with the signature
     * <code>(Class type, String propertyName, <i>Annotation</i>)</code>,
     * where <code><i>Annotation</i></code> refers to the annotation with the
     * adapter definition.
     *
     * <p>The implementation class need not be explicitly specified. By
     * default, the adapter class must be a static inner class of the
     * annotation, named "Adapter".
     */
    // Use void.class to mean default.
    Class implementation() default void.class;

    /**
     * Optionally specify the set of preferred storage types for storing the
     * adapted property, in order of most preferred to least preferred. A type
     * in the set must be supported by the adapt methods to be considered.
     *
     * <p>If the repository is independent, it needs help on deciding exactly
     * how to store the adapted property. A dependent repository will not have
     * as much flexibility in selecting an appropriate type, but it may still
     * need a hint.
     */
    Class[] storageTypePreferences() default {};
}
