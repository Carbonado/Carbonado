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

package com.amazon.carbonado.constraint;

import java.lang.annotation.*;

/**
 * Allows annotations to be defined that restrict property values. The
 * annotation is just a pointer to a constraint checking class. If the
 * constraint class is not explicitly provided, it defaults to a static inner
 * class named "Constraint" in the annotation itself.
 *
 * <p>The constraint class must have a public constructor that accepts the
 * annotation that has the ConstraintDefinition annotation. It must also define
 * several "constrain" methods which perform constraint checks on specific
 * property types.
 * <p>
 * Example integer constraint:
 * <pre>
 * &#64;Documented
 * <b>&#64;Retention(RetentionPolicy.RUNTIME)</b>
 * <b>&#64;Target(ElementType.METHOD)</b>
 * <b>&#64;ConstraintDefinition</b>
 * public &#64;interface IntegerConstraint {
 *     int min() default Integer.MIN_VALUE;
 *
 *     int max() default Integer.MAX_VALUE;
 *
 *     public static class Constraint {
 *         private final String propertyName;
 *         private final int min;
 *         private final int max;
 *
 *         // Constructor may throw a MalformedTypeException if
 *         // params supplied by annotation are illegal.
 *
 *         /**
 *          * @param type optional type of object that contains the constrained property
 *          * @param propertyName name of property with constraint
 *          * @param annotation specific annotation that binds to this constraint class
 *          *&#47;
 *         public Constraint(Class type, String propertyName, IntegerConstraint annotation) {
 *             this.propertyName = propertyName;
 *             this.min = annotation.min();
 *             this.max = annotation.max();
 *         }
 *
 *         // Define a constrain method for each supported property type.
 *
 *         /**
 *          * @param propertyValue specific value to constrain
 *          *&#47;
 *         public void constrain(int propertyValue) throws IllegalArgumentException {
 *             if (propertyValue < min || propertyValue > max) {
 *                 throw new IllegalArgumentException
 *                     ("Value for \"" + propertyName + "\" must be in range " +
 *                      min + ".." + max + ": " + propertyValue);
 *             }
 *         }
 *     }
 * }
 * </pre>
 *
 * The newly defined integer constraint can be applied to property mutators.
 *
 * <pre>
 * public interface UserInfo extends Storable {
 *     ...
 *
 *     int getAge();
 *     // Constraint is called before setting age.
 *     <b>&#64;IntegerConstraint(min=0, max=120)</b>
 *     void setAge(int value);
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface ConstraintDefinition {
    /**
     * Specify class which will perform constraint checking. Must have a public
     * constructor with the signature
     * <code>(Class type, String propertyName, <i>Annotation</i>)</code>,
     * where <code><i>Annotation</i></code> refers to the annotation with the
     * constraint definition.
     *
     * <p>The implementation class need not be explicitly specified. By
     * default, the constraint class must be a static inner class of the
     * annotation, named "Constraint".
     */
    // Use void.class to mean default.
    Class implementation() default void.class;
}
