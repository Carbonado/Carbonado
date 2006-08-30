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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Information about a {@link com.amazon.carbonado.constraint.ConstraintDefinition
 * constraint} annotation applied to a property.
 *
 * @author Brian S O'Neill
 */
public class StorablePropertyConstraint {
    private final StorablePropertyAnnotation mAnnotation;
    private final Constructor mConstructor;
    private final Method mConstrainMethod;

    StorablePropertyConstraint(StorablePropertyAnnotation annotation,
                               Constructor ctor,
                               Method constrainMethod)
    {
        mAnnotation = annotation;
        mConstructor = ctor;
        mConstrainMethod = constrainMethod;
    }

    /**
     * Returns the annotation that applied this constraint.
     */
    public StorablePropertyAnnotation getAnnotation() {
        return mAnnotation;
    }

    /**
     * Returns the constructor for the constraint class. It has the signature
     * <code>(Class type, String propertyName, <i>Annotation</i>)</code>, where
     * <i>Annotation</i> is the fully resolved annotation.
     */
    public Constructor getConstraintConstructor() {
            return mConstructor;
    }

    /**
     * Returns the best matching property checking method in the validator.
     */
    public Method getConstrainMethod() {
        return mConstrainMethod;
    }
}
