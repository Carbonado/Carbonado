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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Information about a custom annotation applied to a property.
 *
 * @author Brian S O'Neill
 */
public class StorablePropertyAnnotation {
    private final Annotation mAnnotation;
    private final Class<? extends Annotation> mAnnotationType;
    private final Method mMethod;

    /**
     * Use this constructor if an annotation was actually defined.
     *
     * @param annotation annotation on method
     * @param method method with annotation
     */
    public StorablePropertyAnnotation(Annotation annotation, Method method) {
        if (annotation == null || method == null) {
            throw new IllegalArgumentException();
        }
        mAnnotation = annotation;
        mAnnotationType = annotation.annotationType();
        mMethod = method;
    }

    /**
     * Use this constructor if an annotation was not defined, but instead is
     * being automatically applied.
     *
     * @param annotationType annotation type on method
     * @param method method with annotation
     */
    public StorablePropertyAnnotation(Class<? extends Annotation> annotationType, Method method) {
        if (annotationType == null || method == null) {
            throw new IllegalArgumentException();
        }
        mAnnotation = method.getAnnotation(annotationType);
        mAnnotationType = annotationType;
        mMethod = method;
    }

    /**
     * Returns the actual annotation instance, which may be null if annotation
     * was automatically applied.
     */
    public Annotation getAnnotation() {
        return mAnnotation;
    }

    /**
     * Returns the type of annotation that was applied to the property method.
     */
    public Class<? extends Annotation> getAnnotationType() {
        return mAnnotationType;
    }

    /**
     * Returns the method that has the annotation.
     */
    public Method getAnnotatedMethod() {
        return mMethod;
    }
}
