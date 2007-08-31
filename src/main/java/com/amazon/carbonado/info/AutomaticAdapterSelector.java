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

import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import org.cojen.util.BeanProperty;

import com.amazon.carbonado.adapter.DateTimeAdapter;
import com.amazon.carbonado.adapter.TextAdapter;

/**
 * Some property types are not expected to be supported by most repositories,
 * except a standard adapter can be applied automatically. Since no annotation
 * is present in the storable definition, selected StorablePropertyAdapter
 * instances return null from getAnnotation. Automatically selected adapters
 * must cope without having an annotation instance, applying defaults.
 *
 * @author Brian S O'Neill
 */
class AutomaticAdapterSelector {
    /**
     * @param property bean property which must have a read method
     * @return adapter with a null annotation, or null if nothing applicable
     */
    static StorablePropertyAdapter selectAdapterFor(final BeanProperty property) {
        final Method readMethod = property.getReadMethod();
        if (readMethod == null) {
            throw new IllegalArgumentException();
        }
        final Class propertyType = property.getType();

        if (DateTime.class.isAssignableFrom(propertyType) ||
            DateMidnight.class.isAssignableFrom(propertyType) ||
            LocalDate.class.isAssignableFrom(propertyType) ||
            LocalDateTime.class.isAssignableFrom(propertyType) ||
            java.util.Date.class.isAssignableFrom(propertyType))
        {
            return selectAdapter(property, DateTimeAdapter.class, readMethod);
        } else if (String.class.isAssignableFrom(propertyType)) {
            return selectAdapter(property, TextAdapter.class, readMethod);
        } // else if ...

        return null;
    }

    private static StorablePropertyAdapter selectAdapter
        (BeanProperty property,
         Class<? extends Annotation> annotationType,
         Method annotatedMethod)
    {
        StorablePropertyAnnotation annotation =
            new StorablePropertyAnnotation(annotationType, annotatedMethod);
        return new StorablePropertyAdapter(property, annotation);
    }
}
