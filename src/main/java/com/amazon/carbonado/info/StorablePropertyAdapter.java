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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.cojen.util.BeanProperty;

import com.amazon.carbonado.adapter.AdapterDefinition;
import com.amazon.carbonado.util.ThrowUnchecked;

/**
 * Information about an {@link com.amazon.carbonado.adapter.AdapterDefinition
 * adapter} annotation applied to a property.
 *
 * @author Brian S O'Neill
 */
public class StorablePropertyAdapter {
    static Class getEnclosingType(BeanProperty property) {
        Method m = property.getReadMethod();
        if (m == null) {
            m = property.getWriteMethod();
        }
        return m.getDeclaringClass();
    }

    /**
     * @return null if not found
     */
    static Class findAdapterClass(Class<? extends Annotation> annotationType) {
        AdapterDefinition ad = annotationType.getAnnotation(AdapterDefinition.class);
        if (ad == null) {
            return null;
        }

        Class adapterClass = ad.implementation();

        if (adapterClass == void.class) {
            // Magic value meaning "use default", which is an inner class of
            // the annotation.

            adapterClass = null;

            // Search for inner class named "Adapter".
            Class[] innerClasses = annotationType.getClasses();
            for (Class c : innerClasses) {
                if ("Adapter".equals(c.getSimpleName())) {
                    adapterClass = c;
                    break;
                }
            }
        }

        return adapterClass;
    }

    /**
     * @return empty array if none found
     */
    @SuppressWarnings("unchecked")
    static Method[] findAdaptMethods(Class<?> propertyType, Class<?> adapterClass) {
        List<Method> adaptMethods = new ArrayList<Method>();

        for (Method adaptMethod : adapterClass.getMethods()) {
            if (!adaptMethod.getName().startsWith("adapt")) {
                continue;
            }
            Class<?> toType = adaptMethod.getReturnType();
            if (toType == void.class) {
                continue;
            }
            Class<?>[] paramTypes = adaptMethod.getParameterTypes();
            Class<?> fromType;
            if (paramTypes.length != 1) {
                continue;
            } else {
                fromType = paramTypes[0];
            }

            if (!fromType.isAssignableFrom(propertyType) &&
                !propertyType.isAssignableFrom(toType)) {
                continue;
            }

            adaptMethods.add(adaptMethod);
        }

        return (Method[]) adaptMethods.toArray(new Method[adaptMethods.size()]);
    }

    private final Class mEnclosingType;
    private final String mPropertyName;
    private final StorablePropertyAnnotation mAnnotation;
    private final Class[] mStorageTypePreferences;
    private final Constructor mConstructor;
    private final Method[] mAdaptMethods;

    private transient Object mAdapterInstance;

    /**
     * Construct a generic StorablePropertyAdapter instance not attached to a
     * storable definition. Call {@link StorableProperty#getAdapter} to gain
     * access to adapter information on actual storable definitions.
     *
     * @param propertyName name of property with adapter
     * @param propertyType declated type of adapted property
     * @param adapterType adapter type
     * @throws IllegalArgumentException if adapterType is not an adapter
     * definition.
     */
    public StorablePropertyAdapter(String propertyName,
                                   Class<?> propertyType,
                                   Class<? extends Annotation> adapterType)
    {
        this(null, propertyName, propertyType, null, adapterType);
    }

    /**
     * Used by StorableIntrospector.
     *
     * @see StorableIntrospector
     */
    StorablePropertyAdapter(BeanProperty property,
                            StorablePropertyAnnotation annotation,
                            AdapterDefinition ad,
                            Constructor ctor,
                            Method[] adaptMethods)
    {
        mEnclosingType = getEnclosingType(property);
        mPropertyName = property.getName();
        mAnnotation = annotation;
        mConstructor = ctor;
        mAdaptMethods = adaptMethods;

        Class[] storageTypePreferences = ad.storageTypePreferences();
        if (storageTypePreferences != null && storageTypePreferences.length == 0) {
            storageTypePreferences = null;
        }
        mStorageTypePreferences = storageTypePreferences;
    }

    /**
     * Used with automatic adapter selection.
     *
     * @see AutomaticAdapterSeletor
     */
    StorablePropertyAdapter(BeanProperty property,
                            StorablePropertyAnnotation annotation)
    {
        this(getEnclosingType(property),
             property.getName(),
             property.getType(),
             annotation,
             annotation.getAnnotationType());
    }

    private StorablePropertyAdapter(Class enclosingType,
                                    String propertyName,
                                    Class<?> propertyType,
                                    StorablePropertyAnnotation annotation,
                                    Class<? extends Annotation> adapterType)
    {
        mEnclosingType = enclosingType;
        mPropertyName = propertyName;
        mAnnotation = annotation;

        AdapterDefinition ad = adapterType.getAnnotation(AdapterDefinition.class);
        if (ad == null) {
            throw new IllegalArgumentException();
        }

        Class[] storageTypePreferences = ad.storageTypePreferences();
        if (storageTypePreferences != null && storageTypePreferences.length == 0) {
            storageTypePreferences = null;
        }
        mStorageTypePreferences = storageTypePreferences;

        Class adapterClass = findAdapterClass(adapterType);
        if (adapterClass == null) {
            throw new IllegalArgumentException();
        }

        try {
            mConstructor = adapterClass.getConstructor(Class.class, String.class, adapterType);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

        mAdaptMethods = findAdaptMethods(propertyType, adapterClass);
        if (mAdaptMethods.length == 0) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns the annotation that applied this adapter, or null if none.
     */
    public StorablePropertyAnnotation getAnnotation() {
        return mAnnotation;
    }

    /**
     * Returns the constructor for the adapter class. It has the signature
     * <code>(Class type, String propertyName, <i>Annotation</i>)</code>, where
     * <i>Annotation</i> is the fully resolved annotation.
     */
    public Constructor getAdapterConstructor() {
        return mConstructor;
    }

    /**
     * Returns an instance of the adapter, for which an adapt method is applied to.
     */
    public Object getAdapterInstance() {
        if (mAdapterInstance == null) {
            try {
                mAdapterInstance = mConstructor.newInstance
                    (mEnclosingType, mPropertyName, mAnnotation.getAnnotation());
            } catch (Exception e) {
                ThrowUnchecked.fireFirstDeclaredCause(e);
            }
        }
        return mAdapterInstance;
    }

    /**
     * Returns the adapter's storage type preferences.
     *
     * @see com.amazon.carbonado.adapter.AdapterDefinition#storageTypePreferences
     */
    public Class[] getStorageTypePreferences() {
        if (mStorageTypePreferences == null) {
            return new Class[0];
        }
        return mStorageTypePreferences.clone();
    }

    /**
     * Returns an adapt method that supports the given conversion, or null if
     * none.
     */
    @SuppressWarnings("unchecked")
    public Method findAdaptMethod(Class from, Class to) {
        Method[] methods = mAdaptMethods;
        List<Method> candidates = new ArrayList<Method>(methods.length);
        for (int i=methods.length; --i>=0; ) {
            Method method = methods[i];
            if (to.isAssignableFrom(method.getReturnType()) &&
                method.getParameterTypes()[0].isAssignableFrom(from)) {
                candidates.add(method);
            }
        }
        reduceCandidates(candidates, to);
        if (candidates.size() == 0) {
            return null;
        }
        return candidates.get(0);
    }

    /**
     * Returns all the adapt methods that convert from the given type.
     */
    public Method[] findAdaptMethodsFrom(Class from) {
        Method[] methods = mAdaptMethods;
        List<Method> candidates = new ArrayList<Method>(methods.length);
        for (int i=methods.length; --i>=0; ) {
            Method method = methods[i];
            if (method.getParameterTypes()[0].isAssignableFrom(from)) {
                candidates.add(method);
            }
        }
        return (Method[]) candidates.toArray(new Method[candidates.size()]);
    }

    /**
     * Returns all the adapt methods that convert to the given type.
     */
    @SuppressWarnings("unchecked")
    public Method[] findAdaptMethodsTo(Class to) {
        Method[] methods = mAdaptMethods;
        List<Method> candidates = new ArrayList<Method>(methods.length);
        for (int i=methods.length; --i>=0; ) {
            Method method = methods[i];
            if (to.isAssignableFrom(method.getReturnType())) {
                candidates.add(method);
            }
        }
        reduceCandidates(candidates, to);
        return (Method[]) candidates.toArray(new Method[candidates.size()]);
    }

    /**
     * Returns the count of all defined adapt methods.
     */
    public int getAdaptMethodCount() {
        return mAdaptMethods.length;
    }

    /**
     * Returns a specific adapt method.
     */
    public Method getAdaptMethod(int index) throws IndexOutOfBoundsException {
        return mAdaptMethods[index];
    }

    /**
     * Returns a new array with all the adapt methods in it.
     */
    public Method[] getAdaptMethods() {
        return mAdaptMethods.clone();
    }

    private void reduceCandidates(List<Method> candidates, Class to) {
        if (candidates.size() <= 1) {
            // Shortcut.
            return;
        }

        // Map "from" type to all methods that convert from it. When reduced,
        // the list lengths are one.
        Map<Class, List<Method>> fromMap = new LinkedHashMap<Class, List<Method>>();

        for (Method method : candidates) {
            Class from = method.getParameterTypes()[0];
            List<Method> matches = fromMap.get(from);
            if (matches == null) {
                matches = new ArrayList<Method>();
                fromMap.put(from, matches);
            }
            matches.add(method);
        }

        candidates.clear();

        for (List<Method> matches : fromMap.values()) {
            Method best = null;
            int bestDistance = Integer.MAX_VALUE;
            for (Method method : matches) {
                int distance = distance(method.getReturnType(), to);
                if (best == null || distance < bestDistance) {
                    best = method;
                    bestDistance = distance;
                }
            }
            candidates.add(best);
        }
    }

    private static int distance(Class from, Class to) {
        int distance = 0;
        while (from != to) {
            from = from.getSuperclass();
            if (from == null) {
                return Integer.MAX_VALUE;
            }
            distance++;
        }
        return distance;
    }
}
