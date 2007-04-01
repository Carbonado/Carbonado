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

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cojen.classfile.MethodDesc;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.BeanComparator;
import org.cojen.util.BeanProperty;
import org.cojen.util.BeanIntrospector;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.AlternateKeys;
import com.amazon.carbonado.Authoritative;
import com.amazon.carbonado.Automatic;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Index;
import com.amazon.carbonado.Indexes;
import com.amazon.carbonado.Join;
import com.amazon.carbonado.Key;
import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Sequence;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Version;
import com.amazon.carbonado.adapter.AdapterDefinition;
import com.amazon.carbonado.constraint.ConstraintDefinition;
import com.amazon.carbonado.lob.Lob;

/**
 * Supports examination of {@link Storable} types, returning all metadata
 * associated with it. As part of the examination, all annotations are gathered
 * up. All examined data is cached, so repeat examinations are fast, unless the
 * examination failed.
 *
 * @author Brian S O'Neill
 */
public class StorableIntrospector {
    // Weakly maps Class objects to softly referenced StorableInfo objects.
    @SuppressWarnings("unchecked")
    private static Map<Class<?>, Reference<StorableInfo<?>>> cCache = new WeakIdentityMap();

    /**
     * Examines the given class and returns a StorableInfo describing it. A
     * MalformedTypeException is thrown for a variety of reasons if the given
     * class is not a well-defined Storable type.
     *
     * @param type Storable type to examine
     * @throws MalformedTypeException if Storable type is not well-formed
     * @throws IllegalArgumentException if type is null
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> StorableInfo<S> examine(Class<S> type) {
        if (type == null) {
            throw new IllegalArgumentException("Storable type must not be null");
        }
        synchronized (cCache) {
            StorableInfo<S> info;
            Reference<StorableInfo<?>> ref = cCache.get(type);
            if (ref != null) {
                info = (StorableInfo<S>) ref.get();
                if (info != null) {
                    return info;
                }
            }

            List<String> errorMessages = new ArrayList<String>();

            // Pull these annotations out but finish processing later.
            List<NameAndDirection> primaryKeyProps;
            List<List<NameAndDirection>> alternateKeyProps;
            List<List<NameAndDirection>> indexProps;

            {
                try {
                    primaryKeyProps = gatherListProperties
                        (errorMessages, null, null, type.getAnnotation(PrimaryKey.class)).get(0);
                } catch (IndexOutOfBoundsException e) {
                    errorMessages.add("No primary key defined");
                    primaryKeyProps = Collections.emptyList();
                }
                alternateKeyProps = gatherListProperties
                    (errorMessages, null, type.getAnnotation(AlternateKeys.class), null);
                indexProps = gatherListProperties
                    (errorMessages, type.getAnnotation(Indexes.class), null, null);
            }

            // Get all the properties.
            Map<String, StorableProperty<S>> properties =
                examineProperties(type, primaryKeyProps, alternateKeyProps);

            // Resolve keys and indexes.

            StorableKey<S> primaryKey;
            {
                Set<OrderedProperty<S>> propSet =
                    resolveKey(errorMessages, type, properties, "primary key", primaryKeyProps);
                primaryKey = new SKey<S>(true, propSet);
            }

            StorableKey<S>[] alternateKeys;
            {
                alternateKeys = new StorableKey[alternateKeyProps.size()];
                int i = 0;
                for (List<NameAndDirection> nameAndDirs : alternateKeyProps) {
                    Set<OrderedProperty<S>> propSet =
                        resolveKey(errorMessages, type, properties, "alternate key", nameAndDirs);
                    alternateKeys[i++] = new SKey<S>(false, propSet);
                }
            }

            StorableIndex<S>[] indexes;
            {
                indexes = new StorableIndex[indexProps.size()];
                int i = 0;
                for (List<NameAndDirection> nameAndDirs : indexProps) {
                    int errorCount = errorMessages.size();
                    Set<OrderedProperty<S>> propSet =
                        resolveKey(errorMessages, type, properties, "index", nameAndDirs);
                    if (errorMessages.size() <= errorCount) {
                        // If index property not found, error message has been
                        // added to list, but propSet might end up being
                        // empty. Rather than get an exception thrown from the
                        // StorableIndex constructor, just don't try to define
                        // the bogus index at all.
                        OrderedProperty<S>[] propArray = new OrderedProperty[propSet.size()];
                        propSet.toArray(propArray);
                        indexes[i] = new StorableIndex<S>(propArray, null);
                    }
                    i++;
                }
            }

            // Sort properties by name, grouped with primary keys first. This
            // ensures a consistent arrangement, even if methods move around in
            // the class file.
            {
                // Store results in a LinkedHashMap to preserve sort order.
                Map<String, StorableProperty<S>> arrangedProperties =
                    new LinkedHashMap<String, StorableProperty<S>>();

                // First dump in primary key properties, in their proper order.
                for (OrderedProperty<S> orderedProp : primaryKey.getProperties()) {
                    StorableProperty<S> prop = orderedProp.getChainedProperty().getPrimeProperty();
                    arrangedProperties.put(prop.getName(), prop);
                }

                // Gather all remaining properties, and then sort them.
                List<StorableProperty<S>> nonPkProperties = new ArrayList<StorableProperty<S>>();

                for (StorableProperty<S> prop : properties.values()) {
                    if (!arrangedProperties.containsKey(prop.getName())) {
                        nonPkProperties.add(prop);
                    }
                }

                Collections.sort(nonPkProperties,
                                 BeanComparator.forClass(StorableProperty.class).orderBy("name"));

                for (StorableProperty<S> prop : nonPkProperties) {
                    arrangedProperties.put(prop.getName(), prop);
                }

                properties = Collections.unmodifiableMap(arrangedProperties);
            }

            // Process type aliases

            String[] aliases;
            Alias alias = type.getAnnotation(Alias.class);
            if (alias == null) {
                aliases = null;
            } else {
                aliases = alias.value();
                if (aliases.length == 0) {
                    errorMessages.add("Alias list is empty");
                }
            }

            info = new Info<S>(type, aliases, indexes, properties,
                               primaryKey, alternateKeys,
                               type.getAnnotation(Independent.class) != null,
                               type.getAnnotation(Authoritative.class) != null);
            cCache.put(type, new SoftReference<StorableInfo<?>>(info));

            // Finish resolving join properties, after properties have been
            // added to cache. This makes it possible for joins to (directly or
            // indirectly) reference their own enclosing type. If not resolved
            // late, then there would be a stack overflow.
            for (StorableProperty property : properties.values()) {
                if (property instanceof JoinProperty) {
                    ((JoinProperty)property).resolve(errorMessages, properties);
                }
            }
            if (errorMessages.size() > 0) {
                cCache.remove(type);
                throw new MalformedTypeException(type, errorMessages);
            }

            return info;
        }
    }

    private static class NameAndDirection {
        final String name;
        final Direction direction;
        NameAndDirection(String name, Direction direction) {
            this.name = name;
            this.direction = direction;
        }

        @Override
        public int hashCode() {
            return name.hashCode() + direction.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof NameAndDirection) {
                // Only compare name.
                return name.equals(((NameAndDirection) obj).name);
            }
            return false;
        }
    }

    /**
     * @param indexes pass in just this for gathering index properties
     * @param keys pass in just this for gathering alternate key properties
     * @param primaryKey pass in just this for gathering primary key properties
     */
    private static List<List<NameAndDirection>> gatherListProperties(List<String> errorMessages,
                                                                     Indexes indexes,
                                                                     AlternateKeys keys,
                                                                     PrimaryKey primaryKey)
    {
        List<List<NameAndDirection>> listlist = new ArrayList<List<NameAndDirection>>();

        if (indexes != null) {
            Index[] ixs = indexes.value();
            if (ixs != null && ixs.length > 0) {
                for (int i=0; i < ixs.length; i++) {
                    String[] propNames = ixs[i].value();
                    if (propNames == null || propNames.length == 0) {
                        errorMessages.add("Empty index defined");
                        continue;
                    }
                    gatherListProperties(errorMessages, "index", propNames, listlist);
                }
            }
        } else if (keys != null) {
            Key[] ixs = keys.value();
            if (ixs != null && ixs.length > 0) {
                for (int i=0; i < ixs.length; i++) {
                    String[] propNames = ixs[i].value();
                    if (propNames == null || propNames.length == 0) {
                        errorMessages.add("Empty alternate key defined");
                        continue;
                    }
                    gatherListProperties(errorMessages, "alternate key", propNames, listlist);
                }
            }
        } else if (primaryKey != null) {
            String[] propNames = primaryKey.value();
            if (propNames == null || propNames.length == 0) {
                errorMessages.add("Empty primary key defined");
            } else {
                gatherListProperties(errorMessages, "primary key", propNames, listlist);
            }
        }

        return listlist;
    }

    private static void gatherListProperties(List<String> errorMessages,
                                             String listName,
                                             String[] propNames,
                                             List<List<NameAndDirection>> listlist)
    {
        int length = propNames.length;
        List<NameAndDirection> nameAndDirs = new ArrayList<NameAndDirection>(length);

        for (int i=0; i<length; i++) {
            String name = propNames[i];
            Direction dir = Direction.UNSPECIFIED;
            if (name.length() > 0) {
                if (name.charAt(0) == '+') {
                    name = name.substring(1);
                    dir = Direction.ASCENDING;
                } else if (name.charAt(0) == '-') {
                    name = name.substring(1);
                    dir = Direction.DESCENDING;
                }
            }

            NameAndDirection nameAndDir = new NameAndDirection(name, dir);

            if (nameAndDirs.contains(nameAndDir)) {
                errorMessages.add
                    ("Duplicate property in " + listName + ": " + Arrays.toString(propNames));
                continue;
            } else {
                nameAndDirs.add(nameAndDir);
            }
        }

        if (nameAndDirs.size() == 0) {
            return;
        }

        if (listlist.contains(nameAndDirs)) {
            errorMessages.add
                ("Duplicate " + listName + " specification: " + Arrays.toString(propNames));
            return;
        }

        listlist.add(nameAndDirs);
    }

    private static <S extends Storable> Set<OrderedProperty<S>>
        resolveKey(List<String> errorMessages,
                   Class<S> type,
                   Map<String, StorableProperty<S>> properties,
                   String elementName,
                   List<NameAndDirection> nameAndDirs)
    {
        Set<OrderedProperty<S>> orderedProps = new LinkedHashSet<OrderedProperty<S>>();

        for (NameAndDirection nameAndDir : nameAndDirs) {
            String name = nameAndDir.name;
            if (name.indexOf('.') > 0) {
                errorMessages.add("Chained property not allowed in " + elementName + ": " + name);
                continue;
            }
            StorableProperty<S> prop = properties.get(name);
            if (prop == null) {
                errorMessages.add
                    ("Property for " + elementName + " not found: " + name);
                continue;
            }
            if (prop.isJoin()) {
                errorMessages.add
                    ("Property of " + elementName + " cannot reference a join property: " + name);
                continue;
            }
            if (Lob.class.isAssignableFrom(prop.getType())) {
                errorMessages.add
                    ("Property of " + elementName + " cannot reference a LOB property: " + name);
                continue;
            }

            orderedProps.add(OrderedProperty.get(prop, nameAndDir.direction));
        }

        if (orderedProps.size() == 0) {
            return Collections.emptySet();
        }
        if (orderedProps.size() == 1) {
            return Collections.singleton(orderedProps.iterator().next());
        }
        return Collections.unmodifiableSet(orderedProps);
    }

    /**
     * Does the real work in examining the given type. The join properties and
     * alternate keys must still be resolved afterwards.
     */
    private static <S extends Storable> Map<String, StorableProperty<S>>
        examineProperties(Class<S> type,
                          List<NameAndDirection> primaryKeyProps,
                          List<List<NameAndDirection>> alternateKeyProps)
        throws MalformedTypeException
    {
        if (Storable.class.isAssignableFrom(type)) {
            if (Storable.class == type) {
                throw new MalformedTypeException(type, "Storable interface must be extended");
            }
        } else {
            throw new MalformedTypeException
                (type, "Does not implement Storable interface");
        }
        int modifiers = type.getModifiers();
        if (Modifier.isFinal(modifiers)) {
            throw new MalformedTypeException(type, "Class is declared final");
        }
        if (!Modifier.isPublic(modifiers)) {
            throw new MalformedTypeException(type, "Class is not public");
        }

        List<String> errorMessages = new ArrayList<String>();

        checkTypeParameter(errorMessages, type);

        // If type is a class, it must have a public or protected no-arg
        // constructor.

        if (!type.isInterface()) {
            Constructor[] ctors = type.getDeclaredConstructors();
            findCtor: {
                for (Constructor c : ctors) {
                    if (c.getParameterTypes().length == 0) {
                        modifiers = c.getModifiers();
                        if (!Modifier.isPublic(modifiers) && !Modifier.isProtected(modifiers)) {
                            errorMessages.add("Cannot call constructor: " + c);
                        }
                        break findCtor;
                    }
                }
                if (type.getEnclosingClass() == null) {
                    errorMessages.add
                        ("Class must have an accesible no-arg constructor");
                } else {
                    errorMessages.add
                        ("Inner class must be static and have an accesible no-arg constructor");
                }
            }
        }

        // All methods to be implemented must be bean property methods that
        // operate on a supported type.

        // First, gather all methods that must be implemented.

        // Gather all methods.  We'll be removing them as we implement them,
        // and if there are any abstract ones left over at the end, why,
        // that would be bad.
        Map<String, Method> methods = gatherAllDeclaredMethods(type);

        // Remove methods not abstract or defined explicitly in
        // Storable. Storable methods still must be implemented, but not as
        // properties.
        for (Iterator<Method> it = methods.values().iterator(); it.hasNext(); ) {
            Method m = it.next();
            if (!Modifier.isAbstract(m.getModifiers()) ||
                m.getDeclaringClass() == Storable.class) {
                it.remove();
                continue;
            }
            // Check if abstract method is just redefining a method in
            // Storable.
            // TODO: Check if abstract method is redefining return type, which
            // is allowed for copy method. The return type must be within its
            // bounds.
            try {
                Method m2 = Storable.class.getMethod(m.getName(), (Class[]) m.getParameterTypes());
                if (m.getReturnType() == m2.getReturnType()) {
                    it.remove();
                }
            } catch (NoSuchMethodException e) {
                // Not defined in Storable.
            }
        }

        // Identify which properties are members of a primary or alternate key.
        Set<String> pkPropertyNames, altKeyPropertyNames;
        {
            pkPropertyNames = new HashSet<String>();
            altKeyPropertyNames = new HashSet<String>();
            for (NameAndDirection nameAndDir : primaryKeyProps) {
                pkPropertyNames.add(nameAndDir.name);
            }
            for (List<NameAndDirection> list : alternateKeyProps) {
                for (NameAndDirection nameAndDir : list) {
                    altKeyPropertyNames.add(nameAndDir.name);
                }
            }
        }

        Map allProperties = BeanIntrospector.getAllProperties(type);

        // Copy only the properties that should be implemented here.
        Map<String, StorableProperty<S>> properties =
            new HashMap<String, StorableProperty<S>>();

        // Remove methods for properties that can be implemented.
        Iterator it = allProperties.values().iterator();
        while (it.hasNext()) {
            BeanProperty property = BeanProperty.class.cast(it.next());
            Method readMethod = property.getReadMethod();
            Method writeMethod = property.getWriteMethod();

            if (readMethod == null && writeMethod == null) {
                continue;
            }

            boolean pk = pkPropertyNames.contains(property.getName());
            boolean altKey = altKeyPropertyNames.contains(property.getName());

            StorableProperty<S> storableProp = makeStorableProperty
                (errorMessages, property, type, pk, altKey);

            if (storableProp == null) {
                // Errors.
                continue;
            }

            if (readMethod != null) {
                String sig = createSig(readMethod);
                if (methods.containsKey(sig)) {
                    methods.remove(sig);
                    properties.put(property.getName(), storableProp);
                } else {
                    continue;
                }
            }

            if (writeMethod != null) {
                String sig = createSig(writeMethod);
                if (methods.containsKey(sig)) {
                    methods.remove(sig);
                    properties.put(property.getName(), storableProp);
                } else {
                    continue;
                }
            }
        }

        // Only include errors on unimplementable methods if there are no other
        // errors. This prevents producing errors caused by other errors.
        Iterator<Method> iter = methods.values().iterator();
        while (iter.hasNext()) {
            Method m = iter.next();
            int methodModifiers = m.getModifiers();
            if (Modifier.isAbstract(methodModifiers )) {
                if (!Modifier.isPublic(methodModifiers) && !Modifier.isProtected(methodModifiers))
                {
                    errorMessages.add("Abstract method cannot be defined (neither public or " +
                                      "protected): " + m);
                } else {
                    errorMessages.add
                        ("Abstract method cannot be defined (not a bean property): " + m);
                }
                // We've reported the error, nothing more to say about it
                iter.remove();
            }
        }

        // Verify at most one version property exists.
        {
            boolean hasVersionProp = false;
            for (StorableProperty property : properties.values()) {
                if (property.isVersion()) {
                    if (hasVersionProp) {
                        errorMessages.add
                            ("At most one property may be designated as the version number");
                        break;
                    }
                    hasVersionProp = true;
                }
            }
        }

        // Only include errors on unimplementable methods if there are no other
        // errors. This prevents producing errors caused by other errors.
        if (errorMessages.size() == 0 && methods.size() > 0) {
            for (Method m : methods.values()) {
                errorMessages.add("Method cannot be implemented: " + m);
            }
        }

        if (errorMessages.size() > 0) {
            throw new MalformedTypeException(type, errorMessages);
        }

        return Collections.unmodifiableMap(properties);
    }

    /**
     * Make sure that the parameter type that is specified to Storable can be
     * assigned to a Storable, and that the given type can be assigned to
     * it. Put another way, the upper bound is Storable, and the lower bound
     * is the given type. type <= parameterized type <= Storable
     */
    @SuppressWarnings("unchecked")
    private static void checkTypeParameter(List<String> errorMessages, Class type) {
        // Only check classes and interfaces that extend Storable.
        if (type != null && Storable.class.isAssignableFrom(type)) {
            if (Storable.class == type) {
                return;
            }
        } else {
            return;
        }

        // Check all superclasses and interfaces.
        checkTypeParameter(errorMessages, type.getSuperclass());

        for (Class c : type.getInterfaces()) {
            checkTypeParameter(errorMessages, c);
        }

        for (Type t : type.getGenericInterfaces()) {
            if (t instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType)t;
                if (pt.getRawType() == Storable.class) {
                    // Found exactly which parameter is passed directly to
                    // Storable. Make sure that it is in the proper bounds.
                    Type arg = pt.getActualTypeArguments()[0];
                    Class param;
                    if (arg instanceof ParameterizedType) {
                        Type raw = ((ParameterizedType)arg).getRawType();
                        if (raw instanceof Class) {
                            param = (Class)raw;
                        } else {
                            continue;
                        }
                    } else if (arg instanceof Class) {
                        param = (Class)arg;
                    } else if (arg instanceof TypeVariable) {
                        // TODO
                        continue;
                    } else {
                        continue;
                    }
                    if (Storable.class.isAssignableFrom(param)) {
                        if (!param.isAssignableFrom(type)) {
                            errorMessages.add
                                ("Type parameter passed from " + type +
                                 " to Storable must be a " + type.getName() + ": " + param);
                            return;
                        }
                    } else {
                        errorMessages.add
                            ("Type parameter passed from " + type +
                             " to Storable must be a Storable: " + param);
                        return;
                    }
                }
            }
        }
    }

    /**
     * If property is a join, then it is not yet completely resolved. Returns
     * null if there are any errors.
     *
     * @param errorMessages error messages go here
     * @param property property to examine
     * @param enclosing enclosing class
     * @param pk true if member of primary key
     * @param altKey true if member of alternate key
     */
    @SuppressWarnings("unchecked")
    private static <S extends Storable> StorableProperty<S> makeStorableProperty
                              (List<String> errorMessages,
                               BeanProperty property,
                               Class<S> enclosing,
                               boolean pk, boolean altKey)
    {
        Nullable nullable = null;
        Alias alias = null;
        Version version = null;
        Sequence sequence = null;
        Automatic automatic = null;
        Independent independent = null;
        Join join = null;

        Method readMethod = property.getReadMethod();
        Method writeMethod = property.getWriteMethod();

        if (readMethod == null) {
            if (writeMethod == null || Modifier.isAbstract(writeMethod.getModifiers())) {
                // If we got here, the onus is on us to create this property. It's never
                // ok for the read method (get) to be null.
                errorMessages.add
                    ("Must define proper 'get' method for property: " + property.getName());
            }
        } else {
            nullable = readMethod.getAnnotation(Nullable.class);
            alias = readMethod.getAnnotation(Alias.class);
            version = readMethod.getAnnotation(Version.class);
            sequence = readMethod.getAnnotation(Sequence.class);
            automatic = readMethod.getAnnotation(Automatic.class);
            independent = readMethod.getAnnotation(Independent.class);
            join = readMethod.getAnnotation(Join.class);
        }

        if (writeMethod == null) {
            if (readMethod == null || Modifier.isAbstract(readMethod.getModifiers())) {
                // Set method is always required for non-join properties. More
                // work is done later on join properties, and sometimes the
                // write method is required.
                if (join == null) {
                    errorMessages.add("Must define proper 'set' method for property: " +
                                      property.getName());
                }
            }
        } else {
            if (writeMethod.getAnnotation(Nullable.class) != null) {
                errorMessages.add
                    ("Nullable annotation not allowed on mutator: " + writeMethod);
            }
            if (writeMethod.getAnnotation(Alias.class) != null) {
                errorMessages.add
                    ("Alias annotation not allowed on mutator: " + writeMethod);
            }
            if (writeMethod.getAnnotation(Version.class) != null) {
                errorMessages.add
                    ("Version annotation not allowed on mutator: " + writeMethod);
            }
            if (writeMethod.getAnnotation(Sequence.class) != null) {
                errorMessages.add
                    ("Sequence annotation not allowed on mutator: " + writeMethod);
            }
            if (writeMethod.getAnnotation(Automatic.class) != null) {
                errorMessages.add
                    ("Automatic annotation not allowed on mutator: " + writeMethod);
            }
            if (writeMethod.getAnnotation(Independent.class) != null) {
                errorMessages.add
                    ("Independent annotation not allowed on mutator: " + writeMethod);
            }
            if (writeMethod.getAnnotation(Join.class) != null) {
                errorMessages.add
                    ("Join annotation not allowed on mutator: " + writeMethod);
            }
        }

        if (nullable != null && property.getType().isPrimitive()) {
            errorMessages.add
                ("Properties which have a primitive type cannot be declared nullable: " +
                 "Property \"" + property.getName() + "\" has type \"" +
                 property.getType() + '"');
        }

        String[] aliases = null;
        if (alias != null) {
            aliases = alias.value();
            if (aliases.length == 0) {
                errorMessages.add
                    ("Alias list is empty for property: " + property.getName());
            }
        }

        StorablePropertyConstraint[] constraints = null;
        if (readMethod != null) {
            // Constraints not allowed on read method. Look for them and
            // generate errors if any found.
            gatherConstraints(property, readMethod, false, errorMessages);
        }
        if (writeMethod != null) {
            constraints = gatherConstraints(property, writeMethod, true, errorMessages);
        }

        StorablePropertyAdapter[] adapters = null;
        if (readMethod != null) {
            adapters = gatherAdapters(property, readMethod, true, errorMessages);
            if (adapters != null && adapters.length > 0) {
                if (join != null) {
                    errorMessages.add
                        ("Join properties cannot have adapters: " + property.getName());
                }
                if (adapters.length > 1) {
                    errorMessages.add
                        ("Only one adpater allowed per property: " + property.getName());
                }
            }
            if (adapters == null || adapters.length == 0) {
                StorablePropertyAdapter autoAdapter =
                    AutomaticAdapterSelector.selectAdapterFor(property);
                if (autoAdapter != null) {
                    adapters = new StorablePropertyAdapter[] {autoAdapter};
                }
            }
        }
        if (writeMethod != null) {
            // Adapters not allowed on write method. Look for them and generate
            // errors if any found.
            gatherAdapters(property, writeMethod, false, errorMessages);
        }

        String sequenceName = null;
        if (sequence != null) {
            sequenceName = sequence.value();
        }

        if (join == null) {
            if (errorMessages.size() > 0) {
                return null;
            }
            return new SimpleProperty<S>
                (property, enclosing, nullable != null, pk, altKey,
                 aliases, constraints, adapters == null ? null : adapters[0],
                 version != null, sequenceName, independent != null, automatic != null);
        }

        // Do additional work for join properties.

        String[] internal = join.internal();
        String[] external = join.external();

        if (internal == null) {
            internal = new String[0];
        }
        if (external == null) {
            external = new String[0];
        }

        if (internal.length != external.length) {
            errorMessages.add
                ("Internal/external lists on Join property \"" + property.getName() +
                 "\" differ in length: " + internal.length + " != " + external.length);
        }

        Class joinedType = property.getType();
        if (Query.class == joinedType) {
            if (nullable != null) {
                errorMessages.add
                    ("Join property \"" + property.getName() +
                     "\" cannot be declared as nullable because the type is Query");
            }

            // Recover the results element type from the accessor. A Mutator is
            // not allowed.

            if (property.getWriteMethod() != null) {
                errorMessages.add
                    ("Join property \"" + property.getName() +
                     "\" cannot have a mutator because the type is Query: " +
                     property.getWriteMethod());
            }

            if (property.getReadMethod() == null) {
                // Default.
                joinedType = Storable.class;
            } else {
                Type genericType = property.getReadMethod().getGenericReturnType();

                if (genericType instanceof Class) {
                    // Default.
                    joinedType = Storable.class;
                } else if (genericType instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType)genericType;
                    Type[] args = pt.getActualTypeArguments();
                    if (args == null || args.length == 0) {
                        // Default.
                        joinedType = Storable.class;
                    } else {
                        Type arg = args[0];
                        while (arg instanceof ParameterizedType) {
                            arg = ((ParameterizedType)arg).getRawType();
                        }
                        if (arg instanceof Class) {
                            joinedType = (Class)arg;
                        }
                    }
                }
            }
        }

        if (!Storable.class.isAssignableFrom(joinedType)) {
            errorMessages.add
                ("Type of join property \"" + property.getName() +
                 "\" is not a Storable: " + joinedType);
        }

        if (property.getReadMethod() != null) {
            Class exceptionType = FetchException.class;

            Class<?>[] exceptions = property.getReadMethod().getExceptionTypes();
            check: {
                for (int i=exceptions.length; --i>=0; ) {
                    if (exceptions[i].isAssignableFrom(exceptionType)) {
                        break check;
                    }
                }

                String exceptionName = exceptionType.getName();
                int index = exceptionName.lastIndexOf('.');
                if (index >= 0) {
                    exceptionName = exceptionName.substring(index + 1);
                }

                errorMessages.add
                    ("Join property accessor must declare throwing a " +
                     exceptionName + ": " + property.getReadMethod());
            }
        }

        if (version != null) {
            errorMessages.add
                ("Join property cannot be declared as a version property: " + property.getName());
        }

        if (errorMessages.size() > 0) {
            return null;
        }

        return new JoinProperty<S>
            (property, enclosing, nullable != null, aliases,
             constraints, adapters == null ? null : adapters[0],
             sequenceName, independent != null, automatic != null,
             joinedType, internal, external);
    }

    private static StorablePropertyConstraint[] gatherConstraints
        (BeanProperty property, Method method, boolean isAllowed, List<String> errorMessages)
    {
        Annotation[] allAnnotations = method.getAnnotations();
        if (allAnnotations.length == 0) {
            return null;
        }

        List<StorablePropertyConstraint> list = new ArrayList<StorablePropertyConstraint>();

        for (Annotation annotation : allAnnotations) {
            Class<? extends Annotation> type = annotation.annotationType();
            ConstraintDefinition cd = type.getAnnotation(ConstraintDefinition.class);
            if (cd == null) {
                continue;
            }
            if (!isAllowed) {
                errorMessages.add("Constraint not allowed on method: " + method);
                return null;
            }

            Class constraintClass = cd.implementation();

            if (constraintClass == void.class) {
                // Magic value meaning "use default", which is an inner class
                // of the annotation.

                constraintClass = null;

                // Search for inner class named "Constraint".
                Class[] innerClasses = type.getClasses();
                for (Class c : innerClasses) {
                    if ("Constraint".equals(c.getSimpleName())) {
                        constraintClass = c;
                        break;
                    }
                }

                if (constraintClass == null) {
                    errorMessages.add
                        ("By default, constraint implementation class must be a static inner " +
                         "class of the annotation named \"Constraint\". Fully qualified name: " +
                         type.getCanonicalName() + ".Constraint");
                    continue;
                }
            }

            int modifiers = constraintClass.getModifiers();

            if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers) ||
                !Modifier.isPublic(modifiers)) {

                errorMessages.add
                    ("Constraint implementation class must be a concrete public class: " +
                     constraintClass.getName());
                continue;
            }

            Constructor ctor;
            try {
                ctor = constraintClass.getConstructor(Class.class, String.class, type);
            } catch (NoSuchMethodException e) {
                errorMessages.add
                    ("Constraint implementation class does not have proper constructor: " +
                     constraintClass.getName());
                continue;
            }

            // Find best constrain method to bind to.

            ConversionComparator cc = new ConversionComparator(property.getType());
            Class bestMatchingType = null;
            Method bestConstrainMethod = null;

            for (Method constrainMethod : constraintClass.getMethods()) {
                if (!constrainMethod.getName().equals("constrain")) {
                    continue;
                }
                if (constrainMethod.getReturnType() != void.class) {
                    continue;
                }
                Class<?>[] paramTypes = constrainMethod.getParameterTypes();
                if (paramTypes.length != 1) {
                    continue;
                }

                Class candidateType = paramTypes[0];

                if (!cc.isConversionPossible(candidateType)) {
                    continue;
                }

                if (bestMatchingType == null || cc.compare(bestMatchingType, candidateType) > 0) {
                    bestMatchingType = candidateType;
                    bestConstrainMethod = constrainMethod;
                }
            }

            if (bestConstrainMethod == null) {
                errorMessages.add("Constraint does not support property type: " +
                                  property.getType().getName() + "; constraint type: " +
                                  annotation.annotationType().getName());
            } else {
                StorablePropertyAnnotation spa =
                    new StorablePropertyAnnotation(annotation, method);
                list.add(new StorablePropertyConstraint(spa, ctor, bestConstrainMethod));
            }
        }

        if (list.size() == 0) {
            return null;
        }

        return (StorablePropertyConstraint[]) list.toArray
            (new StorablePropertyConstraint[list.size()]);
    }

    private static StorablePropertyAdapter[] gatherAdapters
        (BeanProperty property, Method method, boolean isAllowed, List<String> errorMessages)
    {
        Annotation[] allAnnotations = method.getAnnotations();
        if (allAnnotations.length == 0) {
            return null;
        }

        List<StorablePropertyAdapter> list = new ArrayList<StorablePropertyAdapter>();

        for (Annotation annotation : allAnnotations) {
            Class<? extends Annotation> type = annotation.annotationType();
            AdapterDefinition ad = type.getAnnotation(AdapterDefinition.class);
            if (ad == null) {
                continue;
            }
            if (!isAllowed) {
                errorMessages.add("Adapter not allowed on method: " + method);
                return null;
            }

            Class adapterClass = StorablePropertyAdapter.findAdapterClass(type);

            if (adapterClass == null) {
                errorMessages.add
                    ("By default, adapter implementation class must be a static inner " +
                     "class of the annotation named \"Adapter\". Fully qualified name: " +
                     type.getCanonicalName() + ".Adapter");
                continue;
            }

            int modifiers = adapterClass.getModifiers();

            if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers) ||
                !Modifier.isPublic(modifiers)) {

                errorMessages.add
                    ("Adapter implementation class must be a concrete public class: " +
                     adapterClass.getName());
                continue;
            }

            Constructor ctor;
            try {
                ctor = adapterClass.getConstructor(Class.class, String.class, type);
            } catch (NoSuchMethodException e) {
                errorMessages.add
                    ("Adapter implementation class does not have proper constructor: " +
                     adapterClass.getName());
                continue;
            }

            Method[] adaptMethods =
                StorablePropertyAdapter.findAdaptMethods(property.getType(), adapterClass);

            if (adaptMethods.length == 0) {
                errorMessages.add("Adapter does not support property type: " +
                                  property.getType().getName() + "; adapter type: " +
                                  annotation.annotationType().getName());
            } else {
                StorablePropertyAnnotation spa =
                    new StorablePropertyAnnotation(annotation, method);
                list.add(new StorablePropertyAdapter(property, spa, ad, ctor, adaptMethods));
            }
        }

        if (list.size() == 0) {
            return null;
        }

        return (StorablePropertyAdapter[]) list.toArray(new StorablePropertyAdapter[list.size()]);
    }

    /**
     * Returns a new modifiable mapping of method signatures to methods.
     *
     * @return map of {@link #createSig signatures} to methods
     */
    private static Map<String, Method> gatherAllDeclaredMethods(Class clazz) {
        Map<String, Method> methods = new HashMap<String, Method>();
        gatherAllDeclaredMethods(methods, clazz);
        return methods;
    }

    private static void gatherAllDeclaredMethods(Map<String, Method> methods, Class clazz) {
        for (Method m : clazz.getDeclaredMethods()) {
            String desc = createSig(m);
            if (!methods.containsKey(desc)) {
                methods.put(desc, m);
            }
        }

        Class superclass = clazz.getSuperclass();
        if (superclass != null) {
            gatherAllDeclaredMethods(methods, superclass);
        }
        for (Class c : clazz.getInterfaces()) {
            gatherAllDeclaredMethods(methods, c);
        }
    }

    /**
     * Create a representation of the signature which includes the method name.
     * This uniquely identifies the method.
     *
     * @param m method to describe
     */
    private static String createSig(Method m) {
        return m.getName() + ':' + MethodDesc.forMethod(m).getDescriptor();
    }

    private static final class Info<S extends Storable> implements StorableInfo<S> {
        private final Class<S> mType;
        private final String[] mAliases;
        private final StorableIndex<S>[] mIndexes;
        private final Map<String, StorableProperty<S>> mAllProperties;
        private final StorableKey<S> mPrimaryKey;
        private final StorableKey<S>[] mAltKeys;
        private final boolean mIndependent;
        private final boolean mAuthoritative;

        private transient String mName;
        private transient Map<String, StorableProperty<S>> mPrimaryKeyProperties;
        private transient Map<String, StorableProperty<S>> mDataProperties;
        private transient StorableProperty<S> mVersionProperty;

        Info(Class<S> type, String[] aliases, StorableIndex<S>[] indexes,
             Map<String, StorableProperty<S>> properties,
             StorableKey<S> primaryKey,
             StorableKey<S>[] altKeys,
             boolean independent,
             boolean authoritative)
        {
            mType = type;
            mAliases = aliases;
            mIndexes = indexes;
            mAllProperties = properties;
            mPrimaryKey = primaryKey;
            mAltKeys = altKeys;
            mIndependent = independent;
            mAuthoritative = authoritative;
        }

        public String getName() {
            String name = mName;
            if (name == null) {
                name = getStorableType().getName();
                int index = name.lastIndexOf('.');
                if (index >= 0) {
                    name = name.substring(index + 1);
                }
                mName = name;
            }
            return name;
        }

        public Class<S> getStorableType() {
            return mType;
        }

        public Map<String, StorableProperty<S>> getAllProperties() {
            return mAllProperties;
        }

        public Map<String, StorableProperty<S>> getPrimaryKeyProperties() {
            if (mPrimaryKeyProperties == null) {
                Set<? extends OrderedProperty<S>> pkSet = mPrimaryKey.getProperties();
                Map<String, StorableProperty<S>> pkProps =
                    new LinkedHashMap<String, StorableProperty<S>>(pkSet.size());
                for (OrderedProperty<S> prop : pkSet) {
                    StorableProperty<S> prime = prop.getChainedProperty().getPrimeProperty();
                    pkProps.put(prime.getName(), prime);
                }
                mPrimaryKeyProperties = Collections.unmodifiableMap(pkProps);
            }
            return mPrimaryKeyProperties;
        }

        public Map<String, StorableProperty<S>> getDataProperties() {
            if (mDataProperties == null) {
                Map<String, StorableProperty<S>> dataProps =
                    new LinkedHashMap<String, StorableProperty<S>>(mAllProperties.size());
                for (Map.Entry<String, StorableProperty<S>> entry : mAllProperties.entrySet()) {
                    StorableProperty<S> property = entry.getValue();
                    if (!property.isPrimaryKeyMember() && !property.isJoin()) {
                        dataProps.put(entry.getKey(), property);
                    }
                }
                mDataProperties = Collections.unmodifiableMap(dataProps);
            }
            return mDataProperties;
        }

        public StorableProperty<S> getVersionProperty() {
            if (mVersionProperty == null) {
                for (StorableProperty<S> property : mAllProperties.values()) {
                    if (property.isVersion()) {
                        mVersionProperty = property;
                        break;
                    }
                }
            }
            return mVersionProperty;
        }

        public StorableKey<S> getPrimaryKey() {
            return mPrimaryKey;
        }

        public int getAlternateKeyCount() {
            StorableKey<S>[] keys = mAltKeys;
            return keys == null ? 0 : keys.length;
        }

        public StorableKey<S> getAlternateKey(int index) {
            StorableKey<S>[] keys = mAltKeys;
            if (keys == null) {
                throw new IndexOutOfBoundsException();
            } else {
                return keys[index];
            }
        }

        @SuppressWarnings("unchecked")
        public StorableKey<S>[] getAlternateKeys() {
            StorableKey<S>[] keys = mAltKeys;
            if (keys == null) {
                return new StorableKey[0];
            } else {
                return keys.clone();
            }
        }

        public int getAliasCount() {
            String[] aliases = mAliases;
            return aliases == null ? 0 : aliases.length;
        }

        public String getAlias(int index) {
            String[] aliases = mAliases;
            if (aliases == null) {
                throw new IndexOutOfBoundsException();
            } else {
                return aliases[index];
            }
        }

        public String[] getAliases() {
            String[] aliases = mAliases;
            if (aliases == null) {
                return new String[0];
            } else {
                return aliases.clone();
            }
        }

        public int getIndexCount() {
            StorableIndex<S>[] indexes = mIndexes;
            return indexes == null ? 0 : indexes.length;
        }

        public StorableIndex<S> getIndex(int index) {
            StorableIndex<S>[] indexes = mIndexes;
            if (indexes == null) {
                throw new IndexOutOfBoundsException();
            } else {
                return indexes[index];
            }
        }

        @SuppressWarnings("unchecked")
        public StorableIndex<S>[] getIndexes() {
            StorableIndex<S>[] indexes = mIndexes;
            if (indexes == null) {
                return new StorableIndex[0];
            } else {
                return indexes.clone();
            }
        }

        public final boolean isIndependent() {
            return mIndependent;
        }

        public final boolean isAuthoritative() {
            return mAuthoritative;
        }
    }

    private static class SKey<S extends Storable> implements StorableKey<S> {
        private final boolean mPrimary;
        private final Set<OrderedProperty<S>> mProperties;

        SKey(boolean primary, Set<OrderedProperty<S>> properties) {
            mPrimary = primary;
            mProperties = properties;
        }

        public boolean isPrimary() {
            return mPrimary;
        }

        public Set<OrderedProperty<S>> getProperties() {
            return mProperties;
        }

        @Override
        public int hashCode() {
            return mProperties.hashCode();
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof StorableKey) {
                StorableKey<S> other = (StorableKey<S>) obj;
                return isPrimary() == other.isPrimary()
                    && getProperties().equals(other.getProperties());
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("StorableKey ");
            try {
                appendTo(b);
            } catch (IOException e) {
                // Not gonna happen.
            }
            return b.toString();
        }

        /**
         * Appends the same results as toString, but without the "StorableKey"
         * prefix.
         */
        public void appendTo(Appendable app) throws IOException {
            app.append("{properties=[");
            int i = 0;
            for (OrderedProperty<S> prop : mProperties) {
                if (i++ > 0) {
                    app.append(", ");
                }
                prop.appendTo(app);
            }
            app.append(']');
            app.append(", primary=");
            app.append(String.valueOf(isPrimary()));
            app.append('}');
        }
    }

    private static class SimpleProperty<S extends Storable> implements StorableProperty<S> {
        private final BeanProperty mBeanProperty;
        private final Class<S> mEnclosingType;
        private final boolean mNullable;
        private final boolean mPrimaryKey;
        private final boolean mAlternateKey;
        private final String[] mAliases;
        private final StorablePropertyConstraint[] mConstraints;
        private final StorablePropertyAdapter mAdapter;
        private final boolean mIsVersion;
        private final String mSequence;
        private final boolean mIndependent;
        private final boolean mAutomatic;

        SimpleProperty(BeanProperty property, Class<S> enclosing,
                       boolean nullable, boolean primaryKey, boolean alternateKey,
                       String[] aliases, StorablePropertyConstraint[] constraints,
                       StorablePropertyAdapter adapter,
                       boolean isVersion, String sequence,
                       boolean independent, boolean automatic)
        {
            mBeanProperty = property;
            mEnclosingType = enclosing;
            mNullable = property.getType().isPrimitive() ? false : nullable;
            mPrimaryKey = primaryKey;
            mAlternateKey = alternateKey;
            mAliases = aliases;
            mConstraints = constraints;
            mAdapter = adapter;
            mIsVersion = isVersion;
            mSequence = sequence;
            mIndependent = independent;
            mAutomatic = automatic;
        }

        public final String getName() {
            return mBeanProperty.getName();
        }

        public final Class<?> getType() {
            return mBeanProperty.getType();
        }

        public final Class<S> getEnclosingType() {
            return mEnclosingType;
        }

        public final Method getReadMethod() {
            return mBeanProperty.getReadMethod();
        }

        public final String getReadMethodName() {
            Method m = mBeanProperty.getReadMethod();
            if (m != null) {
                return m.getName();
            }
            // Return synthetic name.
            return "get" + getWriteMethod().getName().substring(3);
        }

        public final Method getWriteMethod() {
            return mBeanProperty.getWriteMethod();
        }

        public final String getWriteMethodName() {
            Method m = mBeanProperty.getWriteMethod();
            if (m != null) {
                return m.getName();
            }
            // Return synthetic name.
            String readName = getReadMethod().getName();
            return "set" + readName.substring(readName.startsWith("is") ? 2 : 3);
        }

        public final boolean isNullable() {
            return mNullable;
        }

        public final boolean isPrimaryKeyMember() {
            return mPrimaryKey;
        }

        public final boolean isAlternateKeyMember() {
            return mAlternateKey;
        }

        public final int getAliasCount() {
            String[] aliases = mAliases;
            return aliases == null ? 0 : aliases.length;
        }

        public final String getAlias(int index) {
            String[] aliases = mAliases;
            if (aliases == null) {
                throw new IndexOutOfBoundsException();
            } else {
                return aliases[index];
            }
        }

        public final String[] getAliases() {
            String[] aliases = mAliases;
            if (aliases == null) {
                return new String[0];
            } else {
                return aliases.clone();
            }
        }

        public final String getSequenceName() {
            return mSequence;
        }

        public final boolean isAutomatic() {
            return mAutomatic;
        }

        public final boolean isIndependent() {
            return mIndependent;
        }

        public final boolean isVersion() {
            return mIsVersion;
        }

        public boolean isJoin() {
            return false;
        }

        public Class<? extends Storable> getJoinedType() {
            return null;
        }

        public int getJoinElementCount() {
            return 0;
        }

        public StorableProperty<S> getInternalJoinElement(int index) {
            throw new IndexOutOfBoundsException();
        }

        @SuppressWarnings("unchecked")
        public StorableProperty<S>[] getInternalJoinElements() {
            return new StorableProperty[0];
        }

        public StorableProperty<?> getExternalJoinElement(int index) {
            throw new IndexOutOfBoundsException();
        }

        public StorableProperty<?>[] getExternalJoinElements() {
            return new StorableProperty[0];
        }

        public boolean isQuery() {
            return false;
        }

        public int getConstraintCount() {
            StorablePropertyConstraint[] constraints = mConstraints;
            return constraints == null ? 0 : constraints.length;
        }

        public StorablePropertyConstraint getConstraint(int index) {
            StorablePropertyConstraint[] constraints = mConstraints;
            if (constraints == null) {
                throw new IndexOutOfBoundsException();
            } else {
                return constraints[index];
            }
        }

        public StorablePropertyConstraint[] getConstraints() {
            StorablePropertyConstraint[] constraints = mConstraints;
            if (constraints == null) {
                return new StorablePropertyConstraint[0];
            } else {
                return constraints.clone();
            }
        }

        public StorablePropertyAdapter getAdapter() {
            return mAdapter;
        }

        public int hashCode() {
            return (getName().hashCode() * 31 + getType().getName().hashCode()) * 31
                + getEnclosingType().getName().hashCode();
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof StorableProperty) {
                StorableProperty other = (StorableProperty) obj;
                return getName().equals(other.getName())
                    && getType().equals(other.getType())
                    && getEnclosingType().equals(other.getEnclosingType());
            }

            return false;
        }

        public String toString() {
            StringBuilder b = new StringBuilder();
            try {
                appendTo(b);
            } catch (IOException e) {
                // Not gonna happen
            }
            return b.toString();
        }

        public void appendTo(Appendable app) throws IOException {
            app.append("StorableProperty {name=");
            app.append(getName());
            app.append(", type=");
            app.append(TypeDesc.forClass(getType()).getFullName());
            app.append(", enclosing=");
            app.append(getEnclosingType().getName());
            app.append('}');
        }
    }

    private static final class JoinProperty<S extends Storable> extends SimpleProperty<S> {
        private final Class<? extends Storable> mJoinedType;

        // Just the names of the join properties, held here until properties
        // are fully resolved. After properties are resolved, arrays are thrown away.
        private String[] mInternalNames;
        private String[] mExternalNames;

        // Resolved join properties.
        private StorableProperty<S>[] mInternal;
        private StorableProperty<?>[] mExternal;

        JoinProperty(BeanProperty property, Class<S> enclosing,
                     boolean nullable,
                     String[] aliases, StorablePropertyConstraint[] constraints,
                     StorablePropertyAdapter adapter,
                     String sequence, boolean independent, boolean automatic,
                     Class<? extends Storable> joinedType,
                     String[] internal, String[] external)
        {
            super(property, enclosing, nullable, false, false,
                  aliases, constraints, adapter, false, sequence, independent, automatic);
            mJoinedType = joinedType;

            int length = internal.length;
            if (length != external.length) {
                throw new IllegalArgumentException();
            }

            mInternalNames = internal;
            mExternalNames = external;
        }

        public boolean isJoin() {
            return true;
        }

        public Class<? extends Storable> getJoinedType() {
            return mJoinedType;
        }

        public int getJoinElementCount() {
            return mInternal.length;
        }

        public StorableProperty<S> getInternalJoinElement(int index) {
            return mInternal[index];
        }

        public StorableProperty<S>[] getInternalJoinElements() {
            return mInternal.clone();
        }

        public StorableProperty<?> getExternalJoinElement(int index) {
            return mExternal[index];
        }

        public StorableProperty<?>[] getExternalJoinElements() {
            return mExternal.clone();
        }

        public boolean isQuery() {
            return getType() == Query.class;
        }

        /**
         * Finishes the definition of this join property. Can only be called once.
         */
        @SuppressWarnings("unchecked")
        void resolve(List<String> errorMessages, Map<String, StorableProperty<S>> properties) {
            StorableInfo<?> joinedInfo = examine(getJoinedType());

            if (mInternalNames.length == 0) {
                // Since no join elements specified, perform a natural join.
                // If the joined type is a list, then the join elements are
                // defined by this enclosing type's primary keys. Otherwise,
                // they are defined by the joined type's primary keys.

                Map<String, ? extends StorableProperty<?>> primaryKeys;

                if (isQuery()) {
                    primaryKeys = examine(getEnclosingType()).getPrimaryKeyProperties();
                } else {
                    primaryKeys = joinedInfo.getPrimaryKeyProperties();
                }

                mInternalNames = new String[primaryKeys.size()];
                mExternalNames = new String[primaryKeys.size()];

                int i = 0;
                for (String name : primaryKeys.keySet()) {
                    mInternalNames[i] = name;
                    mExternalNames[i] = name;
                    i++;
                }
            }

            mInternal = new StorableProperty[mInternalNames.length];
            mExternal = new StorableProperty[mExternalNames.length];

            // Verify that internal properties exist and are not themselves joins.
            for (int i=0; i<mInternalNames.length; i++) {
                String internalName = mInternalNames[i];
                StorableProperty property = properties.get(internalName);
                if (property == null) {
                    errorMessages.add
                        ("Cannot find internal join element: \"" +
                         getName() + "\" internally joins to property \"" +
                         internalName + '"');
                    continue;
                }
                if (property.isJoin()) {
                    errorMessages.add
                        ("Join properties cannot join to other join properties: \"" +
                         getName() + "\" internally joins to property \"" +
                         internalName + '"');
                    continue;
                }
                if (Lob.class.isAssignableFrom(property.getType())) {
                    errorMessages.add
                        ("Join properties cannot join to LOB properties: \"" +
                         getName() + "\" internally joins to LOB property \"" +
                         internalName + '"');
                    continue;
                }
                /* this check is too restrictive
                if (property.isNullable() && !isNullable()) {
                    errorMessages.add
                        ("Join must be declared nullable since internal element " +
                         "is nullable: \"" + getName() +
                         "\" internally joins to nullable property \"" + internalName + '"');
                }
                */
                mInternal[i] = property;
            }

            // Verify that external properties exist and are not themselves joins.

            Map<String, ? extends StorableProperty<?>> externalProperties =
                joinedInfo.getAllProperties();

            // Track whether join property is allowed to have a set method.
            boolean mutatorAllowed = !isQuery();

            for (int i=0; i<mExternalNames.length; i++) {
                String externalName = mExternalNames[i];
                StorableProperty property = externalProperties.get(externalName);
                if (property == null) {
                    errorMessages.add
                        ("Cannot find external join element: \"" +
                         getName() + "\" externally joins to property \"" +
                         externalName + '"');
                    continue;
                }
                if (property.isJoin()) {
                    errorMessages.add
                        ("Join properties cannot join to other join properties: \"" +
                         getName() + "\" externally joins to property \"" +
                         externalName + '"');
                    continue;
                }
                if (Lob.class.isAssignableFrom(property.getType())) {
                    errorMessages.add
                        ("Join properties cannot join to LOB properties: \"" +
                         getName() + "\" externally joins to LOB property \"" +
                         externalName + '"');
                    continue;
                }
                if (property.getReadMethod() == null) {
                    mutatorAllowed = false;
                    if (getWriteMethod() != null) {
                        errorMessages.add
                            ("Join property cannot have a mutator if external property " +
                             "has no accessor: Mutator = \"" + getWriteMethod() +
                             "\", external property = \"" + property.getName() + '"');
                        continue;
                    }
                }
                mExternal[i] = property;
            }

            if (errorMessages.size() > 0) {
                return;
            }

            // Verify that join types match type.
            for (int i=0; i<mInternal.length; i++) {
                StorableProperty internalProperty = getInternalJoinElement(i);
                StorableProperty externalProperty = getExternalJoinElement(i);

                if (!internalProperty.isNullable() && externalProperty.isNullable()) {
                    mutatorAllowed = false;
                    if (getWriteMethod() != null) {
                        errorMessages.add
                            ("Join property cannot have a mutator if internal property " +
                             "is required, but external property is nullable: Mutator = \"" +
                             getWriteMethod() +
                             "\", internal property = \"" + internalProperty.getName() +
                             "\", external property = \"" + externalProperty.getName() + '"');
                    }
                }

                Class internalClass = internalProperty.getType();
                Class externalClass = externalProperty.getType();

                if (internalClass == externalClass) {
                    continue;
                }

                TypeDesc internalType = TypeDesc.forClass(internalClass).toObjectType();
                TypeDesc externalType = TypeDesc.forClass(externalClass).toObjectType();

                if (internalType == externalType) {
                    continue;
                }

                compatibilityCheck: {
                    // Conversion to String, CharSequence, or Object is always
                    // allowed.
                    if (externalClass == String.class || externalClass == Object.class ||
                        externalClass == CharSequence.class) {
                        break compatibilityCheck;
                    }

                    // Allow internal type to be "narrower" than external type.

                    // (byte) ==> (Number | short | int | long)
                    // (byte | short) ==> (Number | int | long)
                    // (byte | short | int) ==> (Number | long)
                    // (float) ==> (Number | double)

                    TypeDesc primInternal = internalType.toPrimitiveType();
                    TypeDesc primExternal = externalType.toPrimitiveType();

                    if (primInternal != null) {
                        switch (primInternal.getTypeCode()) {
                        case TypeDesc.BYTE_CODE:
                            if (primExternal == null) {
                                if (externalType.toClass() == Number.class) {
                                    break compatibilityCheck;
                                }
                            } else {
                                switch (primExternal.getTypeCode()) {
                                case TypeDesc.SHORT_CODE:
                                case TypeDesc.INT_CODE:
                                case TypeDesc.LONG_CODE:
                                    break compatibilityCheck;
                                }
                            }
                            break;
                        case TypeDesc.SHORT_CODE:
                            if (primExternal == null) {
                                if (externalType.toClass() == Number.class) {
                                    break compatibilityCheck;
                                }
                            } else {
                                switch (primExternal.getTypeCode()) {
                                case TypeDesc.INT_CODE:
                                case TypeDesc.LONG_CODE:
                                    break compatibilityCheck;
                                }
                            }
                            break;
                        case TypeDesc.INT_CODE:
                            if (primExternal == null) {
                                if (externalType.toClass() == Number.class) {
                                    break compatibilityCheck;
                                }
                            } else {
                                if (primExternal == TypeDesc.LONG) {
                                    break compatibilityCheck;
                                }
                            }
                            break;
                        case TypeDesc.FLOAT_CODE:
                            if (primExternal == null) {
                                if (externalType.toClass() == Number.class) {
                                    break compatibilityCheck;
                                }
                            } else {
                                if (primExternal == TypeDesc.DOUBLE) {
                                    break compatibilityCheck;
                                }
                            }
                            break;
                        }
                    }

                    errorMessages.add
                        ("Join property internal/external type mismatch for \"" +
                         getName() + "\": internal join \"" + getInternalJoinElement(i).getName() +
                         "\" is of type \"" + getInternalJoinElement(i).getType() +
                         "\" and external join \"" + getExternalJoinElement(i).getName() +
                         "\" is of type \"" + getExternalJoinElement(i).getType() + '"');
                    continue;
                }

                // If this point is reached, then types differ, but they are
                // compatible. Still, a mutator on this join property is not
                // allowed due to the difference.

                if (getWriteMethod() != null) {
                    mutatorAllowed = false;
                    errorMessages.add
                        ("Join property cannot have a mutator if external type cannot " +
                         "be reliably converted to internal type: Mutator = \"" +
                         getWriteMethod() + "\", internal join \"" +
                         getInternalJoinElement(i).getName() +
                         "\" is of type \"" + getInternalJoinElement(i).getType() +
                         "\" and external join \"" + getExternalJoinElement(i).getName() +
                         "\" is of type \"" + getExternalJoinElement(i).getType() + '"');
                }
            }

            if (errorMessages.size() > 0) {
                return;
            }

            // Test which keys of joined object are specified.

            // Create a copy of all the primary keys of joined object.
            Set<StorableProperty> primaryKeys =
                new HashSet<StorableProperty>(joinedInfo.getPrimaryKeyProperties().values());

            // Remove external properties from the primary key set.
            for (int i=0; i<mInternal.length; i++) {
                primaryKeys.remove(getExternalJoinElement(i));
            }

            // Do similar test for alternate keys.

            int altKeyCount = joinedInfo.getAlternateKeyCount();
            List<Set<StorableProperty>> altKeys =
                new ArrayList<Set<StorableProperty>>(altKeyCount);

            altKeyScan:
            for (int i=0; i<altKeyCount; i++) {
                Set<StorableProperty> altKey = new HashSet<StorableProperty>();

                for (OrderedProperty op : joinedInfo.getAlternateKey(i).getProperties()) {
                    ChainedProperty chained = op.getChainedProperty();
                    if (chained.getChainCount() > 0) {
                        // Funny alt key. Pretend it does not exist.
                        continue altKeyScan;
                    }
                    altKey.add(chained.getPrimeProperty());
                }

                altKeys.add(altKey);

                // Remove external properties from the alternate key set.
                for (int j=0; j<mInternal.length; j++) {
                    altKey.remove(getExternalJoinElement(j));
                }
            }

            if (isQuery()) {
                // Key of joined object must not be completely specified.

                if (primaryKeys.size() <= 0) {
                    errorMessages.add
                        ("Join property \"" + getName() +
                         "\" completely specifies primary key of joined object; " +
                         "consider declaring the property type as just " +
                         getJoinedType().getName());
                }

                for (Set<StorableProperty> altKey : altKeys) {
                    if (altKey.size() <= 0) {
                        errorMessages.add
                            ("Join property \"" + getName() +
                             "\" completely specifies an alternate key of joined object; " +
                             "consider declaring the property type as just " +
                             getJoinedType().getName());
                        break;
                    }
                }
            } else {
                // Key of joined object must be completely specified.

                fullKeyCheck:
                {
                    if (primaryKeys.size() <= 0) {
                        break fullKeyCheck;
                    }

                    for (Set<StorableProperty> altKey : altKeys) {
                        if (altKey.size() <= 0) {
                            break fullKeyCheck;
                        }
                    }

                    errorMessages.add
                        ("Join property \"" + getName() +
                         "\" doesn't completely specify any key of joined object; consider " +
                         "declaring the property type as Query<" +
                         getJoinedType().getName() + '>');
                }
            }

            if (mutatorAllowed && getWriteMethod() == null) {
                // Require set method to aid join query optimizations. Without
                // set method, inner join result can be needlessly tossed away.
                errorMessages.add
                    ("Must define proper 'set' method for join property: " + getName());
            }

            if (errorMessages.size() == 0) {
                // No errors, throw away names arrays.
                mInternalNames = null;
                mExternalNames = null;
            }
        }
    }
}
