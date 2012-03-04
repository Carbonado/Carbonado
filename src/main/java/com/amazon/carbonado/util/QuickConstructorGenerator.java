/*
 * Copyright 2006-2012 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.WeakIdentityMap;

/**
 * Generates code to invoke constructors. This is a replacement for {@link
 * java.lang.reflect.Constructor} which is easier to use and performs
 * better. In one tested situation, overall performance was improved by about
 * 10%.
 *
 * <p>QuickConstructorGenerator is not general purpose however, as the
 * parameters to the constructor must be known, and the constructor must be
 * public. It is intended to be used for constructing instances of
 * auto-generated classes. The exact parameters may be known at compile time,
 * but the actual object type is not.
 *
 * @author Brian S O'Neill
 * @deprecated use {@link org.cojen.util.QuickConstructorGenerator}
 */
@Deprecated
public class QuickConstructorGenerator {
    // Map<factory class, Map<object type, factory instance>>
    @SuppressWarnings("unchecked")
    private static Map<Class<?>, SoftValuedCache<Class<?>, Object>> cCache = new WeakIdentityMap();

    /**
     * Returns a factory instance for one type of object. Each method in the
     * interface defines a constructor via its parameters. Any checked
     * exceptions declared thrown by the constructor must also be declared by
     * the method. The method return types can be the same type as the
     * constructed object or a supertype.
     *
     * <p>Here is a contrived example for constructing strings. In practice,
     * such a string factory is is useless, since the "new" operator can be
     * invoked directly.
     *
     * <pre>
     * public interface StringFactory {
     *     String newEmptyString();
     *
     *     String newStringFromChars(char[] chars);
     *
     *     String newStringFromBytes(byte[] bytes, String charsetName)
     *         throws UnsupportedEncodingException;
     * }
     * </pre>
     *
     * Here's an example of it being used:
     *
     * <pre>
     * StringFactory sf = QuickConstructorGenerator.getInstance(String.class, StringFactory.class);
     * ...
     * String str = sf.newStringFromChars(new char[] {'h', 'e', 'l', 'l', 'o'});
     * </pre>
     *
     * @param objectType type of object to construct
     * @param factory interface defining which objects can be constructed
     * @throws IllegalArgumentException if factory type is not an interface or
     * if it is malformed
     */
    @SuppressWarnings("unchecked")
    public static synchronized <F> F getInstance(Class<?> objectType, Class<F> factory) {
        try {
            return org.cojen.util.QuickConstructorGenerator.getInstance(objectType, factory);
        } catch (NoClassDefFoundError e) {
            // Use older code instead.
        }

        SoftValuedCache<Class<?>, Object> innerCache = cCache.get(factory);
        if (innerCache == null) {
            innerCache = SoftValuedCache.newCache(7);
            cCache.put(factory, innerCache);
        }
        F instance = (F) innerCache.get(objectType);
        if (instance != null) {
            return instance;
        }

        if (objectType == null) {
            throw new IllegalArgumentException("No object type");
        }
        if (factory == null) {
            throw new IllegalArgumentException("No factory type");
        }
        if (!factory.isInterface()) {
            throw new IllegalArgumentException("Factory must be an interface");
        }

        String prefix = objectType.getName();
        if (prefix.startsWith("java.")) {
            // Defining classes in java packages is restricted.
            int index = prefix.lastIndexOf('.');
            if (index > 0) {
                prefix = prefix.substring(index + 1);
            }
        }
        ClassInjector ci = ClassInjector.create(prefix, objectType.getClassLoader());

        ClassFile cf = null;

        for (Method method : factory.getMethods()) {
            if (!Modifier.isAbstract(method.getModifiers())) {
                continue;
            }

            Constructor ctor;
            try {
                ctor = objectType.getConstructor((Class[]) method.getParameterTypes());
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException(e);
            }

            if (!method.getReturnType().isAssignableFrom(objectType)) {
                throw new IllegalArgumentException
                    ("Method return type must be \"" +
                     objectType.getName() + "\" or supertype: " + method);
            }

            Class<?>[] methodExTypes = method.getExceptionTypes();

            for (Class<?> ctorExType : ctor.getExceptionTypes()) {
                if (RuntimeException.class.isAssignableFrom(ctorExType) ||
                    Error.class.isAssignableFrom(ctorExType)) {
                    continue;
                }
                exCheck: {
                    // Make sure method declares throwing it or a supertype.
                    for (Class<?> methodExType : methodExTypes) {
                        if (methodExType.isAssignableFrom(ctorExType)) {
                            break exCheck;
                        }
                    }
                    throw new IllegalArgumentException("Method must declare throwing \"" +
                                                       ctorExType.getName() +"\": " + method);
                }
            }

            if (cf == null) {
                cf = new ClassFile(ci.getClassName());
                cf.setSourceFile(QuickConstructorGenerator.class.getName());
                cf.setTarget("1.5");
                cf.addInterface(factory);
                cf.markSynthetic();
                cf.addDefaultConstructor();
            }

            // Now define the method that constructs the object.
            CodeBuilder b = new CodeBuilder(cf.addMethod(method));
            b.newObject(TypeDesc.forClass(objectType));
            b.dup();
            int count = b.getParameterCount();
            for (int i=0; i<count; i++) {
                b.loadLocal(b.getParameter(i));
            }
            b.invoke(ctor);
            b.returnValue(TypeDesc.OBJECT);
        }

        if (cf == null) {
            // No methods found to implement.
            throw new IllegalArgumentException("No methods in factory to implement");
        }

        try {
            instance = (F) ci.defineClass(cf).newInstance();
        } catch (IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        } catch (InstantiationException e) {
            throw new UndeclaredThrowableException(e);
        }

        innerCache.put(objectType, instance);

        return instance;
    }
}
