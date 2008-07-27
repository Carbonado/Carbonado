/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.Opcode;
import org.cojen.classfile.TypeDesc;

import org.cojen.util.ClassInjector;
import org.cojen.util.SoftValuedHashMap;

/**
 * General purpose type converter. Custom conversions are possible by supplying
 * an abstract subclass which has public conversion methods whose names begin
 * with "convert". Each conversion method takes a single argument and returns a
 * value.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public abstract class Converter {
    private static final Map<Class, Class<? extends Converter>> cCache = new SoftValuedHashMap();

    /**
     * @param converterType type of converter to generate
     * @throws IllegalArgumentException if converter doesn't a no-arg constructor
     */
    public static <C extends Converter> C build(Class<C> converterType) {
        try {
            return buildClass(converterType).newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException
                ("TypeConverter must have a public no-arg constructor: " + converterType);
        } catch (IllegalAccessException e) {
            // Not expected to happen, since generated constructors are public.
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * @param converterType type of converter to generate
     */
    public static synchronized <C extends Converter> Class<? extends C> buildClass
        (Class<C> converterType)
    {
        Class<? extends C> converterClass = (Class<? extends C>) cCache.get(converterType);
        if (converterClass == null) {
            converterClass = new Builder<C>(converterType).buildClass();
            cCache.put(converterType, converterClass);
        }
        return converterClass;
    }

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(Object from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(byte from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(short from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(int from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(long from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(float from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(double from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(boolean from, Class<T> toType);

    /**
     * @throws IllegalArgumentException if conversion is not supported
     */
    public abstract <T> T convert(char from, Class<T> toType);

    protected IllegalArgumentException conversionNotSupported
        (Object fromValue, Class fromType, Class toType)
    {
        StringBuilder b = new StringBuilder();

        if (fromType == null && fromValue != null) {
            fromType = fromValue.getClass();
        }

        if (fromValue == null) {
            b.append("Actual value null cannot be converted to type ");
        } else {
            b.append("Actual value \"");
            b.append(String.valueOf(fromValue));
            b.append("\", of type \"");
            b.append(TypeDesc.forClass(fromType).getFullName());
            b.append("\", cannot be converted to expected type of ");
        }

        if (toType == null) {
            b.append("null");
        } else {
            b.append('"');
            b.append(TypeDesc.forClass(toType).getFullName());
            b.append('"');
        }

        return new IllegalArgumentException(b.toString());
    }

    private static class Builder<C extends Converter> {
        private final Class<C> mConverterType;

        // Map "from class" to "to class" to optional conversion method.
        private final Map<Class, Map<Class, Method>> mConvertMap;

        private final Class[][] mBoxMatrix = {
            {byte.class, Byte.class, Number.class, Object.class},
            {short.class, Short.class, Number.class, Object.class},
            {int.class, Integer.class, Number.class, Object.class},
            {long.class, Long.class, Number.class, Object.class},
            {float.class, Float.class, Number.class, Object.class},
            {double.class, Double.class, Number.class, Object.class},
            {boolean.class, Boolean.class, Object.class},
            {char.class, Character.class, Object.class},
        };

        private ClassFile mClassFile;

        private int mInnerConvertCounter;

        Builder(Class<C> converterType) {
            if (!Converter.class.isAssignableFrom(converterType)) {
                throw new IllegalArgumentException("Not a TypeConverter: " + converterType);
            }

            mConverterType = converterType;
            mConvertMap = new HashMap<Class, Map<Class, Method>>();

            // Add built-in primitive boxing/unboxing conversions.
            for (Class[] tuple : mBoxMatrix) {
                Map<Class, Method> to = new HashMap<Class, Method>();
                for (Class toType : tuple) {
                    to.put(toType, null);
                }
                mConvertMap.put(tuple[0], to);
                mConvertMap.put(tuple[1], to);
            }

            for (Method m : converterType.getMethods()) {
                if (!m.getName().startsWith("convert")) {
                    continue;
                }
                Class toType = m.getReturnType();
                if (toType == null || toType == void.class) {
                    continue;
                }
                Class[] params = m.getParameterTypes();
                if (params == null || params.length != 1) {
                    continue;
                }

                Map<Class, Method> to = mConvertMap.get(params[0]);
                if (to == null) {
                    to = new HashMap<Class, Method>();
                    mConvertMap.put(params[0], to);
                }

                to.put(toType, m);
            }

            // Add automatic widening conversions.

            // Copy to prevent concurrent modification.
            Map<Class, Map<Class, Method>> convertMap =
                new HashMap<Class, Map<Class, Method>>(mConvertMap);

            for (Map.Entry<Class, Map<Class, Method>> entry : convertMap.entrySet()) {
                Class fromType = entry.getKey();

                // Copy to prevent concurrent modification.
                Map<Class, Method> toMap = new HashMap<Class, Method>(entry.getValue());

                for (Map.Entry<Class, Method> to : toMap.entrySet()) {
                    Class toType = to.getKey();
                    Method conversionMethod = to.getValue();
                    addAutomaticConversion(fromType, toType, conversionMethod);
                }
            }

            /*
            for (Map.Entry<Class, Map<Class, Method>> entry : mConvertMap.entrySet()) {
                Class fromType = entry.getKey();
                for (Map.Entry<Class, Method> to : entry.getValue().entrySet()) {
                    Class toType = to.getKey();
                    Method conversionMethod = to.getValue();
                    System.out.println("from: " + fromType.getName() + ", to: " +
                                       toType.getName() + ", via: " + conversionMethod);
                }
            }
            */
        }

        Class<? extends C> buildClass() {
            ClassInjector ci = ClassInjector
                .create(mConverterType.getName(), mConverterType.getClassLoader());

            mClassFile = new ClassFile(ci.getClassName(), mConverterType);
            mClassFile.markSynthetic();
            mClassFile.setSourceFile(Converter.class.getName());
            mClassFile.setTarget("1.5");

            // Add constructors which match superclass.
            int ctorCount = 0;
            for (Constructor ctor : mConverterType.getDeclaredConstructors()) {
                int modifiers = ctor.getModifiers();
                if (!Modifier.isPublic(modifiers) && !Modifier.isProtected(modifiers)) {
                    continue;
                }

                ctorCount++;

                TypeDesc[] params = new TypeDesc[ctor.getParameterTypes().length];
                for (int i=0; i<params.length; i++) {
                    params[i] = TypeDesc.forClass(ctor.getParameterTypes()[i]);
                }

                MethodInfo mi = mClassFile.addConstructor(Modifiers.PUBLIC, params);
                CodeBuilder b = new CodeBuilder(mi);

                b.loadThis();
                for (int i=0; i<params.length; i++) {
                    b.loadLocal(b.getParameter(0));
                }
                b.invokeSuperConstructor(params);
                b.returnVoid();
            }

            if (ctorCount == 0) {
                throw new IllegalArgumentException
                    ("TypeConverter has no public or protected constructors: " + mConverterType);
            }

            addPrimitiveConvertMethod(byte.class);
            addPrimitiveConvertMethod(short.class);
            addPrimitiveConvertMethod(int.class);
            addPrimitiveConvertMethod(long.class);
            addPrimitiveConvertMethod(float.class);
            addPrimitiveConvertMethod(double.class);
            addPrimitiveConvertMethod(boolean.class);
            addPrimitiveConvertMethod(char.class);

            Method m = getAbstractConvertMethod(Object.class);
            if (m != null) {
                CodeBuilder b = new CodeBuilder(mClassFile.addMethod(m));

                b.loadLocal(b.getParameter(0));
                Label notNull = b.createLabel();
                b.ifNullBranch(notNull, false);
                b.loadNull();
                b.returnValue(TypeDesc.OBJECT);

                notNull.setLocation();
                addConversionSwitch(b, null);
            }

            return ci.defineClass(mClassFile);
        }

        private void addPrimitiveConvertMethod(Class fromType) {
            Method m = getAbstractConvertMethod(fromType);
            if (m == null) {
                return;
            }

            CodeBuilder b = new CodeBuilder(mClassFile.addMethod(m));

            addConversionSwitch(b, fromType);
        }

        private void addConversionSwitch(CodeBuilder b, Class fromType) {
            Map<Class, Method> toMap;
            Map<Class, ?> caseMap;

            if (fromType == null) {
                Map<Class, Map<Class, Method>> convertMap =
                    new HashMap<Class, Map<Class, Method>>(mConvertMap);
                // Remove primitive type cases, since they will never match.
                Iterator<Class> it = convertMap.keySet().iterator();
                while (it.hasNext()) {
                    if (it.next().isPrimitive()) {
                        it.remove();
                    }
                }

                toMap = null;
                caseMap = convertMap;
            } else {
                toMap = mConvertMap.get(fromType);
                caseMap = toMap;
            }

            Map<Integer, List<Class>> caseMatches = new HashMap<Integer, List<Class>>();

            for (Class to : caseMap.keySet()) {
                int caseValue = to.hashCode();
                List<Class> matches = caseMatches.get(caseValue);
                if (matches == null) {
                    matches = new ArrayList<Class>();
                    caseMatches.put(caseValue, matches);
                }
                matches.add(to);
            }

            int[] cases = new int[caseMatches.size()];
            Label[] switchLabels = new Label[caseMatches.size()];
            Label noMatch = b.createLabel();

            {
                int i = 0;
                for (Integer caseValue : caseMatches.keySet()) {
                    cases[i] = caseValue;
                    switchLabels[i] = b.createLabel();
                    i++;
                }
            }

            final TypeDesc classType = TypeDesc.forClass(Class.class);

            LocalVariable caseVar;
            if (toMap == null) {
                b.loadLocal(b.getParameter(0));
                b.invokeVirtual(TypeDesc.OBJECT, "getClass", classType, null);
                caseVar = b.createLocalVariable(null, classType);
                b.storeLocal(caseVar);
            } else {
                caseVar = b.getParameter(1);
            }

            if (caseMap.size() > 1) {
                b.loadLocal(caseVar);
                b.invokeVirtual(Class.class.getName(), "hashCode", TypeDesc.INT, null);
                b.switchBranch(cases, switchLabels, noMatch);
            }

            TypeDesc fromTypeDesc = TypeDesc.forClass(fromType);

            int i = 0;
            for (List<Class> matches : caseMatches.values()) {
                switchLabels[i].setLocation();

                int matchCount = matches.size();
                for (int j=0; j<matchCount; j++) {
                    Class toType = matches.get(j);
                    TypeDesc toTypeDesc = TypeDesc.forClass(toType);

                    // Test against class instance to find exact match.

                    b.loadConstant(toTypeDesc);
                    b.loadLocal(caseVar);
                    Label notEqual;
                    if (j == matchCount - 1) {
                        notEqual = null;
                        b.ifEqualBranch(noMatch, false);
                    } else {
                        notEqual = b.createLabel();
                        b.ifEqualBranch(notEqual, false);
                    }

                    if (toMap == null) {
                        // Switch in a switch, but do so in a separate method
                        // to keep this one small.

                        String name = "convert$" + (++mInnerConvertCounter);
                        TypeDesc[] params = {toTypeDesc, classType};
                        {
                            MethodInfo mi = mClassFile.addMethod
                                (Modifiers.PRIVATE, name, TypeDesc.OBJECT, params);
                            CodeBuilder b2 = new CodeBuilder(mi);
                            addConversionSwitch(b2, toType);
                        }

                        b.loadThis();
                        b.loadLocal(b.getParameter(0));
                        b.checkCast(toTypeDesc);
                        b.loadLocal(b.getParameter(1));
                        b.invokePrivate(name, TypeDesc.OBJECT, params);
                        b.returnValue(TypeDesc.OBJECT);
                    } else {
                        Method convertMethod = toMap.get(toType);

                        if (convertMethod == null) {
                            b.loadLocal(b.getParameter(0));
                            TypeDesc fromPrimDesc = fromTypeDesc.toPrimitiveType();
                            if (fromPrimDesc != null) {
                                b.convert(fromTypeDesc, fromPrimDesc);
                                b.convert(fromPrimDesc, toTypeDesc.toObjectType());
                            } else {
                                b.convert(fromTypeDesc, toTypeDesc.toObjectType());
                            }
                        } else {
                            b.loadThis();
                            b.loadLocal(b.getParameter(0));
                            Class paramType = convertMethod.getParameterTypes()[0];
                            b.convert(fromTypeDesc, TypeDesc.forClass(paramType));
                            b.invoke(convertMethod);
                            TypeDesc retType = TypeDesc.forClass(convertMethod.getReturnType());
                            b.convert(retType, toTypeDesc.toObjectType());
                        }

                        b.returnValue(TypeDesc.OBJECT);
                    }

                    if (notEqual != null) {
                        notEqual.setLocation();
                    }
                }

                i++;
            }

            noMatch.setLocation();

            final TypeDesc valueType = b.getParameter(0).getType();

            if (fromType == null) {
                // Check if object is already the desired type.

                b.loadLocal(b.getParameter(1));
                b.loadLocal(b.getParameter(0));
                b.invokeVirtual(classType, "isInstance", TypeDesc.BOOLEAN,
                                new TypeDesc[] {TypeDesc.OBJECT});
                Label notSupported = b.createLabel();
                b.ifZeroComparisonBranch(notSupported, "==");
                b.loadLocal(b.getParameter(0));
                b.convert(valueType, valueType.toObjectType());
                b.returnValue(TypeDesc.OBJECT);

                notSupported.setLocation();
            }

            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.convert(valueType, valueType.toObjectType());
            if (valueType.isPrimitive()) {
                b.loadConstant(valueType);
            } else {
                b.loadNull();
            }
            b.loadLocal(b.getParameter(1));
            b.invokeVirtual("conversionNotSupported",
                            TypeDesc.forClass(IllegalArgumentException.class),
                            new TypeDesc[] {TypeDesc.OBJECT, classType, classType});
            b.throwObject();
        }

        /**
         * @return null if should not be defined
         */
        private Method getAbstractConvertMethod(Class fromType) {
            Method m;
            try {
                m = mConverterType.getMethod("convert", fromType, Class.class);
            } catch (NoSuchMethodException e) {
                return null;
            }
            if (!Modifier.isAbstract(m.getModifiers())) {
                return null;
            }
            return m;
        }

        private void addAutomaticConversion(Class fromType, Class toType, Method method) {
            if (method != null) {
                Class paramType = method.getParameterTypes()[0];
                if (!paramType.isAssignableFrom(fromType)) {
                    if (!fromType.isPrimitive() && paramType.isPrimitive()) {
                        // Reject because unboxing could result in NullPointerException.
                        return;
                    }
                    if (!new ConversionComparator(fromType).isConversionPossible(paramType)) {
                        // Reject.
                        return;
                    }
                }

                Class returnType = method.getReturnType();
                if (!toType.isAssignableFrom(returnType)) {
                    if (!returnType.isPrimitive() && toType.isPrimitive()) {
                        // Reject because unboxing could result in NullPointerException.
                        return;
                    }
                    if (TypeDesc.forClass(returnType).toObjectType() !=
                        TypeDesc.forClass(toType).toObjectType())
                    {
                        // Reject widening or narrowing return type.
                        return;
                    }
                }
            }

            addConversionIfNotExists(fromType, toType, method);

            // Add no-op conversions.
            addConversionIfNotExists(fromType, fromType, null);
            addConversionIfNotExists(toType, toType, null);

            for (Class[] pair : mBoxMatrix) {
                if (fromType == pair[0]) {
                    addConversionIfNotExists(pair[1], toType, method);
                    if (toType == pair[1]) {
                        addConversionIfNotExists(pair[1], pair[0], method);
                    }
                } else if (fromType == pair[1]) {
                    addConversionIfNotExists(pair[0], toType, method);
                    if (toType == pair[1]) {
                        addConversionIfNotExists(pair[0], pair[1], method);
                    }
                }
                if (toType == pair[0]) {
                    addConversionIfNotExists(fromType, pair[1], method);
                }
            }

            if (fromType == short.class || fromType == Short.class) {
                addAutomaticConversion(byte.class, toType, method);
            } else if (fromType == int.class || fromType == Integer.class) {
                addAutomaticConversion(short.class, toType, method);
            } else if (fromType == long.class || fromType == Long.class) {
                addAutomaticConversion(int.class, toType, method);
            } else if (fromType == float.class || fromType == Float.class) {
                addAutomaticConversion(short.class, toType, method);
            } else if (fromType == double.class || fromType == Double.class) {
                addAutomaticConversion(int.class, toType, method);
                addAutomaticConversion(float.class, toType, method);
            }

            if (toType == byte.class || toType == Byte.class) {
                addAutomaticConversion(fromType, Short.class, method);
            } else if (toType == short.class || toType == Short.class) {
                addAutomaticConversion(fromType, Integer.class, method);
                addAutomaticConversion(fromType, Float.class, method);
            } else if (toType == int.class || toType == Integer.class) {
                addAutomaticConversion(fromType, Long.class, method);
                addAutomaticConversion(fromType, Double.class, method);
            } else if (toType == float.class || toType == Float.class) {
                addAutomaticConversion(fromType, Double.class, method);
            }
        }

        private boolean addConversionIfNotExists(Class fromType, Class toType, Method method) {
            Map<Class, Method> to = mConvertMap.get(fromType);
            if (to == null) {
                to = new HashMap<Class, Method>();
                mConvertMap.put(fromType, to);
            }
            Method existing = to.get(toType);
            if (existing != null) {
                if (method == null) {
                    return false;
                }
                ConversionComparator cc = new ConversionComparator(fromType);
                Class existingFromType = existing.getParameterTypes()[0];
                Class candidateFromType = method.getParameterTypes()[0];
                if (cc.compare(existingFromType, candidateFromType) <= 0) {
                    return false;
                }
            }
            to.put(toType, method);
            return true;
        }
    }
}
