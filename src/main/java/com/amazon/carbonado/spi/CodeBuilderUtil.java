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
package com.amazon.carbonado.spi;

import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.lang.reflect.Method;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Label;
import org.cojen.classfile.TypeDesc;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodDesc;
import org.cojen.util.ClassInjector;

import com.amazon.carbonado.Storable;

import static com.amazon.carbonado.spi.CommonMethodNames.*;

/**
 * Collection of useful utilities for generating Carbonado code.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public class CodeBuilderUtil {

    /**
     * Generate code to throw an exception if a parameter is null
     * @param b CodeBuilder into which to append the code
     * @param paramIndex index of the parameter to check
     */
    public static void assertParameterNotNull(CodeBuilder b, int paramIndex) {
        b.loadLocal(b.getParameter(paramIndex));
        Label notNull = b.createLabel();
        b.ifNullBranch(notNull, false);
        throwException(b, IllegalArgumentException.class, null);
        notNull.setLocation();
    }

    /**
     * Generate code to create a local variable containing the specified parameter coerced
     * to the specified type.  This is useful for re-interpreting erased generics into
     * the more specific genericized type.
     *
     * @param b CodeBuilder into which to append the code
     * @param paramType the more specific type which was erased during compilation
     * @param paramIndex index of the parameter to unerase
     * @return a local variable referencing the type-cast parameter
     */
    public static LocalVariable uneraseGenericParameter(
            CodeBuilder b, TypeDesc paramType, final int paramIndex)
    {
        b.loadLocal(b.getParameter(paramIndex));
        b.checkCast(paramType);
        LocalVariable result = b.createLocalVariable(null, paramType);
        b.storeLocal(result);
        return result;
    }

    /**
     * Generate code to throw an exception with an optional message.
     * @param b {@link CodeBuilder} to which to add code
     * @param type type of the object to throw
     * @param message optional message to provide to the constructor
     */
    public static void throwException(CodeBuilder b, Class type, String message) {
        TypeDesc desc = TypeDesc.forClass(type);
        b.newObject(desc);
        b.dup();
        if (message == null) {
            b.invokeConstructor(desc, null);
        } else {
            b.loadConstant(message);
            b.invokeConstructor(desc, new TypeDesc[] {TypeDesc.STRING});
        }
        b.throwObject();
    }

    /**
     * Collect a set of all the interfaces and recursively all superclasses for the leaf
     * (genericised class) and root (genericised base class).  Eg, for Object<foo>, all
     * classes and implemented interfaces for every superclass between foo (the leaf) and
     * Object (the base).
     * <P>A copy must be coercible into any of these types, and copy bridge methods must be
     * provided to do so.
     *
     * <P>Note that the official documentation for this is in draft form, and you have to be
     * psychic to have figured out the necessity in the first place.
     *
     * @param set set into which the class types will be collected
     * @param leaf leaf class
     * @return same set as was passed in
     */
    public static Set<Class> gatherAllBridgeTypes(Set<Class> set, Class leaf) {
        set.add(leaf);
        for (Class c : leaf.getInterfaces()) {
            gatherAllBridgeTypes(set, c);
        }
        if ((leaf = leaf.getSuperclass()) != null) {
            gatherAllBridgeTypes(set, leaf);
        }
        return set;
    }

    /**
     * Add copy bridge methods for all classes/interfaces between the leaf (genericised class)
     * and the root (genericised baseclass).
     *
     * @param cf file to which to add the copy bridge
     * @param leaf leaf class
     */
    public static void defineCopyBridges(ClassFile cf, Class leaf) {
        for (Class c : gatherAllBridgeTypes(new HashSet<Class>(), leaf)) {
            if (c != Object.class) {
                defineCopyBridge(cf, c);
            }
        }
    }

    /**
     * Add a copy bridge method to the classfile for the given type.  This is needed to allow
     * the genericised class make a copy itself -- which will be erased to the base type -- and
     * return it as the correct type.
     *
     * @param cf file to which to add the copy bridge
     * @param returnClass type returned from generated bridge method
     */
    public static void defineCopyBridge(ClassFile cf, Class returnClass) {
        TypeDesc returnType = TypeDesc.forClass(returnClass);

        MethodInfo mi = cf.addMethod(Modifiers.PUBLIC.toBridge(true),
                                     COPY_METHOD_NAME, returnType, null);
        CodeBuilder b = new CodeBuilder(mi);
        b.loadThis();
        b.invokeVirtual(COPY_METHOD_NAME, cf.getType(), null);
        b.returnValue(returnType);
    }

    /**
     * Returns a new modifiable mapping of method signatures to methods.
     *
     * @return map of {@link #createSig signatures} to methods
     */
    public static Map<String, Method> gatherAllDeclaredMethods(Class clazz) {
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
     * Define a classfile appropriate for most Storables.  Specifically:
     * <ul>
     * <li>implements Storable</li>
     * <li>implements Cloneable
     * <li>abstract if appropriate
     * <li>marked synthetic
     * <li>targetted for java version 1.5
     * </ul>
     * @param ci ClassInjector for the storable
     * @param type specific Storable implementation to generate
     * @param isAbstract true if the class should be abstract
     * @param aSourcefileName identifier for the classfile, typically the factory class name
     * @return ClassFile object ready to have methods added.
     */
    public static <S extends Storable> ClassFile createStorableClassFile(
            ClassInjector ci, Class<S> type, boolean isAbstract, String aSourcefileName)
    {
        ClassFile cf;
        if (type.isInterface()) {
            cf = new ClassFile(ci.getClassName());
            cf.addInterface(type);
        } else {
            cf = new ClassFile(ci.getClassName(), type);
        }

        if (isAbstract) {
            Modifiers modifiers = cf.getModifiers().toAbstract(true);
            cf.setModifiers(modifiers);
        }
        cf.addInterface(Storable.class);
        cf.addInterface(Cloneable.class);
        cf.markSynthetic();
        cf.setSourceFile(aSourcefileName);
        cf.setTarget("1.5");
        return cf;
    }

    /**
     * Generates code to compare a field in this object against the same one in a
     * different instance. Branch to the provided Label if they are not equal.
     *
     * @param b {@link CodeBuilder} to which to add the code
     * @param fieldName the name of the field
     * @param fieldType the type of the field
     * @param testForNull if true and the values are references, they will be considered
     * unequal unless neither or both are null.  If false, assume neither is null.
     * @param fail the label to branch to
     * @param other the other instance to test
     */
    public static void addEqualsCall(CodeBuilder b,
                                     String fieldName,
                                     TypeDesc fieldType,
                                     boolean testForNull,
                                     Label fail,
                                     LocalVariable other)
    {
        b.loadThis();
        b.loadField(fieldName, fieldType);

        b.loadLocal(other);
        b.loadField(fieldName, fieldType);

        addValuesEqualCall(b, fieldType, testForNull, fail, false);
    }

    /**
     * Generates code to compare two values on the stack, and branch to the
     * provided Label if they are not equal.  Both values must be of the same type.
     *
     * <P>The generated instruction consumes both values on the stack.
     *
     * @param b {@link CodeBuilder} to which to add the code
     * @param valueType the type of the values
     * @param testForNull if true and the values are references, they will be considered
     * unequal unless neither or both are null.  If false, assume neither is null.
     * @param label the label to branch to
     * @param choice when true, branch to label if values are equal, else
     * branch to label if values are unequal.
     */
    public static void addValuesEqualCall(final CodeBuilder b,
                                          final TypeDesc valueType,
                                          final boolean testForNull,
                                          final Label label,
                                          final boolean choice)
    {
        if (valueType.getTypeCode() != TypeDesc.OBJECT_CODE) {
            b.ifComparisonBranch(label, choice ? "==" : "!=", valueType);
            return;
        }

        // Equals method returns zero for false, so if choice is true, branch
        // if not zero. Note that operator selection is opposite when invoking
        // a direct ifComparisonBranch method.
        String equalsBranchOp = choice ? "!=" : "==";

        if (!testForNull) {
            addEqualsCallTo(b, valueType);
            b.ifZeroComparisonBranch(label, equalsBranchOp);
            return;
        }

        Label isNotNull = b.createLabel();
        LocalVariable value = b.createLocalVariable(null, valueType);
        b.storeLocal(value);
        b.loadLocal(value);
        b.ifNullBranch(isNotNull, false);

        // First value popped off stack is null. Just test remaining one for null.
        b.ifNullBranch(label, choice);
        Label cont = b.createLabel();
        b.branch(cont);

        // First value popped off stack is not null, but second one might
        // be. Call equals method, but swap values so that the second value is
        // an argument into the equals method.
        isNotNull.setLocation();
        b.loadLocal(value);
        b.swap();
        addEqualsCallTo(b, valueType);
        b.ifZeroComparisonBranch(label, equalsBranchOp);

        cont.setLocation();
    }

    public static void addEqualsCallTo(CodeBuilder b, TypeDesc fieldType) {
        if (fieldType.isArray()) {
            if (!fieldType.getComponentType().isPrimitive()) {
                TypeDesc type = TypeDesc.forClass(Object[].class);
                b.invokeStatic("java.util.Arrays", "deepEquals",
                               TypeDesc.BOOLEAN, new TypeDesc[] {type, type});
            } else {
                b.invokeStatic("java.util.Arrays", "equals",
                               TypeDesc.BOOLEAN, new TypeDesc[] {fieldType, fieldType});
            }
        } else {
            TypeDesc[] params = {TypeDesc.OBJECT};
            if (fieldType.toClass() != null) {
                if (fieldType.toClass().isInterface()) {
                    b.invokeInterface(fieldType, "equals", TypeDesc.BOOLEAN, params);
                } else {
                    b.invokeVirtual(fieldType, "equals", TypeDesc.BOOLEAN, params);
                }
            } else {
                b.invokeVirtual(TypeDesc.OBJECT, "equals", TypeDesc.BOOLEAN, params);
            }
        }
    }

    /**
     * Create a representation of the signature which includes the method name.
     * This uniquely identifies the method.
     *
     * @param m method to describe
     */
    public static String createSig(Method m) {
        return m.getName() + ':' + MethodDesc.forMethod(m).getDescriptor();
    }

    /**
     * Converts a value on the stack. If "to" type is a String, then conversion
     * may call the String.valueOf(from).
     */
    public static void convertValue(CodeBuilder b, Class from, Class to) {
        if (from == to) {
            return;
        }

        TypeDesc fromType = TypeDesc.forClass(from);
        TypeDesc toType = TypeDesc.forClass(to);

        // Let CodeBuilder have a crack at the conversion first.
        try {
            b.convert(fromType, toType);
            return;
        } catch (IllegalArgumentException e) {
            if (to != String.class && to != Object.class && to != CharSequence.class) {
                throw e;
            }
        }

        // Fallback case is to convert to a String.

        if (fromType.isPrimitive()) {
            b.invokeStatic(TypeDesc.STRING, "valueOf", TypeDesc.STRING, new TypeDesc[]{fromType});
        } else {
            // If object on stack is null, then just leave it alone.
            b.dup();
            Label isNull = b.createLabel();
            b.ifNullBranch(isNull, true);
            b.invokeStatic(TypeDesc.STRING, "valueOf", TypeDesc.STRING,
                           new TypeDesc[]{TypeDesc.OBJECT});
            isNull.setLocation();
        }
    }

    /**
     * Determines which overloaded "with" method on Query should be bound to.
     */
    public static TypeDesc bindQueryParam(Class clazz) {
        if (clazz.isPrimitive()) {
            TypeDesc type = TypeDesc.forClass(clazz);
            switch (type.getTypeCode()) {
            case TypeDesc.INT_CODE:
            case TypeDesc.LONG_CODE:
            case TypeDesc.FLOAT_CODE:
            case TypeDesc.DOUBLE_CODE:
                return type;
            }
        }
        return TypeDesc.OBJECT;
    }
}
