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
package com.amazon.carbonado.gen;

import java.util.HashSet;
import java.util.Set;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Label;
import org.cojen.classfile.TypeDesc;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.Opcode;
import org.cojen.util.ClassInjector;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import static com.amazon.carbonado.gen.CommonMethodNames.*;

/**
 * Collection of useful utilities for generating Carbonado code.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 * @since 1.2
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
     * Generate code to throw an exception with a message concatenated at runtime.
     *
     * @param b {@link CodeBuilder} to which to add code
     * @param type type of the object to throw
     * @param messages messages to concat at runtime
     */
    public static void throwConcatException(CodeBuilder b, Class type, String... messages) {
        if (messages == null || messages.length == 0) {
            throwException(b, type, null);
            return;
        }
        if (messages.length == 1) {
            throwException(b, type, messages[0]);
            return;
        }

        TypeDesc desc = TypeDesc.forClass(type);
        b.newObject(desc);
        b.dup();

        TypeDesc[] params = new TypeDesc[] {TypeDesc.STRING};

        for (int i=0; i<messages.length; i++) {
            b.loadConstant(String.valueOf(messages[i]));
            if (i > 0) {
                b.invokeVirtual(TypeDesc.STRING, "concat", TypeDesc.STRING, params);
            }
        }

        b.invokeConstructor(desc, params);
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
     * Add copy bridge methods for all classes/interfaces between the leaf
     * (genericised class) and the root (genericised baseclass).
     *
     * @param cf file to which to add the copy bridge
     * @param leaf leaf class
     */
    public static void defineCopyBridges(ClassFile cf, Class leaf) {
        for (Class c : gatherAllBridgeTypes(new HashSet<Class>(), leaf)) {
            if (c != Object.class) {
                defineCopyBridge(cf, leaf, c);
            }
        }
    }

    /**
     * Add a copy bridge method to the classfile for the given type.  This is
     * needed to allow the genericised class make a copy itself -- which will
     * be erased to the base type -- and return it as the correct type.
     *
     * @param cf file to which to add the copy bridge
     * @param leaf leaf class
     * @param returnClass type returned from generated bridge method
     */
    private static void defineCopyBridge(ClassFile cf, Class leaf, Class returnClass) {
        TypeDesc returnType = TypeDesc.forClass(returnClass);

        if (isPublicMethodFinal(leaf, COPY_METHOD_NAME, returnType, null)) {
            // Cannot override.
            return;
        }

        MethodInfo mi = cf.addMethod(Modifiers.PUBLIC.toBridge(true),
                                     COPY_METHOD_NAME, returnType, null);
        CodeBuilder b = new CodeBuilder(mi);
        b.loadThis();
        b.invokeVirtual(COPY_METHOD_NAME, cf.getType(), null);
        b.returnValue(returnType);
    }

    /**
     * Defines a Storable prepare method, which assumes that a support field
     * exists and a single-argument constructor exists which accepts a support
     * instance.
     *
     * @param cf file to which to add the prepare method
     * @since 1.2
     */
    public static void definePrepareMethod(ClassFile cf,
                                           Class storableClass,
                                           TypeDesc supportCtorType)
    {
        definePrepareMethod(cf, storableClass, supportCtorType,
                            StorableGenerator.SUPPORT_FIELD_NAME,
                            TypeDesc.forClass(TriggerSupport.class));
    }

    /**
     * Defines a Storable prepare method, which assumes that a support field
     * exists and a single-argument constructor exists which accepts a support
     * instance.
     *
     * @param cf file to which to add the prepare method
     * @since 1.2
     */
    public static void definePrepareMethod(ClassFile cf,
                                           Class storableClass,
                                           TypeDesc supportCtorType,
                                           String supportFieldName,
                                           TypeDesc supportFieldType)
    {
        TypeDesc storableType = TypeDesc.forClass(storableClass);

        if (!isPublicMethodFinal(storableClass, PREPARE_METHOD_NAME, storableType, null)) {
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, PREPARE_METHOD_NAME, cf.getType(), null);

            CodeBuilder b = new CodeBuilder(mi);
            b.newObject(cf.getType());
            b.dup();
            b.loadThis();
            b.loadField(supportFieldName, supportFieldType);
            if (supportFieldType != supportCtorType) {
                b.checkCast(supportCtorType);
            }
            b.invokeConstructor(new TypeDesc[] {supportCtorType});
            b.returnValue(cf.getType());
        }

        definePrepareBridges(cf, storableClass);
    }

    /**
     * Add prepare bridge methods for all classes/interfaces between the leaf
     * (genericised class) and the root (genericised baseclass).
     *
     * @param cf file to which to add the prepare bridge
     * @param leaf leaf class
     * @since 1.2
     */
    public static void definePrepareBridges(ClassFile cf, Class leaf) {
        for (Class c : gatherAllBridgeTypes(new HashSet<Class>(), leaf)) {
            if (c != Object.class) {
                definePrepareBridge(cf, leaf, c);
            }
        }
    }

    /**
     * Add a prepare bridge method to the classfile for the given type.
     *
     * @param cf file to which to add the prepare bridge
     * @param leaf leaf class
     * @param returnClass type returned from generated bridge method
     * @since 1.2
     */
    private static void definePrepareBridge(ClassFile cf, Class leaf, Class returnClass) {
        TypeDesc returnType = TypeDesc.forClass(returnClass);

        if (isPublicMethodFinal(leaf, PREPARE_METHOD_NAME, returnType, null)) {
            // Cannot override.
            return;
        }

        MethodInfo mi = cf.addMethod(Modifiers.PUBLIC.toBridge(true),
                                     PREPARE_METHOD_NAME, returnType, null);
        CodeBuilder b = new CodeBuilder(mi);
        b.loadThis();
        b.invokeVirtual(PREPARE_METHOD_NAME, cf.getType(), null);
        b.returnValue(returnType);
    }

    /**
     * Returns true if a public final method exists which matches the given
     * specification.
     */
    public static boolean isPublicMethodFinal(Class clazz, String name,
                                              TypeDesc retType, TypeDesc[] params)
    {
        if (!clazz.isInterface()) {
            Class[] paramClasses;
            if (params == null || params.length == 0) {
                paramClasses =  null;
            } else {
                paramClasses = new Class[params.length];
                for (int i=0; i<params.length; i++) {
                    paramClasses[i] = params[i].toClass();
                }
            }
            try {
                Method existing = clazz.getMethod(name, paramClasses);
                if (Modifier.isFinal(existing.getModifiers())) {
                    if (retType == null) {
                        retType = TypeDesc.forClass(void.class);
                    }
                    if (TypeDesc.forClass(existing.getReturnType()) == retType) {
                        // Method is already implemented and is final.
                        return true;
                    }
                }
            } catch (NoSuchMethodException e) {
            }
        }

        return false;
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
     * provided Label if they are not equal.  Both values must be of the same
     * type. If they are floating point values, NaN is considered equal to NaN,
     * which is inconsistent with the usual treatment for NaN.
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
            if (valueType.getTypeCode() == TypeDesc.FLOAT_CODE) {
                // Special treatment to handle NaN.
                b.invokeStatic(TypeDesc.FLOAT.toObjectType(), "compare", TypeDesc.INT,
                               new TypeDesc[] {TypeDesc.FLOAT, TypeDesc.FLOAT});
                b.ifZeroComparisonBranch(label, choice ? "==" : "!=");
            } else if (valueType.getTypeCode() == TypeDesc.DOUBLE_CODE) {
                // Special treatment to handle NaN.
                b.invokeStatic(TypeDesc.DOUBLE.toObjectType(), "compare", TypeDesc.INT,
                               new TypeDesc[] {TypeDesc.DOUBLE, TypeDesc.DOUBLE});
                b.ifZeroComparisonBranch(label, choice ? "==" : "!=");
            } else {
                b.ifComparisonBranch(label, choice ? "==" : "!=", valueType);
            }
            return;
        }

        if (!testForNull) {
            String op = addEqualsCallTo(b, valueType, choice);
            b.ifZeroComparisonBranch(label, op);
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

        // First value popped off stack is not null, but second one might be.
        isNotNull.setLocation();
        if (compareToType(valueType) == null) {
            // Call equals method, but swap values so that the second value is
            // an argument into the equals method.
            b.loadLocal(value);
            b.swap();
        } else {
            // Need to test for second argument too, since compareTo method
            // cannot cope with null.
            LocalVariable value2 = b.createLocalVariable(null, valueType);
            b.storeLocal(value2);
            b.loadLocal(value2);
            b.ifNullBranch(label, !choice);
            // Load both values in preparation for calling compareTo method.
            b.loadLocal(value);
            b.loadLocal(value2);
        }
 
        String op = addEqualsCallTo(b, valueType, choice);
        b.ifZeroComparisonBranch(label, op);
 
        cont.setLocation();
    }
 
    /**
     * @param fieldType must be an object type
     * @return null if compareTo should not be called
     */
    private static TypeDesc compareToType(TypeDesc fieldType) {
        if (fieldType.toPrimitiveType() == TypeDesc.FLOAT) {
            // Special treatment to handle NaN.
            return TypeDesc.FLOAT.toObjectType();
        } else if (fieldType.toPrimitiveType() == TypeDesc.DOUBLE) {
            // Special treatment to handle NaN.
            return TypeDesc.DOUBLE.toObjectType();
        } else if (BigDecimal.class.isAssignableFrom(fieldType.toClass())) {
            // Call compareTo to disregard scale.
            return TypeDesc.forClass(BigDecimal.class);
        } else {
            return null;
        }
    }

    /**
     * @param fieldType must be an object type
     * @return zero comparison branch operator
     */
    private static String addEqualsCallTo(CodeBuilder b, TypeDesc fieldType, boolean choice) {
        if (fieldType.isArray()) {
            // FIXME: Array comparisons don't handle desired comparison of NaN.
            if (!fieldType.getComponentType().isPrimitive()) {
                TypeDesc type = TypeDesc.forClass(Object[].class);
                b.invokeStatic("java.util.Arrays", "deepEquals",
                               TypeDesc.BOOLEAN, new TypeDesc[] {type, type});
            } else {
                b.invokeStatic("java.util.Arrays", "equals",
                               TypeDesc.BOOLEAN, new TypeDesc[] {fieldType, fieldType});
            }
            return choice ? "!=" : "==";
        }

        TypeDesc compareToType = compareToType(fieldType);
        if (compareToType != null) {
            b.invokeVirtual(compareToType, "compareTo",
                            TypeDesc.INT, new TypeDesc[] {compareToType});
            return choice ? "==" : "!=";
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
            return choice ? "!=" : "==";
        }
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
     * Generates code to push an initial version property value on the stack.
     *
     * @throws SupportException if version type is not supported
     */
    public static void initialVersion(CodeBuilder b, TypeDesc type, int value)
        throws SupportException
    {
        adjustVersion(b, type, value, false);
    }

    /**
     * Generates code to increment a version property value, already on the stack.
     *
     * @throws SupportException if version type is not supported
     */
    public static void incrementVersion(CodeBuilder b, TypeDesc type)
        throws SupportException
    {
        adjustVersion(b, type, 0, true);
    }

    private static void adjustVersion(CodeBuilder b, TypeDesc type, int value, boolean increment)
        throws SupportException
    {
        TypeDesc primitiveType = type.toPrimitiveType();
        supportCheck: {
            if (primitiveType != null) {
                switch (primitiveType.getTypeCode()) {
                case TypeDesc.INT_CODE:
                case TypeDesc.LONG_CODE:
                    break supportCheck;
                }
            }
            throw new SupportException("Unsupported version type: " + type.getFullName());
        }

        if (!increment) {
            if (primitiveType == TypeDesc.LONG) {
                b.loadConstant((long) value);
            } else {
                b.loadConstant(value);
            }
        } else {
            Label setVersion = b.createLabel();
            if (!type.isPrimitive()) {
                b.dup();
                Label versionNotNull = b.createLabel();
                b.ifNullBranch(versionNotNull, false);
                b.pop();
                if (primitiveType == TypeDesc.LONG) {
                    b.loadConstant(1L);
                } else {
                    b.loadConstant(1);
                }
                b.branch(setVersion);
                versionNotNull.setLocation();
                b.convert(type, primitiveType);
            }
            if (primitiveType == TypeDesc.LONG) {
                b.loadConstant(1L);
                b.math(Opcode.LADD);
            } else {
                b.loadConstant(1);
                b.math(Opcode.IADD);
            }
            setVersion.setLocation();
        }

        b.convert(primitiveType, type);
    }

    /**
     * Generates code to push a blank value to the stack. For objects, it is
     * null, and for primitive types it is zero or false.
     */
    public static void blankValue(CodeBuilder b, TypeDesc type) {
        switch (type.getTypeCode()) {
        default:
            b.loadNull();
            break;

        case TypeDesc.BYTE_CODE:
        case TypeDesc.CHAR_CODE:
        case TypeDesc.SHORT_CODE:
        case TypeDesc.INT_CODE:
            b.loadConstant(0);
            break;

        case TypeDesc.BOOLEAN_CODE:
            b.loadConstant(false);
            break;

        case TypeDesc.LONG_CODE:
            b.loadConstant(0L);
            break;

        case TypeDesc.FLOAT_CODE:
            b.loadConstant(0.0f);
            break;

        case TypeDesc.DOUBLE_CODE:

            b.loadConstant(0.0);
            break;
        }
    }

    /**
     * Determines which overloaded "with" method on Query should be bound to.
     */
    public static TypeDesc bindQueryParam(Class clazz) {
        // This method is a bit vestigial. Once upon a time the Query class did
        // not support all primitive types.
        if (clazz.isPrimitive()) {
            TypeDesc type = TypeDesc.forClass(clazz);
            switch (type.getTypeCode()) {
            case TypeDesc.INT_CODE:
            case TypeDesc.LONG_CODE:
            case TypeDesc.FLOAT_CODE:
            case TypeDesc.DOUBLE_CODE:
            case TypeDesc.BOOLEAN_CODE:
            case TypeDesc.CHAR_CODE:
            case TypeDesc.BYTE_CODE:
            case TypeDesc.SHORT_CODE:
                return type;
            }
        }
        return TypeDesc.OBJECT;
    }

    /**
     * Appends a String to a StringBuilder. A StringBuilder and String must be
     * on the stack, and a StringBuilder is left on the stack after the call.
     */
    public static void callStringBuilderAppendString(CodeBuilder b) {
        // Because of JDK1.5 bug which exposes AbstractStringBuilder class,
        // cannot use reflection to get method signature.
        TypeDesc stringBuilder = TypeDesc.forClass(StringBuilder.class);
        b.invokeVirtual(stringBuilder, "append", stringBuilder, new TypeDesc[] {TypeDesc.STRING});
    }

    /**
     * Appends a char to a StringBuilder. A StringBuilder and char must be on
     * the stack, and a StringBuilder is left on the stack after the call.
     */
    public static void callStringBuilderAppendChar(CodeBuilder b) {
        // Because of JDK1.5 bug which exposes AbstractStringBuilder class,
        // cannot use reflection to get method signature.
        TypeDesc stringBuilder = TypeDesc.forClass(StringBuilder.class);
        b.invokeVirtual(stringBuilder, "append", stringBuilder, new TypeDesc[] {TypeDesc.CHAR});
    }

    /**
     * Calls length on a StringBuilder on the stack, leaving an int on the stack.
     */
    public static void callStringBuilderLength(CodeBuilder b) {
        // Because of JDK1.5 bug which exposes AbstractStringBuilder class,
        // cannot use reflection to get method signature.
        TypeDesc stringBuilder = TypeDesc.forClass(StringBuilder.class);
        b.invokeVirtual(stringBuilder, "length", TypeDesc.INT, null);
    }

    /**
     * Calls setLength on a StringBuilder. A StringBuilder and int must be on
     * the stack, and both are consumed after the call.
     */
    public static void callStringBuilderSetLength(CodeBuilder b) {
        // Because of JDK1.5 bug which exposes AbstractStringBuilder class,
        // cannot use reflection to get method signature.
        TypeDesc stringBuilder = TypeDesc.forClass(StringBuilder.class);
        b.invokeVirtual(stringBuilder, "setLength", null, new TypeDesc[] {TypeDesc.INT});
    }

    /**
     * Calls toString on a StringBuilder. A StringBuilder must be on the stack,
     * and a String is left on the stack after the call.
     */
    public static void callStringBuilderToString(CodeBuilder b) {
        // Because of JDK1.5 bug which exposes AbstractStringBuilder class,
        // cannot use reflection to get method signature.
        TypeDesc stringBuilder = TypeDesc.forClass(StringBuilder.class);
        b.invokeVirtual(stringBuilder, "toString", TypeDesc.STRING, null);
    }
}
