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

package com.amazon.carbonado.cursor;

import java.lang.reflect.Method;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeAssembler;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.Opcode;
import org.cojen.classfile.TypeDesc;
import static org.cojen.classfile.TypeDesc.*;

import org.cojen.util.ClassInjector;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.ExistsFilter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.util.ThrowUnchecked;
import com.amazon.carbonado.util.QuickConstructorGenerator;

import com.amazon.carbonado.gen.CodeBuilderUtil;


/**
 * Generates Cursor implementations that wrap another Cursor, applying a
 * property matching filter on each result.
 *
 * @author Brian S O'Neill
 * @see FilteredCursor
 */
class FilteredCursorGenerator {
    private static final String SUB_FILTER_INIT_METHOD = "subFilterInit$";

    private static Map cCache = new WeakIdentityMap();

    /**
     * Returns a factory for creating new filtered cursor instances.
     *
     * @param filter filter specification
     * @throws IllegalArgumentException if filter is null
     * @throws UnsupportedOperationException if filter is not supported
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> Factory<S> getFactory(Filter<S> filter) {
        if (filter == null) {
            throw new IllegalArgumentException();
        }
        synchronized (cCache) {
            Factory<S> factory = (Factory<S>) cCache.get(filter);
            if (factory != null) {
                return factory;
            }
            Class<Cursor<S>> clazz = generateClass(filter);
            factory = QuickConstructorGenerator.getInstance(clazz, Factory.class);
            cCache.put(filter, factory);
            return factory;
        }
    }

    @SuppressWarnings("unchecked")
    private static <S extends Storable> Class<Cursor<S>> generateClass(Filter<S> filter) {
        filter.accept(new Validator<S>(), null);

        Class<S> type = filter.getStorableType();

        String packageName;
        {
            String name = type.getName();
            int index = name.lastIndexOf('.');
            if (index >= 0) {
                packageName = name.substring(0, index);
            } else {
                packageName = "";
            }
        }

        // Try to generate against the same loader as the storable type. This
        // allows the generated class access to it, preventing a
        // NoClassDefFoundError.
        ClassLoader loader = type.getClassLoader();

        ClassInjector ci = ClassInjector.create(packageName + ".FilteredCursor", loader);
        ClassFile cf = new ClassFile(ci.getClassName(), FilteredCursor.class);
        cf.markSynthetic();
        cf.setSourceFile(FilteredCursorGenerator.class.getName());
        cf.setTarget("1.5");

        // Begin constructor definition.
        CodeBuilder ctorBuilder;
        {
            TypeDesc cursorType = TypeDesc.forClass(Cursor.class);
            TypeDesc objectArrayType = TypeDesc.forClass(Object[].class);
            TypeDesc[] params = {cursorType, objectArrayType};
            MethodInfo ctor = cf.addConstructor(Modifiers.PUBLIC, params);
            ctorBuilder = new CodeBuilder(ctor);
            ctorBuilder.loadThis();
            ctorBuilder.loadLocal(ctorBuilder.getParameter(0));
            ctorBuilder.invokeSuperConstructor(new TypeDesc[] {cursorType});
        }

        // Begin isAllowed method definition.
        CodeBuilder isAllowedBuilder;
        LocalVariable storableVar;
        {
            TypeDesc[] params = {OBJECT};
            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, "isAllowed", BOOLEAN, params);
            isAllowedBuilder = new CodeBuilder(mi);

            storableVar = isAllowedBuilder.getParameter(0);

            // Filter out any instances of null. Shouldn't happen though.
            isAllowedBuilder.loadLocal(storableVar);
            Label notNull = isAllowedBuilder.createLabel();
            isAllowedBuilder.ifNullBranch(notNull, false);
            isAllowedBuilder.loadConstant(false);
            isAllowedBuilder.returnValue(BOOLEAN);
            notNull.setLocation();

            // Cast the parameter to the expected type.
            isAllowedBuilder.loadLocal(storableVar);
            TypeDesc storableType = TypeDesc.forClass(type);
            isAllowedBuilder.checkCast(storableType);
            storableVar = isAllowedBuilder.createLocalVariable("storable", storableType);
            isAllowedBuilder.storeLocal(storableVar);
        }

        // Capture property filter ordinals before optimization scrambles them.
        Map<PropertyFilter, Integer> propertyOrdinalMap;
        {
            PropertyOrdinalMapVisitor<S> visitor = new PropertyOrdinalMapVisitor<S>();
            filter.accept(visitor, null);
            propertyOrdinalMap = visitor.getPropertyOrdinalMap();
        }

        filter = ShortCircuitOptimizer.optimize(filter);

        CodeGen<S> cg = new CodeGen<S>
            (propertyOrdinalMap, cf, ctorBuilder, isAllowedBuilder, storableVar);
        filter.accept(cg, null);

        List<Filter> subFilters = cg.finishSubFilterInit();

        // Finish constructor.
        ctorBuilder.returnVoid();

        Class generated = ci.defineClass(cf);

        // Pass sub-filter instances to be stored in static fields.
        if (subFilters != null && subFilters.size() > 0) {
            try {
                Method init = generated.getMethod(SUB_FILTER_INIT_METHOD, Filter[].class);
                init.invoke(null, (Object) subFilters.toArray(new Filter[subFilters.size()]));
            } catch (Exception e) {
                ThrowUnchecked.fireDeclaredRootCause(e);
            }
        }

        return (Class<Cursor<S>>) generated;
    }

    public static interface Factory<S extends Storable> {
        /**
         * @param cursor cursor to wrap and filter
         * @param filterValues values corresponding to original filter used to create this factory
         */
        Cursor<S> newFilteredCursor(Cursor<S> cursor, Object... filterValues);
    }

    /**
     * Ensures properties can be read and that relational operation is
     * supported.
     */
    private static class Validator<S extends Storable> extends Visitor<S, Object, Object> {
        Validator() {
        }

        @Override
        public Object visit(PropertyFilter<S> filter, Object param) {
            ChainedProperty<S> chained = filter.getChainedProperty();

            switch (filter.getOperator()) {
            case EQ: case NE:
                // Use equals() method instead of comparator.
                break;
            default:
                TypeDesc typeDesc = TypeDesc.forClass(chained.getType()).toObjectType();

                if (!Comparable.class.isAssignableFrom(typeDesc.toClass())) {
                    throw new UnsupportedOperationException
                        ("Property \"" + chained + "\" does not implement Comparable");
                }
                break;
            }

            // Follow the chain, verifying that each property has an accessible
            // read method.

            checkForReadMethod(chained, chained.getPrimeProperty());
            for (int i=0; i<chained.getChainCount(); i++) {
                checkForReadMethod(chained, chained.getChainedProperty(i));
            }

            return null;
        }

        private void checkForReadMethod(ChainedProperty<S> chained,
                                        StorableProperty<?> property) {
            if (property.getReadMethod() == null) {
                String msg;
                if (chained.getChainCount() == 0) {
                    msg = "Property \"" + property.getName() + "\" cannot be read";
                } else {
                    msg = "Property \"" + property.getName() + "\" of \"" + chained +
                        "\" cannot be read";
                }
                throw new UnsupportedOperationException(msg);
            }
        }
    }

    private static class CodeGen<S extends Storable> extends Visitor<S, Object, Object> {
        private static final String FIELD_PREFIX = "value$";
        private static final String FILTER_FIELD_PREFIX = "filter$";

        private final Map<PropertyFilter, Integer> mPropertyOrdinalMap;
        private final ClassFile mClassFile;
        private final CodeBuilder mCtorBuilder;
        private final CodeBuilder mIsAllowedBuilder;
        private final LocalVariable mStorableVar;

        private final Stack<Scope> mScopeStack;

        private final Map<PropertyFilter, Object> mGeneratedPropertyFilters;

        private List<Filter> mSubFilters;
        private Map<Filter, String> mGeneratedSubFilters;
        private CodeBuilder mSubFilterInitBuilder;

        CodeGen(Map<PropertyFilter, Integer> propertyOrdinalMap,
                ClassFile cf,
                CodeBuilder ctorBuilder,
                CodeBuilder isAllowedBuilder,
                LocalVariable storableVar)
        {
            mPropertyOrdinalMap = propertyOrdinalMap;
            mClassFile = cf;
            mCtorBuilder = ctorBuilder;
            mIsAllowedBuilder = isAllowedBuilder;
            mStorableVar = storableVar;
            mScopeStack = new Stack<Scope>();
            mScopeStack.push(new Scope(null, null));
            mGeneratedPropertyFilters = new IdentityHashMap<PropertyFilter, Object>();
        }

        public List<Filter> finishSubFilterInit() {
            if (mSubFilterInitBuilder != null) {
                mSubFilterInitBuilder.returnVoid();
            }
            if (mSubFilters == null) {
                return Collections.emptyList();
            }
            return mSubFilters;
        }

        @Override
        public Object visit(OrFilter<S> filter, Object param) {
            Label failLocation = mIsAllowedBuilder.createLabel();
            // Inherit success location to short-circuit if 'or' test succeeds.
            mScopeStack.push(new Scope(failLocation, getScope().mSuccessLocation));
            filter.getLeftFilter().accept(this, param);
            failLocation.setLocation();
            mScopeStack.pop();
            filter.getRightFilter().accept(this, param);
            return null;
        }

        @Override
        public Object visit(AndFilter<S> filter, Object param) {
            Label successLocation = mIsAllowedBuilder.createLabel();
            // Inherit fail location to short-circuit if 'and' test fails.
            mScopeStack.push(new Scope(getScope().mFailLocation, successLocation));
            filter.getLeftFilter().accept(this, param);
            successLocation.setLocation();
            mScopeStack.pop();
            filter.getRightFilter().accept(this, param);
            return null;
        }

        @Override
        public Object visit(PropertyFilter<S> filter, Object param) {
            TypeDesc type = TypeDesc.forClass(filter.getChainedProperty().getType());
            String fieldName = addFilterField(filter, type);
            loadChainedProperty(mIsAllowedBuilder, filter.getChainedProperty());
            addPropertyFilter(mIsAllowedBuilder, fieldName, type, filter.getOperator());
            return null;
        }

        @Override
        public Object visit(ExistsFilter<S> filter, Object param) {
            // Load join property value to stack.
            CodeBuilder b = mIsAllowedBuilder;
            loadChainedProperty(b, filter.getChainedProperty());

            if (!filter.getChainedProperty().getLastProperty().isQuery()) {
                // Checking for existence or non-existence of many-to-one join.
                // Implement by comparing against null.
                getScope().successIfNullElseFail(b, filter.isNotExists());
                return null;
            }
            
            // Recursively gather all the properties to be passed to sub-filter.
            final List<PropertyFilter> subPropFilters = new ArrayList<PropertyFilter>();

            filter.getSubFilter().accept(new Visitor() {
                @Override
                public Object visit(PropertyFilter filter, Object param) {
                    subPropFilters.add(filter);
                    return null;
                }
                @Override
                public Object visit(ExistsFilter filter, Object param) {
                    return filter.getSubFilter().accept(this, param);
                }
            }, null);

            final TypeDesc queryType = TypeDesc.forClass(Query.class);

            // Refine Query filter, if sub-filter isn't open.
            if (!filter.getSubFilter().isOpen()) {
                String subFilterFieldName = addStaticFilterField(filter.getSubFilter());

                TypeDesc filterType = TypeDesc.forClass(Filter.class);

                b.loadStaticField(subFilterFieldName, filterType);
                b.invokeInterface(queryType, "and", queryType, new TypeDesc[] {filterType});

                for (PropertyFilter subPropFilter : subPropFilters) {
                    String fieldName = addFilterField(subPropFilter, OBJECT);
                    // Pass value to Query.
                    b.loadThis();
                    b.loadField(fieldName, OBJECT);
                    b.invokeInterface(queryType, "with", queryType, new TypeDesc[] {OBJECT});
                }
            }

            // Call the all-important Query.exists method.
            b.invokeInterface(queryType, "exists", BOOLEAN, null);

            // Success if boolean value is true (non-zero), opposite for "not exists".
            RelOp op = filter.isNotExists() ? RelOp.EQ : RelOp.NE;
            getScope().successIfZeroComparisonElseFail(b, op);

            return null;
        }

        private String addFilterField(PropertyFilter filter, TypeDesc type) {
            final int propertyOrdinal = mPropertyOrdinalMap.get(filter);
            final String fieldName = FIELD_PREFIX + propertyOrdinal;

            if (mGeneratedPropertyFilters.containsKey(filter)) {
                return fieldName;
            }

            final TypeDesc fieldType = actualFieldType(type);

            mClassFile.addField(Modifiers.PRIVATE.toFinal(true), fieldName, fieldType);

            // Add code to constructor to store value into field.
            {
                CodeBuilder b = mCtorBuilder;
                b.loadThis();
                b.loadLocal(b.getParameter(1));
                b.loadConstant(propertyOrdinal);
                b.loadFromArray(OBJECT);
                if (type != OBJECT) {
                    b.checkCast(type.toObjectType());
                    convertProperty(b, type.toObjectType(), fieldType);
                }
                b.storeField(fieldName, fieldType);
            }

            mGeneratedPropertyFilters.put(filter, filter);
            return fieldName;
        }

        private String addStaticFilterField(Filter filter) {
            if (mSubFilters == null) {
                mSubFilters = new ArrayList<Filter>();
                mGeneratedSubFilters = new IdentityHashMap<Filter, String>();
            }

            if (mGeneratedSubFilters.containsKey(filter)) {
                return mGeneratedSubFilters.get(filter);
            }

            final int filterOrdinal = mSubFilters.size();
            final String fieldName = FILTER_FIELD_PREFIX + filterOrdinal;
            final TypeDesc filterType = TypeDesc.forClass(Filter.class);

            mClassFile.addField(Modifiers.PRIVATE.toStatic(true), fieldName, filterType);

            mSubFilters.add(filter);

            if (mSubFilterInitBuilder == null) {
                TypeDesc filterArrayType = filterType.toArrayType();
                mSubFilterInitBuilder = new CodeBuilder
                    (mClassFile.addMethod
                     (Modifiers.PUBLIC.toStatic(true),
                      SUB_FILTER_INIT_METHOD, null, new TypeDesc[] {filterArrayType}));

                // This method must be public, so add a check to ensure it is
                // called at most once. Just check one filter to see if it is non-null.

                mSubFilterInitBuilder.loadStaticField(fieldName, filterType);
                Label isNull = mSubFilterInitBuilder.createLabel();
                mSubFilterInitBuilder.ifNullBranch(isNull, true);
                CodeBuilderUtil.throwException
                    (mSubFilterInitBuilder, IllegalStateException.class, null);
                isNull.setLocation();
            }

            // Now add code to init field later.
            mSubFilterInitBuilder.loadLocal(mSubFilterInitBuilder.getParameter(0));
            mSubFilterInitBuilder.loadConstant(filterOrdinal);
            mSubFilterInitBuilder.loadFromArray(filterType);
            mSubFilterInitBuilder.storeStaticField(fieldName, filterType);

            mGeneratedSubFilters.put(filter, fieldName);

            return fieldName;
        }

        private Scope getScope() {
            return mScopeStack.peek();
        }

        /**
         * Generated code checks if chained properties resolve to null, and if
         * so, branches to the current scope's fail or success location.
         */
        private void loadChainedProperty(CodeBuilder b, ChainedProperty<?> chained) {
            b.loadLocal(mStorableVar);
            loadProperty(b, chained.getPrimeProperty());
            for (int i=0; i<chained.getChainCount(); i++) {
                // Check if last loaded property was null. Fail for inner join,
                // success for outer join.
                b.dup();
                Label notNull = b.createLabel();
                b.ifNullBranch(notNull, false);
                b.pop();
                if (chained.isOuterJoin(i)) {
                    getScope().success(b);
                } else {
                    getScope().fail(b);
                }
                notNull.setLocation();

                // Now load next property in chain.
                loadProperty(b, chained.getChainedProperty(i));
            }
        }

        private void loadProperty(CodeBuilder b, StorableProperty<?> property) {
            Method readMethod = property.getReadMethod();
            if (readMethod == null) {
                // Invoke synthetic package accessible method.
                String readName = property.getReadMethodName();
                try {
                    readMethod = property.getEnclosingType().getDeclaredMethod(readName);
                } catch (NoSuchMethodException e) {
                    // This shouldn't happen since it was checked for earlier.
                    throw new IllegalArgumentException
                        ("Property \"" + property.getName() + "\" cannot be read");
                }
            }
            b.invoke(readMethod);
        }

        private void addPropertyFilter(CodeBuilder b,
                                       String fieldName, TypeDesc type, RelOp relOp)
        {
            TypeDesc fieldType = actualFieldType(type);

            if (type.getTypeCode() == OBJECT_CODE) {
                // Check if actual property being examined is null.

                LocalVariable actualValue = b.createLocalVariable("temp", type);
                b.storeLocal(actualValue);
                b.loadLocal(actualValue);
                Label notNull = b.createLabel();

                switch (relOp) {
                case EQ: default: // actual = ?
                    // If actual value is null, so test value must be null also
                    // to succeed.
                case LE: // actual <= ?
                    // If actual value is null, and since null is high, test
                    // value must be null also to succeed.
                    b.ifNullBranch(notNull, false);
                    b.loadThis();
                    b.loadField(fieldName, fieldType);
                    getScope().successIfNullElseFail(b, true);
                    break;
                case NE: // actual != ?
                    // If actual value is null, so test value must be non-null
                    // to succeed.
                case GT: // actual > ?
                    // If actual value is null, and since null is high, test
                    // value must be non-null to succeed.
                    b.ifNullBranch(notNull, false);
                    b.loadThis();
                    b.loadField(fieldName, fieldType);
                    getScope().successIfNullElseFail(b, false);
                    break;
                case LT: // actual < ?
                    // Since null is high, always fail if actual value is
                    // null. Don't need to examine test value.
                    getScope().failIfNull(b, true);
                    break;
                case GE: // actual >= ?
                    // Since null is high, always succeed if actual value is
                    // null. Don't need to examine test value.
                    getScope().successIfNull(b, true);
                    break;
                }

                notNull.setLocation();

                // At this point, actual property value is known to not be null.

                // Check if property being tested for is null.
                b.loadThis();
                b.loadField(fieldName, fieldType);

                switch (relOp) {
                case EQ: default: // non-null actual = ?
                    // If test value is null, fail.
                case GT: // non-null actual > ?
                    // If test value is null, and since null is high, fail.
                case GE: // non-null actual >= ?
                    // If test value is null, and since null is high, fail.
                    getScope().failIfNull(b, true);
                    break;
                case NE: // non-null actual != ?
                    // If test value is null, succeed
                case LE: // non-null actual <= ?
                    // If test value is null, and since null is high, succeed.
                case LT: // non-null actual < ?
                    // If test value is null, and since null is high, succeed.
                    getScope().successIfNull(b, true);
                    break;
                }

                b.loadLocal(actualValue);
            }

            // When this point is reached, actual property value is on the
            // operand stack, and it is non-null. Property value to test
            // against is not on the stack, but it is known to be non-null.

            TypeDesc primitiveType = type.toPrimitiveType();

            if (primitiveType == null) {
                b.loadThis();
                b.loadField(fieldName, fieldType);

                // Do object comparison
                switch (relOp) {
                case EQ: case NE: default:
                    if (fieldType.isArray()) {
                        TypeDesc arraysDesc = TypeDesc.forClass(Arrays.class);
                        TypeDesc componentType = fieldType.getComponentType();
                        TypeDesc arrayType = fieldType;
                        String methodName;
                        if (componentType.isArray()) {
                            methodName = "deepEquals";
                            arrayType = OBJECT.toArrayType();
                        } else {
                            methodName = "equals";
                            if (!componentType.isPrimitive()) {
                                arrayType = OBJECT.toArrayType();
                            }
                        }
                        b.invokeStatic(arraysDesc, methodName, BOOLEAN,
                                       new TypeDesc[] {arrayType, arrayType});
                    } else {
                        b.invokeVirtual(OBJECT, "equals", BOOLEAN, new TypeDesc[] {OBJECT});
                    }
                    // Success if boolean value is non-zero for EQ, zero for NE.
                    getScope().successIfZeroComparisonElseFail(b, relOp.reverse());
                    break;

                case GT: case GE: case LE: case LT:
                    // Compare method exists because it was checked for earlier
                    b.invokeInterface(TypeDesc.forClass(Comparable.class), "compareTo",
                                      INT, new TypeDesc[] {OBJECT});
                    getScope().successIfZeroComparisonElseFail(b, relOp);
                    break;
                }
            } else {
                if (!type.isPrimitive()) {
                    // Extract primitive value out of wrapper.
                    b.convert(type, primitiveType);
                }

                // Floating point values are compared based on actual
                // bits. This allows NaN to be considered in the comparison.
                if (primitiveType == FLOAT) {
                    convertProperty(b, primitiveType, INT);
                    primitiveType = INT;
                } else if (primitiveType == DOUBLE) {
                    convertProperty(b, primitiveType, LONG);
                    primitiveType = LONG;
                }

                b.loadThis();
                b.loadField(fieldName, fieldType);
                // No need to do anything special for floating point since it
                // has been pre-converted to bits.
                b.convert(fieldType, primitiveType);

                switch (primitiveType.getTypeCode()) {
                case INT_CODE: default:
                    getScope().successIfComparisonElseFail(b, relOp);
                    break;

                case LONG_CODE:
                    b.math(Opcode.LCMP);
                    getScope().successIfZeroComparisonElseFail(b, relOp);
                    break;
                }
            }
        }

        /**
         * Returns the actual field type used to store the given property
         * type. Floating point values are represented in their "bit" form, in
         * order to compare against NaN.
         */
        private static TypeDesc actualFieldType(TypeDesc type) {
            if (type.toPrimitiveType() == FLOAT) {
                if (type.isPrimitive()) {
                    type = INT;
                } else {
                    type = INT.toObjectType();
                }
            } else if (type.toPrimitiveType() == DOUBLE) {
                if (type.isPrimitive()) {
                    type = LONG;
                } else {
                    type = LONG.toObjectType();
                }
            }
            return type;
        }

        /**
         * Converts property value on the stack.
         */
        private void convertProperty(CodeBuilder b, TypeDesc fromType, TypeDesc toType) {
            TypeDesc fromPrimType = fromType.toPrimitiveType();

            if (fromPrimType != FLOAT && fromPrimType != DOUBLE) {
                // Not converting floating point, so just convert as normal.
                b.convert(fromType, toType);
                return;
            }

            TypeDesc toPrimType = toType.toPrimitiveType();

            if (toPrimType != INT && toPrimType != LONG) {
                // Floating point not being converted to bits, so just convert as normal.
                b.convert(fromType, toType);
                return;
            }

            Label done = b.createLabel();
            
            if (!fromType.isPrimitive() && !toType.isPrimitive()) {
                b.dup();
                Label notNull = b.createLabel();
                b.ifNullBranch(notNull, false);
                // Need to replace one null with another null.
                b.pop();
                b.loadNull();
                b.branch(done);
                notNull.setLocation();
            }

            b.convert(fromType, toPrimType, CodeAssembler.CONVERT_FP_BITS);

            Label box = b.createLabel();

            // Floating point bits need to be flipped for negative values.

            if (toPrimType == INT) {
                b.dup();
                b.ifZeroComparisonBranch(box, ">=");
                b.loadConstant(0x7fffffff);
                b.math(Opcode.IXOR);
            } else {
                b.dup2();
                b.loadConstant(0L);
                b.math(Opcode.LCMP);
                b.ifZeroComparisonBranch(box, ">=");
                b.loadConstant(0x7fffffffffffffffL);
                b.math(Opcode.LXOR);
            }

            box.setLocation();
            b.convert(toPrimType, toType);

            done.setLocation();
        }

        /**
         * Defines boolean logic branching scope.
         */
        private static class Scope {
            // If null, return false on test failure.
            final Label mFailLocation;
            // If null, return true on test success.
            final Label mSuccessLocation;

            Scope(Label failLocation, Label successLocation) {
                mFailLocation = failLocation;
                mSuccessLocation = successLocation;
            }

            void fail(CodeBuilder b) {
                if (mFailLocation != null) {
                    b.branch(mFailLocation);
                } else {
                    b.loadConstant(false);
                    b.returnValue(BOOLEAN);
                }
            }

            /**
             * Branch to the fail location if the value on the stack is
             * null. When choice is false, fail on non-null.
             *
             * @param choice if true, fail when null, else fail when not null
             */
            void failIfNull(CodeBuilder b, boolean choice) {
                if (mFailLocation != null) {
                    b.ifNullBranch(mFailLocation, choice);
                } else {
                    Label pass = b.createLabel();
                    b.ifNullBranch(pass, !choice);
                    b.loadConstant(false);
                    b.returnValue(BOOLEAN);
                    pass.setLocation();
                }
            }

            void success(CodeBuilder b) {
                if (mSuccessLocation != null) {
                    b.branch(mSuccessLocation);
                } else {
                    b.loadConstant(true);
                    b.returnValue(BOOLEAN);
                }
            }

            /**
             * Branch to the success location if the value on the stack is
             * null. When choice is false, success on non-null.
             *
             * @param choice if true, success when null, else success when not null
             */
            void successIfNull(CodeBuilder b, boolean choice) {
                if (mSuccessLocation != null) {
                    b.ifNullBranch(mSuccessLocation, choice);
                } else {
                    Label pass = b.createLabel();
                    b.ifNullBranch(pass, !choice);
                    b.loadConstant(true);
                    b.returnValue(BOOLEAN);
                    pass.setLocation();
                }
            }

            /**
             * Branch to the success location if the value on the stack is
             * null. When choice is false, success on non-null. If the success
             * condition is not met, the fail location is branched to.
             *
             * @param choice if true, success when null, else success when not null
             */
            void successIfNullElseFail(CodeBuilder b, boolean choice) {
                if (mSuccessLocation != null) {
                    b.ifNullBranch(mSuccessLocation, choice);
                    fail(b);
                } else if (mFailLocation != null) {
                    b.ifNullBranch(mFailLocation, !choice);
                    success(b);
                } else {
                    Label success = b.createLabel();
                    b.ifNullBranch(success, choice);
                    b.loadConstant(false);
                    b.returnValue(BOOLEAN);
                    success.setLocation();
                    b.loadConstant(true);
                    b.returnValue(BOOLEAN);
                }
            }

            void successIfZeroComparisonElseFail(CodeBuilder b, RelOp relOp) {
                if (mSuccessLocation != null) {
                    b.ifZeroComparisonBranch(mSuccessLocation, relOpToChoice(relOp));
                    fail(b);
                } else if (mFailLocation != null) {
                    b.ifZeroComparisonBranch(mFailLocation, relOpToChoice(relOp.reverse()));
                    success(b);
                } else {
                    if (relOp == RelOp.NE) {
                        b.returnValue(BOOLEAN);
                    } else if (relOp == RelOp.EQ) {
                        b.loadConstant(1);
                        b.math(Opcode.IAND);
                        b.loadConstant(1);
                        b.math(Opcode.IXOR);
                        b.returnValue(BOOLEAN);
                    } else {
                        Label success = b.createLabel();
                        b.ifZeroComparisonBranch(success, relOpToChoice(relOp));
                        b.loadConstant(false);
                        b.returnValue(BOOLEAN);
                        success.setLocation();
                        b.loadConstant(true);
                        b.returnValue(BOOLEAN);
                    }
                }
            }

            void successIfComparisonElseFail(CodeBuilder b, RelOp relOp) {
                if (mSuccessLocation != null) {
                    b.ifComparisonBranch(mSuccessLocation, relOpToChoice(relOp));
                    fail(b);
                } else if (mFailLocation != null) {
                    b.ifComparisonBranch(mFailLocation, relOpToChoice(relOp.reverse()));
                    success(b);
                } else {
                    Label success = b.createLabel();
                    b.ifComparisonBranch(success, relOpToChoice(relOp));
                    b.loadConstant(false);
                    b.returnValue(BOOLEAN);
                    success.setLocation();
                    b.loadConstant(true);
                    b.returnValue(BOOLEAN);
                }
            }

            private String relOpToChoice(RelOp relOp) {
                switch (relOp) {
                case EQ: default:
                    return "==";
                case NE:
                    return "!=";
                case LT:
                    return "<";
                case GE:
                    return ">=";
                case GT:
                    return ">";
                case LE:
                    return "<=";
                }
            }
        }
    }
}
