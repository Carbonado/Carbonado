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

package com.amazon.carbonado.repo.map;

import java.lang.reflect.UndeclaredThrowableException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;

import org.cojen.util.ClassInjector;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.gen.CodeBuilderUtil;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class Key<S extends Storable> implements Comparable<Key<S>> {
    protected final S mStorable;
    protected final Comparator<S> mComparator;

    Key(S storable, Comparator<S> comparator) {
        mStorable = storable;
        mComparator = comparator;
    }

    @Override
    public String toString() {
        return mStorable.toString();
    }

    public int compareTo(Key<S> other) {
        int result = mComparator.compare(mStorable, other.mStorable);
        if (result == 0) {
            result = tieBreaker() - other.tieBreaker();
        }
        return result;
    }

    protected int tieBreaker() {
        return 0;
    }

    public static interface Assigner<S extends Storable> {
        void setKeyValues(S storable, Object[] identityValues);

        void setKeyValues(S storable, Object[] identityValues, Object rangeValue);
    }

    private static final Map<Class, Assigner> mAssigners;

    static {
        mAssigners = new SoftValuedHashMap();
    }

    public static synchronized <S extends Storable> Assigner<S> getAssigner(Class<S> clazz) {
        Assigner<S> assigner = mAssigners.get(clazz);
        if (assigner == null) {
            assigner = createAssigner(clazz);
            mAssigners.put(clazz, assigner);
        }
        return assigner;
    }

    private static <S extends Storable> Assigner<S> createAssigner(Class<S> clazz) {
        final StorableInfo<S> info = StorableIntrospector.examine(clazz);

        ClassInjector ci = ClassInjector.create(clazz.getName(), clazz.getClassLoader());
        ClassFile cf = new ClassFile(ci.getClassName());
        cf.addInterface(Assigner.class);
        cf.markSynthetic();
        cf.setSourceFile(Key.class.getName());
        cf.setTarget("1.5");

        cf.addDefaultConstructor();

        // Define required setKeyValues methods.

        List<OrderedProperty<S>> pk =
            new ArrayList<OrderedProperty<S>>(info.getPrimaryKey().getProperties());

        TypeDesc storableType = TypeDesc.forClass(Storable.class);
        TypeDesc storableArrayType = storableType.toArrayType();
        TypeDesc userStorableType = TypeDesc.forClass(info.getStorableType());
        TypeDesc objectArrayType = TypeDesc.OBJECT.toArrayType();

        MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, "setKeyValues", null,
                                     new TypeDesc[] {storableType, objectArrayType});

        CodeBuilder b = new CodeBuilder(mi);

        LocalVariable userStorableVar = b.createLocalVariable(null, userStorableType);
        b.loadLocal(b.getParameter(0));
        b.checkCast(userStorableType);
        b.storeLocal(userStorableVar);

        // Switch on the number of values supplied.

        /* Switch looks like this for two pk properties:

        switch(identityValues.length) {
        default:
            throw new IllegalArgumentException();
        case 2:
            storable.setPkProp2(identityValues[1]);
        case 1:
            storable.setPkProp1(identityValues[0]);
        case 0:
        }

        */

        b.loadLocal(b.getParameter(1));
        b.arrayLength();

        int[] cases = new int[pk.size() + 1];
        Label[] labels = new Label[pk.size() + 1];

        for (int i=0; i<labels.length; i++) {
            cases[i] = pk.size() - i;
            labels[i] = b.createLabel();
        }

        Label defaultLabel = b.createLabel();

        b.switchBranch(cases, labels, defaultLabel);

        for (int i=0; i<labels.length; i++) {
            labels[i].setLocation();
            int prop = cases[i] - 1;
            if (prop >= 0) {
                b.loadLocal(userStorableVar);
                b.loadLocal(b.getParameter(1));
                b.loadConstant(prop);
                b.loadFromArray(storableArrayType);
                callSetPropertyValue(b, pk.get(prop));
            }
            // Fall through to next case.
        }

        b.returnVoid();

        defaultLabel.setLocation();
        CodeBuilderUtil.throwException(b, IllegalArgumentException.class, null);

        // The setKeyValues method that takes a range value calls the other
        // setKeyValues method first, to take care of the identityValues.

        mi = cf.addMethod(Modifiers.PUBLIC, "setKeyValues", null,
                          new TypeDesc[] {storableType, objectArrayType, TypeDesc.OBJECT});

        b = new CodeBuilder(mi);

        b.loadThis();
        b.loadLocal(b.getParameter(0));
        b.loadLocal(b.getParameter(1));
        b.invokeVirtual("setKeyValues", null, new TypeDesc[] {storableType, objectArrayType});

        userStorableVar = b.createLocalVariable(null, userStorableType);
        b.loadLocal(b.getParameter(0));
        b.checkCast(userStorableType);
        b.storeLocal(userStorableVar);

        // Switch on the number of values supplied.

        /* Switch looks like this for two pk properties:

        switch(identityValues.length) {
        default:
            throw new IllegalArgumentException();
        case 0:
            storable.setPkProp1(rangeValue);
            return;
        case 1:
            storable.setPkProp2(rangeValue);
            return;
        }

        */

        b.loadLocal(b.getParameter(1));
        b.arrayLength();

        cases = new int[pk.size()];
        labels = new Label[pk.size()];

        for (int i=0; i<labels.length; i++) {
            cases[i] = i;
            labels[i] = b.createLabel();
        }

        defaultLabel = b.createLabel();

        b.switchBranch(cases, labels, defaultLabel);

        for (int i=0; i<labels.length; i++) {
            labels[i].setLocation();
            int prop = cases[i];
            b.loadLocal(userStorableVar);
            b.loadLocal(b.getParameter(2));
            callSetPropertyValue(b, pk.get(prop));
            b.returnVoid();
        }

        defaultLabel.setLocation();
        CodeBuilderUtil.throwException(b, IllegalArgumentException.class, null);

        try {
            return (Assigner<S>) ci.defineClass(cf).newInstance();
        } catch (IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        } catch (InstantiationException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /**
     * Creates code to call set method. Assumes Storable and property value
     * are already on the stack.
     */
    private static void callSetPropertyValue(CodeBuilder b, OrderedProperty<?> op) {
        StorableProperty<?> property = op.getChainedProperty().getLastProperty();
        TypeDesc propType = TypeDesc.forClass(property.getType());
        if (propType != TypeDesc.OBJECT) {
            TypeDesc objectType = propType.toObjectType();
            b.checkCast(objectType);
            // Potentially unbox primitive.
            b.convert(objectType, propType);
        }
        b.invoke(property.getWriteMethod());
    }
}
