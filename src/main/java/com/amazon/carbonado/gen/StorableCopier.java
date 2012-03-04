/*
 * Copyright 2010-2012 Amazon Technologies, Inc. or its affiliates.
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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Constructor;

import java.util.WeakHashMap;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;

import org.cojen.util.ClassInjector;

/**
 * Copies properties between otherwise incompatible Storables. Only matched
 * properties are copied, and primitive types are converted.
 *
 * @author Brian S O'Neill
 * @since 1.2.2
 */
public abstract class StorableCopier<S extends Storable, T extends Storable> {
    private static final WeakHashMap<Class, Object> cClassKeyCache;
    private static final WeakHashMap<Object, From> cFromCache;

    static {
        cClassKeyCache = new WeakHashMap<Class, Object>();
        cFromCache = new WeakHashMap<Object, From>();
    }

    static synchronized Object classKey(Class clazz) {
        Object key = cClassKeyCache.get(clazz);
        if (key == null) {
            key = new Object();
            cClassKeyCache.put(clazz, key);
        }
        return key;
    }

    public static synchronized <S extends Storable> From<S> from(Class<S> source) {
        Object key = classKey(source);
        From<S> from = (From<S>) cFromCache.get(key);
        if (from == null) {
            from = new From<S>(source);
            cFromCache.put(key, from);
        }
        return from;
    }

    public static class From<S extends Storable> {
        private final Class<S> mSource;
        private final WeakHashMap<Object, StorableCopier> mCopierCache;

        From(Class<S> source) {
            mSource = source;
            mCopierCache = new WeakHashMap<Object, StorableCopier>();
        }

        public synchronized <T extends Storable> StorableCopier<S, T> to(Class<T> target) {
            Object key = classKey(target);
            StorableCopier<S, T> copier = (StorableCopier<S, T>) mCopierCache.get(key);
            if (copier == null) {
                if (mSource == target) {
                    copier = (StorableCopier<S, T>) Direct.THE;
                } else {
                    copier = new Wrapped<S, T>(new Wrapper<S, T>(mSource, target).generate());
                }
                mCopierCache.put(key, copier);
            }
            return copier;
        }
    }

    protected StorableCopier() {
    }

    public abstract void copyAllProperties(S source, T target);

    public abstract void copyPrimaryKeyProperties(S source, T target);

    public abstract void copyVersionProperty(S source, T target);

    public abstract void copyUnequalProperties(S source, T target);

    public abstract void copyDirtyProperties(S source, T target);

    private static class Wrapped<S extends Storable, T extends Storable>
        extends StorableCopier<S, T>
    {
        private final Constructor<? extends S> mWrapperCtor;

        private Wrapped(Constructor<? extends S> ctor) {
            mWrapperCtor = ctor;
        }

        public void copyAllProperties(S source, T target) {
            source.copyAllProperties(wrap(target));
        }

        public void copyPrimaryKeyProperties(S source, T target) {
            source.copyPrimaryKeyProperties(wrap(target));
        }

        public void copyVersionProperty(S source, T target) {
            source.copyVersionProperty(wrap(target));
        }

        public void copyUnequalProperties(S source, T target) {
            source.copyUnequalProperties(wrap(target));
        }

        public void copyDirtyProperties(S source, T target) {
            source.copyDirtyProperties(wrap(target));
        }

        private S wrap(T target) {
            try {
                return mWrapperCtor.newInstance(target);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }

    private static class Direct<S extends Storable> extends StorableCopier<S, S> {
        static final Direct THE = new Direct();

        private Direct() {
        }

        public void copyAllProperties(S source, S target) {
            source.copyAllProperties(target);
        }

        public void copyPrimaryKeyProperties(S source, S target) {
            source.copyPrimaryKeyProperties(target);
        }

        public void copyVersionProperty(S source, S target) {
            source.copyVersionProperty(target);
        }

        public void copyUnequalProperties(S source, S target) {
            source.copyUnequalProperties(target);
        }

        public void copyDirtyProperties(S source, S target) {
            source.copyDirtyProperties(target);
        }
    }

    private static class Wrapper<W extends Storable, D extends Storable> {
        private final StorableInfo<W> mWrapperInfo;
        private final StorableInfo<D> mDelegateInfo;
        private final ClassInjector mClassInjector;
        private final ClassFile mClassFile;

        Wrapper(Class<W> wrapper, Class<D> delegate) {
            mWrapperInfo = StorableIntrospector.examine(wrapper);
            mDelegateInfo = StorableIntrospector.examine(delegate);

            ClassLoader loader = wrapper.getClassLoader();
            try {
                loader.loadClass(delegate.getName());
            } catch (ClassNotFoundException e) {
                loader = delegate.getClassLoader();
                try {
                    loader.loadClass(wrapper.getName());
                } catch (ClassNotFoundException e2) {
                    // This could be fixed by creating an intermediate class loader, but
                    // other issues might crop up.
                    throw new IllegalStateException
                        ("Unable for find common class loader for source and target types: " +
                         wrapper.getClass() + ", " + delegate.getClass());
                }
            }

            mClassInjector = ClassInjector.create(wrapper.getName(), loader);

            mClassFile = CodeBuilderUtil.createStorableClassFile
                (mClassInjector, mWrapperInfo.getStorableType(),
                 false, StorableCopier.class.getName());
        }

        Constructor<? extends W> generate() {
            TypeDesc delegateType = TypeDesc.forClass(mDelegateInfo.getStorableType());

            mClassFile.addField(Modifiers.PRIVATE.toFinal(true), "delegate", delegateType);

            MethodInfo mi = mClassFile.addConstructor
                (Modifiers.PUBLIC, new TypeDesc[] {delegateType});
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.invokeSuperConstructor(null);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.storeField("delegate", delegateType);
            b.returnVoid();

            // Implement property access methods.
            for (StorableProperty<W> wrapperProp : mWrapperInfo.getAllProperties().values()) {
                if (wrapperProp.isDerived()) {
                    continue;
                }

                TypeDesc wrapperPropType = TypeDesc.forClass(wrapperProp.getType());

                StorableProperty<D> delegateProp =
                    mDelegateInfo.getAllProperties().get(wrapperProp.getName());

                if (delegateProp == null || delegateProp.isDerived()) {
                    addUnmatchedProperty(wrapperProp, wrapperPropType);
                    continue;
                }

                TypeDesc delegatePropType = TypeDesc.forClass(delegateProp.getType());

                if (wrapperPropType.equals(delegatePropType)) {
                    // No conversion required.
                    Method m = canDefine(wrapperProp.getReadMethod());
                    if (m != null) {
                        b = new CodeBuilder(mClassFile.addMethod(m));
                        if (delegateProp.getReadMethod() == null) {
                            CodeBuilderUtil.blankValue(b, wrapperPropType);
                        } else {
                            b.loadThis();
                            b.loadField("delegate", delegateType);
                            b.invoke(delegateProp.getReadMethod());
                        }
                        b.returnValue(wrapperPropType);
                    }

                    m = canDefine(wrapperProp.getWriteMethod());
                    if (m != null) {
                        b = new CodeBuilder(mClassFile.addMethod(m));
                        if (delegateProp.getWriteMethod() != null) {
                            b.loadThis();
                            b.loadField("delegate", delegateType);
                            b.loadLocal(b.getParameter(0));
                            b.invoke(delegateProp.getWriteMethod());
                        }
                        b.returnVoid();
                    }
                    
                    continue;
                }

                TypeDesc wrapperPrimPropType = wrapperPropType.toPrimitiveType();
                TypeDesc delegatePrimPropType = delegatePropType.toPrimitiveType();

                if (wrapperPrimPropType == null || delegatePrimPropType == null) {
                    addUnmatchedProperty(wrapperProp, wrapperPropType);
                    continue;
                }

                // Convert primitive or boxed type.

                Method m = canDefine(wrapperProp.getReadMethod());
                if (m != null) {
                    b = new CodeBuilder(mClassFile.addMethod(m));
                    if (delegateProp.getReadMethod() == null) {
                        CodeBuilderUtil.blankValue(b, wrapperPropType);
                    } else {
                        b.loadThis();
                        b.loadField("delegate", delegateType);
                        b.invoke(delegateProp.getReadMethod());
                        if (wrapperPropType.isPrimitive() && !delegatePropType.isPrimitive()) {
                            // Check for null.
                            b.dup();
                            Label notNull = b.createLabel();
                            b.ifNullBranch(notNull, false);
                            CodeBuilderUtil.blankValue(b, wrapperPropType);
                            b.returnValue(wrapperPropType);
                            notNull.setLocation();
                        }
                        b.convert(delegatePropType, wrapperPropType);
                    }
                    b.returnValue(wrapperPropType);
                }

                m = canDefine(wrapperProp.getWriteMethod());
                if (m != null) {
                    b = new CodeBuilder(mClassFile.addMethod(m));
                    if (delegateProp.getWriteMethod() != null) {
                        b.loadThis();
                        b.loadField("delegate", delegateType);
                        b.loadLocal(b.getParameter(0));
                        if (!wrapperPropType.isPrimitive() && delegatePropType.isPrimitive()) {
                            // Check for null.
                            Label notNull = b.createLabel();
                            b.ifNullBranch(notNull, false);
                            CodeBuilderUtil.blankValue(b, delegatePropType);
                            b.invoke(delegateProp.getWriteMethod());
                            b.returnVoid();
                            notNull.setLocation();
                            b.loadLocal(b.getParameter(0));
                        }
                        b.convert(wrapperPropType, delegatePropType);
                        b.invoke(delegateProp.getWriteMethod());
                    }
                    b.returnVoid();
                }
            }

            try {
                Class<? extends W> wrapperClass = mClassInjector.defineClass(mClassFile);
                return wrapperClass.getConstructor(mDelegateInfo.getStorableType());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private void addUnmatchedProperty(StorableProperty<W> wrapperProp,
                                          TypeDesc wrapperPropType)
        {
            Method m = canDefine(wrapperProp.getReadMethod());
            if (m != null) {
                CodeBuilder b = new CodeBuilder(mClassFile.addMethod(m));
                CodeBuilderUtil.blankValue(b, wrapperPropType);
                b.returnValue(wrapperPropType);
            }

            m = canDefine(wrapperProp.getWriteMethod());
            if (m != null) {
                CodeBuilder b = new CodeBuilder(mClassFile.addMethod(m));
                b.returnVoid();
            }
        }

        private static Method canDefine(Method m) {
            return (m == null || Modifier.isFinal(m.getModifiers())) ? null : m;
        }
    }
}
