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

package com.amazon.carbonado.raw;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.EnumSet;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.spi.MasterFeature;
import com.amazon.carbonado.spi.MasterStorableGenerator;
import com.amazon.carbonado.spi.MasterSupport;
import com.amazon.carbonado.spi.StorableGenerator;
import com.amazon.carbonado.spi.TriggerSupport;

import static com.amazon.carbonado.spi.CommonMethodNames.*;

/**
 * Generates and caches abstract implementations of {@link Storable} types
 * which are encoded and decoded in a raw format. The generated abstract
 * classes extend those created by {@link MasterStorableGenerator}.
 *
 * @author Brian S O'Neill
 * @see GenericStorableCodec
 * @see RawSupport
 */
public class RawStorableGenerator {
    // Note: All generated fields/methods have a "$" character in them to
    // prevent name collisions with any inherited fields/methods. User storable
    // properties are defined as fields which exactly match the property
    // name. We don't want collisions with those either. Legal bean properties
    // cannot have "$" in them, so there's nothing to worry about.

    /** Name of protected abstract method in generated storable */
    public static final String
        ENCODE_KEY_METHOD_NAME = "encodeKey$",
        DECODE_KEY_METHOD_NAME = "decodeKey$",
        ENCODE_DATA_METHOD_NAME = "encodeData$",
        DECODE_DATA_METHOD_NAME = "decodeData$";

    @SuppressWarnings("unchecked")
    private static Map<Class, Flavors<? extends Storable>> cCache = new WeakIdentityMap();

    /**
     * Collection of different abstract class flavors.
     */
    static class Flavors<S extends Storable> {
        private Reference<Class<? extends S>> mMasterFlavor;

        private Reference<Class<? extends S>> mNonMasterFlavor;

        /**
         * May return null.
         */
        Class<? extends S> getClass(boolean isMaster) {
            Reference<Class<? extends S>> ref;
            if (isMaster) {
                ref = mMasterFlavor;
            } else {
                ref = mNonMasterFlavor;
            }
            return (ref != null) ? ref.get() : null;
        }

        @SuppressWarnings("unchecked")
        void setClass(Class<? extends S> clazz, boolean isMaster) {
            Reference<Class<? extends S>> ref = new SoftReference(clazz);
            if (isMaster) {
                mMasterFlavor = ref;
            } else {
                mNonMasterFlavor = ref;
            }
        }
    }

    // Can't be instantiated or extended
    private RawStorableGenerator() {
    }

    /**
     * Returns an abstract implementation of the given Storable type, which is
     * fully thread-safe. The Storable type itself may be an interface or a
     * class. If it is a class, then it must not be final, and it must have a
     * public, no-arg constructor. Two constructors are defined for the
     * abstract implementation:
     *
     * <pre>
     * public &lt;init&gt;(RawSupport);

     * public &lt;init&gt;(RawSupport, byte[] key, byte[] value);
     * </pre>
     *
     * <p>Subclasses must implement the following abstract protected methods,
     * whose exact names are defined by constants in this class:
     *
     * <pre>
     * // Encode the primary key of this storable.
     * protected abstract byte[] encodeKey();
     *
     * // Encode all properties of this storable excluding the primary key.
     * protected abstract byte[] encodeData();
     *
     * // Decode the primary key into properties of this storable.
     * // Note: this method is also invoked by the four argument constructor.
     * protected abstract void decodeKey(byte[]);
     *
     * // Decode the data into properties of this storable.
     * // Note: this method is also invoked by the four argument constructor.
     * protected abstract void decodeData(byte[]);
     * </pre>
     *
     * @param isMaster when true, version properties, sequences, and triggers are managed
     * @throws IllegalArgumentException if type is null
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> Class<? extends S>
        getAbstractClass(Class<S> type, boolean isMaster)
        throws SupportException, IllegalArgumentException
    {
        synchronized (cCache) {
            Class<? extends S> abstractClass;

            Flavors<S> flavors = (Flavors<S>) cCache.get(type);

            if (flavors == null) {
                flavors = new Flavors<S>();
                cCache.put(type, flavors);
            } else if ((abstractClass = flavors.getClass(isMaster)) != null) {
                return abstractClass;
            }

            abstractClass = generateAbstractClass(type, isMaster);
            flavors.setClass(abstractClass, isMaster);

            return abstractClass;
        }
    }

    @SuppressWarnings("unchecked")
    private static <S extends Storable> Class<? extends S>
        generateAbstractClass(Class<S> storableClass, boolean isMaster)
        throws SupportException
    {
        EnumSet<MasterFeature> features;
        if (isMaster) {
            features = EnumSet.of(MasterFeature.VERSIONING,
                                  MasterFeature.UPDATE_FULL,
                                  MasterFeature.INSERT_SEQUENCES,
                                  MasterFeature.INSERT_CHECK_REQUIRED);
        } else {
            features = EnumSet.of(MasterFeature.UPDATE_FULL);
        }

        final Class<? extends S> abstractClass =
            MasterStorableGenerator.getAbstractClass(storableClass, features);

        ClassInjector ci = ClassInjector.create
            (storableClass.getName(), abstractClass.getClassLoader());

        ClassFile cf = new ClassFile(ci.getClassName(), abstractClass);
        cf.setModifiers(cf.getModifiers().toAbstract(true));
        cf.markSynthetic();
        cf.setSourceFile(RawStorableGenerator.class.getName());
        cf.setTarget("1.5");

        // Declare some types.
        final TypeDesc storableType = TypeDesc.forClass(Storable.class);
        final TypeDesc triggerSupportType = TypeDesc.forClass(TriggerSupport.class);
        final TypeDesc masterSupportType = TypeDesc.forClass(MasterSupport.class);
        final TypeDesc rawSupportType = TypeDesc.forClass(RawSupport.class);
        final TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);

        // Add constructor that accepts a RawSupport.
        {
            TypeDesc[] params = {rawSupportType};
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeSuperConstructor(new TypeDesc[] {masterSupportType});
            b.returnVoid();
        }

        // Add constructor that accepts a RawSupport, an encoded key, and an
        // encoded data.
        {
            TypeDesc[] params = {rawSupportType, byteArrayType, byteArrayType};
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeSuperConstructor(new TypeDesc[] {masterSupportType});

            params = new TypeDesc[] {byteArrayType};

            b.loadThis();
            b.loadLocal(b.getParameter(1));
            b.invokeVirtual(DECODE_KEY_METHOD_NAME, null, params);

            b.loadThis();
            b.loadLocal(b.getParameter(2));
            b.invokeVirtual(DECODE_DATA_METHOD_NAME, null, params);

            // Indicate that object is clean by calling markAllPropertiesClean.
            b.loadThis();
            b.invokeVirtual(MARK_ALL_PROPERTIES_CLEAN, null, null);

            b.returnVoid();
        }

        // Declare protected abstract methods.
        {
            cf.addMethod(Modifiers.PROTECTED.toAbstract(true),
                         ENCODE_KEY_METHOD_NAME, byteArrayType, null);
            cf.addMethod(Modifiers.PROTECTED.toAbstract(true),
                         DECODE_KEY_METHOD_NAME, null, new TypeDesc[]{byteArrayType});
            cf.addMethod(Modifiers.PROTECTED.toAbstract(true),
                         ENCODE_DATA_METHOD_NAME, byteArrayType, null);
            cf.addMethod(Modifiers.PROTECTED.toAbstract(true),
                         DECODE_DATA_METHOD_NAME, null, new TypeDesc[]{byteArrayType});
        }

        // Add required protected doTryLoad_master method, which delegates to RawSupport.
        {
            MethodInfo mi = cf.addMethod
                (Modifiers.PROTECTED.toFinal(true),
                 MasterStorableGenerator.DO_TRY_LOAD_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, triggerSupportType);
            b.checkCast(rawSupportType);
            b.loadThis();
            b.invokeVirtual(ENCODE_KEY_METHOD_NAME, byteArrayType, null);
            TypeDesc[] params = {byteArrayType};
            b.invokeInterface(rawSupportType, "tryLoad", byteArrayType, params);
            LocalVariable encodedDataVar = b.createLocalVariable(null, byteArrayType);
            b.storeLocal(encodedDataVar);
            b.loadLocal(encodedDataVar);
            Label notNull = b.createLabel();
            b.ifNullBranch(notNull, false);
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);
            notNull.setLocation();
            b.loadThis();
            b.loadLocal(encodedDataVar);
            params = new TypeDesc[] {byteArrayType};
            b.invokeVirtual(DECODE_DATA_METHOD_NAME, null, params);
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add required protected doTryInsert_master method, which delegates to RawSupport.
        {
            MethodInfo mi = cf.addMethod
                (Modifiers.PROTECTED.toFinal(true),
                 MasterStorableGenerator.DO_TRY_INSERT_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
            CodeBuilder b = new CodeBuilder(mi);

            // return rawSupport.tryInsert(this, this.encodeKey$(), this.encodeData$());
            b.loadThis();
            b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, triggerSupportType);
            b.checkCast(rawSupportType);
            b.loadThis(); // pass this to tryInsert method
            b.loadThis();
            b.invokeVirtual(ENCODE_KEY_METHOD_NAME, byteArrayType, null);
            b.loadThis();
            b.invokeVirtual(ENCODE_DATA_METHOD_NAME, byteArrayType, null);
            TypeDesc[] params = {storableType, byteArrayType, byteArrayType};
            b.invokeInterface(rawSupportType, "tryInsert", TypeDesc.BOOLEAN, params);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add required protected doTryUpdate_master method, which delegates to RawSupport.
        {
            MethodInfo mi = cf.addMethod
                (Modifiers.PROTECTED.toFinal(true),
                 MasterStorableGenerator.DO_TRY_UPDATE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
            CodeBuilder b = new CodeBuilder(mi);

            // rawSupport.store(this, this.encodeKey$(), this.encodeData$());
            // return true;
            b.loadThis();
            b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, triggerSupportType);
            b.checkCast(rawSupportType);
            b.loadThis(); // pass this to store method
            b.loadThis();
            b.invokeVirtual(ENCODE_KEY_METHOD_NAME, byteArrayType, null);
            b.loadThis();
            b.invokeVirtual(ENCODE_DATA_METHOD_NAME, byteArrayType, null);
            TypeDesc[] params = {storableType, byteArrayType, byteArrayType};
            b.invokeInterface(rawSupportType, "store", null, params);
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add required protected doTryDelete_master method, which delegates to RawSupport.
        {
            MethodInfo mi = cf.addMethod
                (Modifiers.PROTECTED.toFinal(true),
                 MasterStorableGenerator.DO_TRY_DELETE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
            CodeBuilder b = new CodeBuilder(mi);

            // return rawSupport.tryDelete(this.encodeKey$());
            b.loadThis();
            b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, triggerSupportType);
            b.checkCast(rawSupportType);
            b.loadThis();
            b.invokeVirtual(ENCODE_KEY_METHOD_NAME, byteArrayType, null);

            TypeDesc[] params = {byteArrayType};
            b.invokeInterface(rawSupportType, "tryDelete", TypeDesc.BOOLEAN, params);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        return ci.defineClass(cf);
    }
}
