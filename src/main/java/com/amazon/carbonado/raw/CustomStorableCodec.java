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

import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.util.QuickConstructorGenerator;

/**
 * Allows codecs to be defined for storables that have a custom encoding.
 *
 * @author Brian S O'Neill
 * @see CustomStorableCodecFactory
 */
public abstract class CustomStorableCodec<S extends Storable> implements StorableCodec<S> {
    // Generated storable instances maintain a reference to user-defined
    // concrete subclass of this class.
    private static final String CUSTOM_STORABLE_CODEC_FIELD_NAME = "customStorableCodec$";

    @SuppressWarnings("unchecked")
    private static Map<Class, RawStorableGenerator.Flavors<? extends Storable>> cCache =
        new WeakIdentityMap();

    /**
     * Returns a storable implementation that calls into CustomStorableCodec
     * implementation for encoding and decoding.
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> Class<? extends S>
        getStorableClass(Class<S> type, boolean isMaster)
        throws SupportException
    {
        synchronized (cCache) {
            Class<? extends S> storableClass;

            RawStorableGenerator.Flavors<S> flavors =
                (RawStorableGenerator.Flavors<S>) cCache.get(type);

            if (flavors == null) {
                flavors = new RawStorableGenerator.Flavors<S>();
                cCache.put(type, flavors);
            } else if ((storableClass = flavors.getClass(isMaster)) != null) {
                return storableClass;
            }

            storableClass = generateStorableClass(type, isMaster);
            flavors.setClass(storableClass, isMaster);

            return storableClass;
        }
    }

    @SuppressWarnings("unchecked")
    private static <S extends Storable> Class<? extends S>
        generateStorableClass(Class<S> type, boolean isMaster)
        throws SupportException
    {
        final Class<? extends S> abstractClass =
            RawStorableGenerator.getAbstractClass(type, isMaster);

        ClassInjector ci = ClassInjector.create
            (type.getName(), abstractClass.getClassLoader());

        ClassFile cf = new ClassFile(ci.getClassName(), abstractClass);
        cf.markSynthetic();
        cf.setSourceFile(CustomStorableCodec.class.getName());
        cf.setTarget("1.5");

        // Declare some types.
        final TypeDesc rawSupportType = TypeDesc.forClass(RawSupport.class);
        final TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
        final TypeDesc[] byteArrayParam = {byteArrayType};
        final TypeDesc customStorableCodecType = TypeDesc.forClass(CustomStorableCodec.class);

        // Add field for saving reference to concrete CustomStorableCodec.
        cf.addField(Modifiers.PRIVATE.toFinal(true),
                    CUSTOM_STORABLE_CODEC_FIELD_NAME, customStorableCodecType);

        // Add constructor that accepts a RawSupport and a CustomStorableCodec.
        {
            TypeDesc[] params = {rawSupportType, customStorableCodecType};
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);

            // Call super class constructor.
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            params = new TypeDesc[] {rawSupportType};
            b.invokeSuperConstructor(params);

            // Set private reference to customStorableCodec.
            b.loadThis();
            b.loadLocal(b.getParameter(1));
            b.storeField(CUSTOM_STORABLE_CODEC_FIELD_NAME, customStorableCodecType);

            b.returnVoid();
        }

        // Add constructor that accepts a RawSupport, an encoded key, an
        // encoded data, and a CustomStorableCodec.
        {
            TypeDesc[] params = {rawSupportType, byteArrayType, byteArrayType,
                                 customStorableCodecType};
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);

            // Set private reference to customStorableCodec before calling
            // super constructor. This is necessary because super class
            // constructor will call our decode methods, which need the
            // customStorableCodec. This trick is not allowed in Java, but the
            // virtual machine verifier allows it.
            b.loadThis();
            b.loadLocal(b.getParameter(3));
            b.storeField(CUSTOM_STORABLE_CODEC_FIELD_NAME, customStorableCodecType);

            // Now call super class constructor.
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.loadLocal(b.getParameter(1));
            b.loadLocal(b.getParameter(2));
            params = new TypeDesc[] {rawSupportType, byteArrayType, byteArrayType};
            b.invokeSuperConstructor(params);

            b.returnVoid();
        }

        // Implement protected abstract methods inherited from parent class.

        // byte[] encodeKey()
        {
            // Encode the primary key into a byte array that supports correct
            // ordering. No special key comparator is needed.
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED,
                                         RawStorableGenerator.ENCODE_KEY_METHOD_NAME,
                                         byteArrayType, null);
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadField(CUSTOM_STORABLE_CODEC_FIELD_NAME, customStorableCodecType);
            TypeDesc[] params = {TypeDesc.forClass(Storable.class)};
            b.loadThis();
            b.invokeVirtual(customStorableCodecType, "encodePrimaryKey", byteArrayType, params);
            b.returnValue(byteArrayType);
        }

        // byte[] encodeData()
        {
            // Encoding non-primary key data properties.
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED,
                                         RawStorableGenerator.ENCODE_DATA_METHOD_NAME,
                                         byteArrayType, null);
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadField(CUSTOM_STORABLE_CODEC_FIELD_NAME, customStorableCodecType);
            TypeDesc[] params = {TypeDesc.forClass(Storable.class)};
            b.loadThis();
            b.invokeVirtual(customStorableCodecType, "encodeData", byteArrayType, params);
            b.returnValue(byteArrayType);
        }

        // void decodeKey(byte[])
        {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED,
                                         RawStorableGenerator.DECODE_KEY_METHOD_NAME,
                                         null, byteArrayParam);
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadField(CUSTOM_STORABLE_CODEC_FIELD_NAME, customStorableCodecType);
            TypeDesc[] params = {TypeDesc.forClass(Storable.class), byteArrayType};
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeVirtual(customStorableCodecType, "decodePrimaryKey", null, params);
            b.returnVoid();
        }

        // void decodeData(byte[])
        {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED,
                                         RawStorableGenerator.DECODE_DATA_METHOD_NAME,
                                         null, byteArrayParam);
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadField(CUSTOM_STORABLE_CODEC_FIELD_NAME, customStorableCodecType);
            TypeDesc[] params = {TypeDesc.forClass(Storable.class), byteArrayType};
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeVirtual(customStorableCodecType, "decodeData", null, params);
            b.returnVoid();
        }

        return ci.defineClass(cf);
    }

    private final Class<S> mType;
    private final int mPkPropertyCount;
    private final InstanceFactory mInstanceFactory;

    // Modified by CustomStorableCodecFactory after construction. This provides
    // backwards compatibility with implementations of CustomStorableCodecFactory.
    RawSupport<S> mSupport;

    public interface InstanceFactory {
        Storable instantiate(RawSupport support, CustomStorableCodec codec);

        Storable instantiate(RawSupport support, byte[] key, byte[] value,
                             CustomStorableCodec codec)
            throws FetchException;
    }

    /**
     * @param isMaster when true, version properties and sequences are managed
     * @throws SupportException if Storable is not supported
     */
    public CustomStorableCodec(Class<S> type, boolean isMaster) throws SupportException {
        this(type, isMaster, null);
    }

    /**
     * @param isMaster when true, version properties and sequences are managed
     * @throws SupportException if Storable is not supported
     * @since 1.2
     */
    public CustomStorableCodec(Class<S> type, boolean isMaster, RawSupport<S> support)
        throws SupportException
    {
        mType = type;
        mPkPropertyCount = getPrimaryKeyIndex().getPropertyCount();
        Class<? extends S> storableClass = getStorableClass(type, isMaster);
        mInstanceFactory = QuickConstructorGenerator
            .getInstance(storableClass, InstanceFactory.class);
        mSupport = support;
    }

    public Class<S> getStorableType() {
        return mType;
    }

    /**
     * @since 1.2
     */
    @SuppressWarnings("unchecked")
    public S instantiate() {
        return (S) mInstanceFactory.instantiate(support(), this);
    }

    /**
     * @since 1.2
     */
    @SuppressWarnings("unchecked")
    public S instantiate(byte[] key, byte[] value)
        throws FetchException
    {
        return (S) mInstanceFactory.instantiate(support(), key, value, this);
    }

    @SuppressWarnings("unchecked")
    public S instantiate(RawSupport<S> support) {
        return (S) mInstanceFactory.instantiate(support, this);
    }

    @SuppressWarnings("unchecked")
    public S instantiate(RawSupport<S> support, byte[] key, byte[] value)
        throws FetchException
    {
        return (S) mInstanceFactory.instantiate(support, key, value, this);
    }

    public byte[] encodePrimaryKey(S storable) {
        return encodePrimaryKey(storable, 0, mPkPropertyCount);
    }

    public byte[] encodePrimaryKey(Object[] values) {
        return encodePrimaryKey(values, 0, mPkPropertyCount);
    }

    /**
     * @since 1.2
     */
    public RawSupport<S> getSupport() {
        return mSupport;
    }

    private RawSupport<S> support() {
        RawSupport<S> support = mSupport;
        if (support == null) {
            throw new IllegalStateException("No RawSupport");
        }
        return support;
    }

    /**
     * Convenient access to all the storable properties.
     */
    public Map<String, ? extends StorableProperty<S>> getAllProperties() {
        return StorableIntrospector.examine(getStorableType()).getAllProperties();
    }

    /**
     * Convenient way to define the clustered primary key index
     * descriptor. Direction can be specified by prefixing the property name
     * with a '+' or '-'. If unspecified, direction is assumed to be ascending.
     */
    @SuppressWarnings("unchecked")
    public StorableIndex<S> buildPkIndex(String... propertyNames) {
        Map<String, ? extends StorableProperty<S>> map = getAllProperties();
        int length = propertyNames.length;
        StorableProperty<S>[] properties = new StorableProperty[length];
        Direction[] directions = new Direction[length];
        for (int i=0; i<length; i++) {
            String name = propertyNames[i];
            char c = name.charAt(0);
            Direction dir = Direction.fromCharacter(c);
            if (dir != Direction.UNSPECIFIED || c == Direction.UNSPECIFIED.toCharacter()) {
                name = name.substring(1);
            } else {
                // Default to ascending if not specified.
                dir = Direction.ASCENDING;
            }
            if ((properties[i] = map.get(name)) == null) {
                throw new IllegalArgumentException("Unknown property: " + name);
            }
            directions[i] = dir;
        }
        return new StorableIndex<S>(properties, directions, true, true);
    }

    /**
     * Decode the primary key into properties of the storable.
     */
    public abstract void decodePrimaryKey(S storable, byte[] bytes)
        throws CorruptEncodingException;

    /**
     * Encode all properties of the storable excluding the primary key.
     */
    public abstract byte[] encodeData(S storable);

    /**
     * Decode the data into properties of the storable.
     */
    public abstract void decodeData(S storable, byte[] bytes)
        throws CorruptEncodingException;
}
