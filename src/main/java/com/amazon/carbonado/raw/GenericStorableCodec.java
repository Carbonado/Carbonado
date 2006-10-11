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

import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.IntHashMap;
import org.cojen.util.KeyFactory;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.spi.CodeBuilderUtil;
import com.amazon.carbonado.util.ThrowUnchecked;
import com.amazon.carbonado.util.QuickConstructorGenerator;

/**
 * Generic codec that supports any kind of storable by auto-generating and
 * caching storable implementations.
 *
 * @author Brian S O'Neill
 * @see GenericStorableCodecFactory
 */
public class GenericStorableCodec<S extends Storable> implements StorableCodec<S> {
    private static final String BLANK_KEY_FIELD_NAME = "blankKey$";
    private static final String CODEC_FIELD_NAME = "codec$";
    private static final String ASSIGN_CODEC_METHOD_NAME = "assignCodec$";

    // Maps GenericEncodingStrategy instances to GenericStorableCodec instances.
    private static final Map cCache = new SoftValuedHashMap();

    /**
     * Returns an instance of the codec. The Storable type itself may be an
     * interface or a class. If it is a class, then it must not be final, and
     * it must have a public, no-arg constructor.
     *
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, encode a storable layout generation
     * value in one or four bytes. Generation 0..127 is encoded in one byte, and
     * 128..max is encoded in four bytes, with the most significant bit set.
     * @throws SupportException if Storable is not supported
     * @throws amazon.carbonado.MalformedTypeException if Storable type is not well-formed
     * @throws IllegalArgumentException if type is null
     */
    @SuppressWarnings("unchecked")
    static synchronized <S extends Storable> GenericStorableCodec<S> getInstance
        (GenericStorableCodecFactory factory,
         GenericEncodingStrategy<S> encodingStrategy, boolean isMaster, Layout layout)
        throws SupportException
    {
        Object key;
        if (layout == null) {
            key = KeyFactory.createKey(new Object[] {encodingStrategy, isMaster});
        } else {
            key = KeyFactory.createKey
                (new Object[] {encodingStrategy, isMaster, factory, layout.getGeneration()});
        }

        GenericStorableCodec<S> codec = (GenericStorableCodec<S>) cCache.get(key);
        if (codec == null) {
            codec = new GenericStorableCodec<S>
                (factory,
                 encodingStrategy.getType(),
                 generateStorable(encodingStrategy, isMaster, layout),
                 encodingStrategy,
                 layout);
            cCache.put(key, codec);
        }

        return codec;
    }

    @SuppressWarnings("unchecked")
    private static <S extends Storable> Class<? extends S> generateStorable
        (GenericEncodingStrategy<S> encodingStrategy, boolean isMaster, Layout layout)
        throws SupportException
    {
        final Class<S> storableClass = encodingStrategy.getType();
        final Class<? extends S> abstractClass =
            RawStorableGenerator.getAbstractClass(storableClass, isMaster);
        final int generation = layout == null ? -1 : layout.getGeneration();

        ClassInjector ci = ClassInjector.create
            (storableClass.getName(), abstractClass.getClassLoader());

        ClassFile cf = new ClassFile(ci.getClassName(), abstractClass);
        cf.markSynthetic();
        cf.setSourceFile(GenericStorableCodec.class.getName());
        cf.setTarget("1.5");

        // Declare some types.
        final TypeDesc rawSupportType = TypeDesc.forClass(RawSupport.class);
        final TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
        final TypeDesc[] byteArrayParam = {byteArrayType};
        final TypeDesc codecType = TypeDesc.forClass(GenericStorableCodec.class);
        final TypeDesc decoderType = TypeDesc.forClass(Decoder.class);
        final TypeDesc weakRefType = TypeDesc.forClass(WeakReference.class);

        // If Layout provided, then keep a (weak) static reference to this
        // GenericStorableCodec in order to get decoders for different
        // generations. It is assigned a value after the class is loaded via a
        // public static method. It can only be assigned once.
        if (layout != null) {
            cf.addField(Modifiers.PRIVATE.toStatic(true), CODEC_FIELD_NAME, weakRefType);
            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC.toStatic(true), ASSIGN_CODEC_METHOD_NAME,
                                         null, new TypeDesc[] {weakRefType});
            CodeBuilder b = new CodeBuilder(mi);
            b.loadStaticField(CODEC_FIELD_NAME, weakRefType);
            Label done = b.createLabel();
            b.ifNullBranch(done, false);
            b.loadLocal(b.getParameter(0));
            b.storeStaticField(CODEC_FIELD_NAME, weakRefType);
            done.setLocation();
            b.returnVoid();
        }

        // Add constructor that accepts a RawSupport.
        {
            TypeDesc[] params = {rawSupportType};
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeSuperConstructor(params);
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
            b.loadLocal(b.getParameter(1));
            b.loadLocal(b.getParameter(2));
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

            // TODO: Consider caching generated key. Rebuild if null or if pk is dirty.

            // assembler            = b
            // properties           = null (defaults to all key properties)
            // instanceVar          = null (null means "this")
            // adapterInstanceClass = null (null means use instanceVar, in this case is "this")
            // useReadMethods       = false (will read fields directly)
            // partialStartVar      = null (only support encoding all properties)
            // partialEndVar        = null (only support encoding all properties)
            LocalVariable encodedVar =
                encodingStrategy.buildKeyEncoding(b, null, null, null, false, null, null);

            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        }

        // byte[] encodeData()
        {
            // Encoding non-primary key data properties.
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED,
                                         RawStorableGenerator.ENCODE_DATA_METHOD_NAME,
                                         byteArrayType, null);
            CodeBuilder b = new CodeBuilder(mi);

            // assembler            = b
            // properties           = null (defaults to all non-key properties)
            // instanceVar          = null (null means "this")
            // adapterInstanceClass = null (null means use instanceVar, in this case is "this")
            // useReadMethods       = false (will read fields directly)
            // generation           = generation
            LocalVariable encodedVar =
                encodingStrategy.buildDataEncoding(b, null, null, null, false, generation);

            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        }

        // void decodeKey(byte[])
        {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED,
                                         RawStorableGenerator.DECODE_KEY_METHOD_NAME,
                                         null, byteArrayParam);
            CodeBuilder b = new CodeBuilder(mi);

            // assembler            = b
            // properties           = null (defaults to all key properties)
            // instanceVar          = null (null means "this")
            // adapterInstanceClass = null (null means use instanceVar, in this case is "this")
            // useWriteMethods      = false (will set fields directly)
            // encodedVar           = references byte array with encoded key
            encodingStrategy.buildKeyDecoding(b, null, null, null, false, b.getParameter(0));

            b.returnVoid();
        }

        // void decodeData(byte[])
        {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED,
                                         RawStorableGenerator.DECODE_DATA_METHOD_NAME,
                                         null, byteArrayParam);
            CodeBuilder b = new CodeBuilder(mi);
            Label altGenerationHandler = b.createLabel();

            // assembler            = b
            // properties           = null (defaults to all non-key properties)
            // instanceVar          = null (null means "this")
            // adapterInstanceClass = null (null means use instanceVar, in this case is "this")
            // useWriteMethods      = false (will set fields directly)
            // generation           = generation
            // altGenerationHandler = altGenerationHandler
            // encodedVar           = references byte array with encoded data
            encodingStrategy.buildDataDecoding
                (b, null, null, null, false, generation, altGenerationHandler, b.getParameter(0));

            b.returnVoid();

            // Support decoding alternate generation.

            altGenerationHandler.setLocation();
            LocalVariable actualGeneration = b.createLocalVariable(null, TypeDesc.INT);
            b.storeLocal(actualGeneration);

            b.loadStaticField(CODEC_FIELD_NAME, weakRefType);
            b.invokeVirtual(weakRefType, "get", TypeDesc.OBJECT, null);
            b.dup();
            Label haveCodec = b.createLabel();
            b.ifNullBranch(haveCodec, false);

            // Codec got reclaimed, which is unlikely to happen during normal
            // use since it must be referenced by the storage object.
            b.pop(); // Don't need the duped codec instance.
            CodeBuilderUtil.throwException(b, IllegalStateException.class, "Codec missing");

            haveCodec.setLocation();
            b.checkCast(codecType);
            b.loadLocal(actualGeneration);
            b.invokeVirtual(codecType, "getDecoder", decoderType, new TypeDesc[] {TypeDesc.INT});
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeInterface(decoderType, "decode", null,
                              new TypeDesc[] {TypeDesc.forClass(Storable.class), byteArrayType});

            b.returnVoid();
        }

        return ci.defineClass(cf);
    }

    private final GenericStorableCodecFactory mFactory;

    private final Class<S> mType;

    private final Class<? extends S> mStorableClass;

    // Weakly reference the encoding strategy because of the way
    // GenericStorableCodec instances are cached in a SoftValuedHashMap.
    // GenericStorableCodec can still be reclaimed by the garbage collector.
    private final WeakReference<GenericEncodingStrategy<S>> mEncodingStrategy;

    private final GenericInstanceFactory mInstanceFactory;

    private final SearchKeyFactory<S> mPrimaryKeyFactory;

    // Maps OrderedProperty[] keys to SearchKeyFactory instances.
    private final Map mSearchKeyFactories = new SoftValuedHashMap();

    private final Layout mLayout;

    // Maps layout generations to Decoders.
    private IntHashMap mDecoders;

    private GenericStorableCodec(GenericStorableCodecFactory factory,
                                 Class<S> type, Class<? extends S> storableClass,
                                 GenericEncodingStrategy<S> encodingStrategy,
                                 Layout layout) {
        mFactory = factory;
        mType = type;
        mStorableClass = storableClass;
        mEncodingStrategy = new WeakReference<GenericEncodingStrategy<S>>(encodingStrategy);
        mInstanceFactory = QuickConstructorGenerator
            .getInstance(storableClass, GenericInstanceFactory.class);
        mPrimaryKeyFactory = getSearchKeyFactory(encodingStrategy.gatherAllKeyProperties());
        mLayout = layout;

        if (layout != null) {
            try {
                // Assign static reference back to this codec.
                Method m = storableClass.getMethod
                    (ASSIGN_CODEC_METHOD_NAME, WeakReference.class);
                m.invoke(null, new WeakReference(this));
            } catch (Exception e) {
                ThrowUnchecked.fireFirstDeclaredCause(e);
            }
        }
    }

    /**
     * Returns the type of Storable that code is generated for.
     */
    public final Class<S> getStorableType() {
        return mType;
    }

    /**
     * Instantiate a Storable with no key or value defined yet.
     *
     * @param support binds generated storable with a storage layer
     */
    @SuppressWarnings("unchecked")
    public S instantiate(RawSupport<S> support) {
        return (S) mInstanceFactory.instantiate(support);
    }

    /**
     * Instantiate a Storable with a specific key and value.
     *
     * @param support binds generated storable with a storage layer
     */
    @SuppressWarnings("unchecked")
    public S instantiate(RawSupport<S> support, byte[] key, byte[] value)
        throws FetchException
    {
        return (S) mInstanceFactory.instantiate(support, key, value);
    }

    public StorableIndex<S> getPrimaryKeyIndex() {
        return getEncodingStrategy().getPrimaryKeyIndex();
    }

    public int getPrimaryKeyPrefixLength() {
        return getEncodingStrategy().getConstantKeyPrefixLength();
    }

    public byte[] encodePrimaryKey(S storable) {
        return mPrimaryKeyFactory.encodeSearchKey(storable);
    }

    public byte[] encodePrimaryKey(S storable, int rangeStart, int rangeEnd) {
        return mPrimaryKeyFactory.encodeSearchKey(storable, rangeStart, rangeEnd);
    }

    public byte[] encodePrimaryKey(Object[] values) {
        return mPrimaryKeyFactory.encodeSearchKey(values);
    }

    public byte[] encodePrimaryKey(Object[] values, int rangeStart, int rangeEnd) {
        return mPrimaryKeyFactory.encodeSearchKey(values, rangeStart, rangeEnd);
    }

    public byte[] encodePrimaryKeyPrefix() {
        return mPrimaryKeyFactory.encodeSearchKeyPrefix();
    }

    /**
     * Returns a concrete Storable implementation, which is fully
     * thread-safe. It has two constructors defined:
     *
     * <pre>
     * public &lt;init&gt;(Storage, RawSupport);
     *
     * public &lt;init&gt;(Storage, RawSupport, byte[] key, byte[] value);
     * </pre>
     *
     * Convenience methods are provided in this class to instantiate the
     * generated Storable.
     */
    public Class<? extends S> getStorableClass() {
        return mStorableClass;
    }

    /**
     * Returns a search key factory, which is useful for implementing indexes
     * and queries.
     *
     * @param properties properties to build the search key from
     */
    @SuppressWarnings("unchecked")
    public SearchKeyFactory<S> getSearchKeyFactory(OrderedProperty<S>[] properties) {
        // This KeyFactory makes arrays work as hashtable keys.
        Object key = org.cojen.util.KeyFactory.createKey(properties);

        synchronized (mSearchKeyFactories) {
            SearchKeyFactory<S> factory = (SearchKeyFactory<S>) mSearchKeyFactories.get(key);
            if (factory == null) {
                factory = generateSearchKeyFactory(properties);
                mSearchKeyFactories.put(key, factory);
            }
            return factory;
        }
    }

    /**
     * Returns a data decoder for the given generation.
     *
     * @throws FetchNoneException if generation is unknown
     */
    public Decoder<S> getDecoder(int generation) throws FetchNoneException, FetchException {
        try {
            synchronized (mLayout) {
                IntHashMap decoders = mDecoders;
                if (decoders == null) {
                    mDecoders = decoders = new IntHashMap();
                }
                Decoder<S> decoder = (Decoder<S>) decoders.get(generation);
                if (decoder == null) {
                    decoder = generateDecoder(generation);
                    mDecoders.put(generation, decoder);
                }
                return decoder;
            }
        } catch (NullPointerException e) {
            if (mLayout == null) {
                throw new FetchNoneException("Layout evolution not supported");
            }
            throw e;
        }
    }

    private GenericEncodingStrategy<S> getEncodingStrategy() {
        // Should never be null, even though it is weakly referenced. As long
        // as this class can be reached by the cache, the encoding strategy
        // object exists since it is the cache key.
        return mEncodingStrategy.get();
    }

    @SuppressWarnings("unchecked")
    private SearchKeyFactory<S> generateSearchKeyFactory(OrderedProperty<S>[] properties) {
        GenericEncodingStrategy encodingStrategy = getEncodingStrategy();

        ClassInjector ci;
        {
            StringBuilder b = new StringBuilder();
            b.append(mType.getName());
            b.append('$');
            for (OrderedProperty property : properties) {
                if (property.getDirection() == Direction.UNSPECIFIED) {
                    property = property.direction(Direction.ASCENDING);
                }
                try {
                    property.appendTo(b);
                } catch (java.io.IOException e) {
                    // Not gonna happen
                }
            }
            String prefix = b.toString();
            ci = ClassInjector.create(prefix, mStorableClass.getClassLoader());
        }

        ClassFile cf = new ClassFile(ci.getClassName());
        cf.addInterface(SearchKeyFactory.class);
        cf.markSynthetic();
        cf.setSourceFile(GenericStorableCodec.class.getName());
        cf.setTarget("1.5");

        // Add public no-arg constructor.
        cf.addDefaultConstructor();

        // Declare some types.
        final TypeDesc storableType = TypeDesc.forClass(Storable.class);
        final TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
        final TypeDesc objectArrayType = TypeDesc.forClass(Object[].class);
        final TypeDesc instanceType = TypeDesc.forClass(mStorableClass);

        // Define encodeSearchKey(Storable).
        try {
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, "encodeSearchKey", byteArrayType,
                 new TypeDesc[] {storableType});
            CodeBuilder b = new CodeBuilder(mi);
            b.loadLocal(b.getParameter(0));
            b.checkCast(instanceType);
            LocalVariable instanceVar = b.createLocalVariable(null, instanceType);
            b.storeLocal(instanceVar);

            // assembler            = b
            // properties           = properties to encode
            // instanceVar          = instanceVar which references storable instance
            // adapterInstanceClass = null (null means use instanceVar)
            // useReadMethods       = false (will read fields directly)
            // partialStartVar      = null (only support encoding all properties)
            // partialEndVar        = null (only support encoding all properties)
            LocalVariable encodedVar = encodingStrategy.buildKeyEncoding
                (b, properties, instanceVar, null, false, null, null);

            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        } catch (SupportException e) {
            // Shouldn't happen since all properties were checked in order
            // to create this StorableCodec.
            throw new UndeclaredThrowableException(e);
        }

        // Define encodeSearchKey(Storable, int, int).
        try {
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, "encodeSearchKey", byteArrayType,
                 new TypeDesc[] {storableType, TypeDesc.INT, TypeDesc.INT});
            CodeBuilder b = new CodeBuilder(mi);
            b.loadLocal(b.getParameter(0));
            b.checkCast(instanceType);
            LocalVariable instanceVar = b.createLocalVariable(null, instanceType);
            b.storeLocal(instanceVar);

            // assembler            = b
            // properties           = properties to encode
            // instanceVar          = instanceVar which references storable instance
            // adapterInstanceClass = null (null means use instanceVar)
            // useReadMethods       = false (will read fields directly)
            // partialStartVar      = int parameter 1, references start property index
            // partialEndVar        = int parameter 2, references end property index
            LocalVariable encodedVar = encodingStrategy.buildKeyEncoding
                (b, properties, instanceVar, null, false, b.getParameter(1), b.getParameter(2));

            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        } catch (SupportException e) {
            // Shouldn't happen since all properties were checked in order
            // to create this StorableCodec.
            throw new UndeclaredThrowableException(e);
        }

        // The Storable class that we generated earlier is a subclass of the
        // abstract class defined by StorableGenerator. StorableGenerator
        // creates static final adapter instances, with protected
        // access. Calling getSuperclass results in the exact class that
        // StorableGenerator made, which is where the fields are.
        final Class<?> adapterInstanceClass = getStorableClass().getSuperclass();

        // Define encodeSearchKey(Object[] values).
        try {
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, "encodeSearchKey", byteArrayType,
                 new TypeDesc[] {objectArrayType});
            CodeBuilder b = new CodeBuilder(mi);

            // assembler            = b
            // properties           = properties to encode
            // instanceVar          = parameter 0, an object array
            // adapterInstanceClass = adapterInstanceClass - see comment above
            // useReadMethods       = false (will read fields directly)
            // partialStartVar      = null (only support encoding all properties)
            // partialEndVar        = null (only support encoding all properties)
            LocalVariable encodedVar = encodingStrategy.buildKeyEncoding
                (b, properties, b.getParameter(0), adapterInstanceClass, false, null, null);

            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        } catch (SupportException e) {
            // Shouldn't happen since all properties were checked in order
            // to create this StorableCodec.
            throw new UndeclaredThrowableException(e);
        }

        // Define encodeSearchKey(Object[] values, int, int).
        try {
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, "encodeSearchKey", byteArrayType,
                 new TypeDesc[] {objectArrayType, TypeDesc.INT, TypeDesc.INT});
            CodeBuilder b = new CodeBuilder(mi);

            // assembler            = b
            // properties           = properties to encode
            // instanceVar          = parameter 0, an object array
            // adapterInstanceClass = adapterInstanceClass - see comment above
            // useReadMethods       = false (will read fields directly)
            // partialStartVar      = int parameter 1, references start property index
            // partialEndVar        = int parameter 2, references end property index
            LocalVariable encodedVar = encodingStrategy.buildKeyEncoding
                (b, properties, b.getParameter(0), adapterInstanceClass,
                 false, b.getParameter(1), b.getParameter(2));

            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        } catch (SupportException e) {
            // Shouldn't happen since all properties were checked in order
            // to create this StorableCodec.
            throw new UndeclaredThrowableException(e);
        }

        // Define encodeSearchKeyPrefix().
        try {
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, "encodeSearchKeyPrefix", byteArrayType, null);
            CodeBuilder b = new CodeBuilder(mi);

            if (encodingStrategy.getKeyPrefixPadding() == 0 &&
                encodingStrategy.getKeySuffixPadding() == 0) {
                // Return null instead of a zero-length array.
                b.loadNull();
                b.returnValue(byteArrayType);
            } else {
                // Build array once and re-use. Trust that no one modifies it.
                cf.addField(Modifiers.PRIVATE.toStatic(true).toFinal(true),
                            BLANK_KEY_FIELD_NAME, byteArrayType);
                b.loadStaticField(BLANK_KEY_FIELD_NAME, byteArrayType);
                b.returnValue(byteArrayType);

                // Create static initializer to set field.
                mi = cf.addInitializer();
                b = new CodeBuilder(mi);

                // assembler            = b
                // properties           = no parameters - we just want the key prefix
                // instanceVar          = null (no parameters means we don't need this)
                // adapterInstanceClass = null (no parameters means we don't need this)
                // useReadMethods       = false (no parameters means we don't need this)
                // partialStartVar      = null (no parameters means we don't need this)
                // partialEndVar        = null (no parameters means we don't need this)
                LocalVariable encodedVar = encodingStrategy.buildKeyEncoding
                    (b, new OrderedProperty[0], null, null, false, null, null);

                b.loadLocal(encodedVar);
                b.storeStaticField(BLANK_KEY_FIELD_NAME, byteArrayType);
                b.returnVoid();
            }
        } catch (SupportException e) {
            // Shouldn't happen since all properties were checked in order
            // to create this StorableCodec.
            throw new UndeclaredThrowableException(e);
        }

        Class<? extends SearchKeyFactory> clazz = ci.defineClass(cf);
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new UndeclaredThrowableException(e);
        } catch (IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private Decoder<S> generateDecoder(int generation) throws FetchException {
        // Create an encoding strategy against the reconstructed storable.
        GenericEncodingStrategy<? extends Storable> altStrategy;
        try {
            Class<? extends Storable> altStorable = mLayout.getGeneration(generation)
                .reconstruct(mStorableClass.getClassLoader());
            altStrategy = mFactory.createStrategy(altStorable, null);
        } catch (RepositoryException e) {
            throw new CorruptEncodingException(e);
        }

        ClassInjector ci = ClassInjector.create(mType.getName(), mStorableClass.getClassLoader());
        ClassFile cf = new ClassFile(ci.getClassName());
        cf.addInterface(Decoder.class);
        cf.markSynthetic();
        cf.setSourceFile(GenericStorableCodec.class.getName());
        cf.setTarget("1.5");

        // Add public no-arg constructor.
        cf.addDefaultConstructor();

        // Declare some types.
        final TypeDesc storableType = TypeDesc.forClass(Storable.class);
        final TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);

        // Define the required decode method.
        MethodInfo mi = cf.addMethod
            (Modifiers.PUBLIC, "decode", null, new TypeDesc[] {storableType, byteArrayType});
        CodeBuilder b = new CodeBuilder(mi);

        LocalVariable uncastDestVar = b.getParameter(0);
        b.loadLocal(uncastDestVar);
        LocalVariable destVar = b.createLocalVariable(null, TypeDesc.forClass(mStorableClass));
        b.checkCast(destVar.getType());
        b.storeLocal(destVar);
        LocalVariable dataVar = b.getParameter(1);

        // assembler            = b
        // properties           = null (defaults to all non-key properties)
        // instanceVar          = "dest" storable
        // adapterInstanceClass = null (null means use instanceVar, in this case is "dest")
        // useWriteMethods      = false (will set fields directly)
        // generation           = generation
        // altGenerationHandler = null (generation should match)
        // encodedVar           = "data" byte array
        try {
            altStrategy.buildDataDecoding
                (b, null, destVar, null, false, generation, null, dataVar);
        } catch (SupportException e) {
            throw new CorruptEncodingException(e);
        }

        b.returnVoid();

        Class<? extends Decoder> clazz = ci.defineClass(cf);
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new UndeclaredThrowableException(e);
        } catch (IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /**
     * Creates custom raw search keys for {@link Storable} types. It is
     * intended for supporting queries and indexes.
     */
    public interface SearchKeyFactory<S extends Storable> {
        /**
         * Build a search key by extracting all the desired properties from the
         * given storable.
         *
         * @param storable extract a subset of properties from this instance
         * @return raw search key
         */
        byte[] encodeSearchKey(S storable);

        /**
         * Build a search key by extracting all the desired properties from the
         * given storable.
         *
         * @param storable extract a subset of properties from this instance
         * @param rangeStart index of first property to use. Its value must be less
         * than the count of properties used by this factory.
         * @param rangeEnd index of last property to use, exlusive. Its value must
         * be less than or equal to the count of properties used by this factory.
         * @return raw search key
         */
        byte[] encodeSearchKey(S storable, int rangeStart, int rangeEnd);

        /**
         * Build a search key by supplying property values without a storable.
         *
         * @param values values to build into a key. It must be long enough to
         * accommodate all of properties used by this factory.
         * @return raw search key
         */
        byte[] encodeSearchKey(Object[] values);

        /**
         * Build a search key by supplying property values without a storable.
         *
         * @param values values to build into a key. The length may be less than
         * the amount of properties used by this factory. It must not be less than the
         * difference between rangeStart and rangeEnd.
         * @param rangeStart index of first property to use. Its value must be less
         * than the count of properties used by this factory.
         * @param rangeEnd index of last property to use, exlusive. Its value must
         * be less than or equal to the count of properties used by this factory.
         * @return raw search key
         */
        byte[] encodeSearchKey(Object[] values, int rangeStart, int rangeEnd);

        /**
         * Returns the search key for when there are no values. Returned value
         * may be null.
         */
        byte[] encodeSearchKeyPrefix();
    }

    /**
     * Used for decoding different generations of Storable.
     */
    public interface Decoder<S extends Storable> {
        /**
         * @param dest storable to receive decoded properties
         * @param data decoded into properties, some of which may be dropped if
         * destination storable doesn't have it
         */
        void decode(S dest, byte[] data);
    }
}
