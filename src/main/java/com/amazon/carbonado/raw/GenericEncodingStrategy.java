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

import java.lang.reflect.Method;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.cojen.classfile.CodeAssembler;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.Opcode;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.BeanComparator;
import org.cojen.util.BeanIntrospector;
import org.cojen.util.BeanProperty;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.Clob;
import com.amazon.carbonado.lob.Lob;

import static com.amazon.carbonado.gen.StorableGenerator.*;
import com.amazon.carbonado.gen.TriggerSupport;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;

/**
 * Generates bytecode instructions for encoding/decoding Storable properties
 * to/from raw bytes.
 *
 * <p>Note: subclasses must override and specialize the hashCode and equals
 * methods. Failure to do so interferes with {@link StorableCodecFactory}'s
 * generated code cache.
 *
 * @author Brian S O'Neill
 */
public class GenericEncodingStrategy<S extends Storable> {
    private static enum Mode { KEY, DATA, SERIAL }

    private final Class<S> mType;
    private final StorableIndex<S> mPkIndex;

    private final int mKeyPrefixPadding;
    private final int mKeySuffixPadding;
    private final int mDataPrefixPadding;
    private final int mDataSuffixPadding;

    /**
     * @param type type of Storable to generate code for
     * @param pkIndex specifies sequence and ordering of key properties (optional)
     */
    public GenericEncodingStrategy(Class<S> type, StorableIndex<S> pkIndex) {
        this(type, pkIndex, 0, 0, 0, 0);
    }

    /**
     * @param type type of Storable to generate code for
     * @param pkIndex specifies sequence and ordering of key properties (optional)
     * @param keyPrefixPadding amount of padding bytes at start of keys
     * @param keySuffixPadding amount of padding bytes at end of keys
     * @param dataPrefixPadding amount of padding bytes at start of data values
     * @param dataSuffixPadding amount of padding bytes at end of data values
     */
    @SuppressWarnings("unchecked")
    public GenericEncodingStrategy(Class<S> type, StorableIndex<S> pkIndex,
                                   int keyPrefixPadding, int keySuffixPadding,
                                   int dataPrefixPadding, int dataSuffixPadding) {
        mType = type;

        if (keyPrefixPadding < 0 || keySuffixPadding < 0 ||
            dataPrefixPadding < 0 || dataSuffixPadding < 0) {
            throw new IllegalArgumentException();
        }
        mKeyPrefixPadding = keyPrefixPadding;
        mKeySuffixPadding = keySuffixPadding;
        mDataPrefixPadding = dataPrefixPadding;
        mDataSuffixPadding = dataSuffixPadding;

        if (pkIndex == null) {
            Map<String, ? extends StorableProperty<S>> map =
                StorableIntrospector.examine(mType).getPrimaryKeyProperties();

            StorableProperty<S>[] properties = new StorableProperty[map.size()];
            map.values().toArray(properties);

            Direction[] directions = new Direction[map.size()];
            Arrays.fill(directions, Direction.UNSPECIFIED);

            pkIndex = new StorableIndex<S>(properties, directions, true);
        }

        mPkIndex = pkIndex;
    }

    /**
     * Generates bytecode instructions to encode properties. The encoding is
     * suitable for "key" encoding, which means it is correctly comparable.
     *
     * <p>Note: if a partialStartVar is provided and this strategy has a key
     * prefix, the prefix is allocated only if the runtime value of
     * partialStartVar is zero. Likewise, if a partialEndVar is provided and
     * this strategy has a key suffix, the suffix is allocated only of the
     * runtime value of partialEndVar is one less than the property count.
     *
     * @param assembler code assembler to receive bytecode instructions
     * @param properties specific properties to encode, defaults to all key
     * properties if null
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are read from the runtime value of this array instead
     * of a Storable instance.
     * @param adapterInstanceClass class containing static references to
     * adapter instances - defaults to instanceVar
     * @param useReadMethods when true, access properties by public read
     * methods instead of protected fields - should be used if class being
     * generated doesn't have access to these fields
     * @param partialStartVar optional variable for supporting partial key
     * generation. It must be an int, whose runtime value must be less than the
     * properties array length. It marks the range start of the partial
     * property range.
     * @param partialEndVar optional variable for supporting partial key
     * generation. It must be an int, whose runtime value must be less than or
     * equal to the properties array length. It marks the range end (exclusive)
     * of the partial property range.
     *
     * @return local variable referencing a byte array with encoded key
     *
     * @throws SupportException if any property type is not supported
     * @throws IllegalArgumentException if assembler is null, or if instanceVar
     * is not the correct instance type, or if partial variable types are not
     * ints
     */
    public LocalVariable buildKeyEncoding(CodeAssembler assembler,
                                          OrderedProperty<S>[] properties,
                                          LocalVariable instanceVar,
                                          Class<?> adapterInstanceClass,
                                          boolean useReadMethods,
                                          LocalVariable partialStartVar,
                                          LocalVariable partialEndVar)
        throws SupportException
    {
        properties = ensureKeyProperties(properties);
        return buildEncoding(Mode.KEY, assembler,
                             extractProperties(properties), extractDirections(properties),
                             instanceVar, adapterInstanceClass,
                             useReadMethods,
                             -1, // no generation support
                             partialStartVar, partialEndVar);
    }

    /**
     * Generates bytecode instructions to decode properties. A
     * CorruptEncodingException may be thrown from generated code.
     *
     * @param assembler code assembler to receive bytecode instructions
     * @param properties specific properties to decode, defaults to all key
     * properties if null
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are placed into the runtime value of this array instead
     * of a Storable instance.
     * @param adapterInstanceClass class containing static references to
     * adapter instances - defaults to instanceVar
     * @param useWriteMethods when true, set properties by public write
     * methods instead of protected fields - should be used if class being
     * generated doesn't have access to these fields
     * @param encodedVar required variable, which must be a byte array. At
     * runtime, it references an encoded key.
     *
     * @throws SupportException if any property type is not supported
     * @throws IllegalArgumentException if assembler is null, or if instanceVar
     * is not the correct instance type, or if encodedVar is not a byte array
     */
    public void buildKeyDecoding(CodeAssembler assembler,
                                 OrderedProperty<S>[] properties,
                                 LocalVariable instanceVar,
                                 Class<?> adapterInstanceClass,
                                 boolean useWriteMethods,
                                 LocalVariable encodedVar)
        throws SupportException
    {
        properties = ensureKeyProperties(properties);
        buildDecoding(Mode.KEY, assembler,
                      extractProperties(properties), extractDirections(properties),
                      instanceVar, adapterInstanceClass, useWriteMethods,
                      -1, null, // no generation support
                      encodedVar);
    }

    /**
     * Generates bytecode instructions to encode properties. The encoding is
     * suitable for "data" encoding, which means it is not correctly
     * comparable, but it is more efficient than key encoding. Partial encoding
     * is not supported.
     *
     * @param assembler code assembler to receive bytecode instructions
     * @param properties specific properties to encode, defaults to all non-key
     * properties if null
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are read from the runtime value of this array instead
     * of a Storable instance.
     * @param adapterInstanceClass class containing static references to
     * adapter instances - defaults to instanceVar
     * @param useReadMethods when true, access properties by public read
     * methods instead of protected fields
     * @param generation when non-negative, write a storable layout generation
     * value in one or four bytes. Generation 0..127 is encoded in one byte, and
     * 128..max is encoded in four bytes, with the most significant bit set.
     *
     * @return local variable referencing a byte array with encoded data
     *
     * @throws SupportException if any property type is not supported
     * @throws IllegalArgumentException if assembler is null, or if instanceVar
     * is not the correct instance type
     */
    public LocalVariable buildDataEncoding(CodeAssembler assembler,
                                           StorableProperty<S>[] properties,
                                           LocalVariable instanceVar,
                                           Class<?> adapterInstanceClass,
                                           boolean useReadMethods,
                                           int generation)
        throws SupportException
    {
        properties = ensureDataProperties(properties);
        return buildEncoding(Mode.DATA, assembler,
                             properties, null,
                             instanceVar, adapterInstanceClass,
                             useReadMethods, generation, null, null);
    }

    /**
     * Generates bytecode instructions to decode properties. A
     * CorruptEncodingException may be thrown from generated code.
     *
     * @param assembler code assembler to receive bytecode instructions
     * @param properties specific properties to decode, defaults to all non-key
     * properties if null
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are placed into the runtime value of this array instead
     * of a Storable instance.
     * @param adapterInstanceClass class containing static references to
     * adapter instances - defaults to instanceVar
     * @param useWriteMethods when true, set properties by public write
     * methods instead of protected fields - should be used if class being
     * generated doesn't have access to these fields
     * @param generation when non-negative, decoder expects a storable layout
     * generation value to match this value. Otherwise, it throws a
     * CorruptEncodingException.
     * @param altGenerationHandler if non-null and a generation is provided,
     * this label defines an alternate generation handler. It is executed
     * instead of throwing a CorruptEncodingException if the generation doesn't
     * match. The actual generation is available on the top of the stack for
     * the handler to consume.
     * @param encodedVar required variable, which must be a byte array. At
     * runtime, it references encoded data.
     *
     * @throws SupportException if any property type is not supported
     * @throws IllegalArgumentException if assembler is null, or if instanceVar
     * is not the correct instance type, or if encodedVar is not a byte array
     */
    public void buildDataDecoding(CodeAssembler assembler,
                                  StorableProperty<S>[] properties,
                                  LocalVariable instanceVar,
                                  Class<?> adapterInstanceClass,
                                  boolean useWriteMethods,
                                  int generation,
                                  Label altGenerationHandler,
                                  LocalVariable encodedVar)
        throws SupportException
    {
        properties = ensureDataProperties(properties);
        buildDecoding(Mode.DATA, assembler, properties, null,
                      instanceVar, adapterInstanceClass, useWriteMethods,
                      generation, altGenerationHandler, encodedVar);
    }

    /**
     * Generates bytecode instructions to encode properties and their
     * states. This encoding is suitable for short-term serialization only.
     *
     * @param assembler code assembler to receive bytecode instructions
     * @param properties specific properties to decode, defaults to all
     * properties if null
     * @return local variable referencing a byte array with encoded data
     * @throws SupportException if any property type is not supported
     * @since 1.2
     */
    public LocalVariable buildSerialEncoding(CodeAssembler assembler,
                                             StorableProperty<S>[] properties)
        throws SupportException
    {
        properties = ensureAllProperties(properties);
        return buildEncoding
            (Mode.SERIAL, assembler, properties, null, null, null, false, -1, null, null);
    }

    /**
     * Generates bytecode instructions to decode properties and their states. A
     * CorruptEncodingException may be thrown from generated code.
     *
     * @param assembler code assembler to receive bytecode instructions
     * @param properties specific properties to decode, defaults to all
     * properties if null
     * @param encodedVar required variable, which must be a byte array. At
     * runtime, it references encoded data.
     * @throws SupportException if any property type is not supported
     * @throws IllegalArgumentException if encodedVar is not a byte array
     * @since 1.2
     */
    public void buildSerialDecoding(CodeAssembler assembler,
                                    StorableProperty<S>[] properties,
                                    LocalVariable encodedVar)
        throws SupportException
    {
        properties = ensureAllProperties(properties);
        buildDecoding
            (Mode.SERIAL, assembler, properties, null, null, null, false, -1, null, encodedVar);
    }

    /**
     * Returns the type of Storable that code is generated for.
     */
    public final Class<S> getType() {
        return mType;
    }

    /**
     * Returns true if the type of the given property type is supported. The
     * types currently supported are primitives, primitive wrapper objects,
     * Strings, and byte arrays.
     */
    public boolean isSupported(Class<?> propertyType) {
        return isSupported(TypeDesc.forClass(propertyType));
    }

    /**
     * Returns true if the type of the given property type is supported. The
     * types currently supported are primitives, primitive wrapper objects,
     * Strings, byte arrays and Lobs.
     */
    public boolean isSupported(TypeDesc propertyType) {
        if (propertyType.toPrimitiveType() != null) {
            return true;
        }
        if (propertyType == TypeDesc.STRING || propertyType == TypeDesc.forClass(byte[].class)) {
            return true;
        }
        Class clazz = propertyType.toClass();
        if (clazz != null) {
            return Lob.class.isAssignableFrom(clazz) ||
                BigInteger.class.isAssignableFrom(clazz) ||
                BigDecimal.class.isAssignableFrom(clazz);
        }
        return false;
    }

    public int getKeyPrefixPadding() {
        return mKeyPrefixPadding;
    }

    public int getKeySuffixPadding() {
        return mKeySuffixPadding;
    }

    public int getDataPrefixPadding() {
        return mDataPrefixPadding;
    }

    public int getDataSuffixPadding() {
        return mDataSuffixPadding;
    }

    /**
     * Returns amount of prefix key bytes that encoding strategy instance
     * produces which are always the same. Default implementation returns 0.
     */
    public int getConstantKeyPrefixLength() {
        return 0;
    }

    @Override
    public int hashCode() {
        return mType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof GenericEncodingStrategy) {
            GenericEncodingStrategy other = (GenericEncodingStrategy) obj;
            return mType == other.mType
                && mKeyPrefixPadding == other.mKeyPrefixPadding
                && mKeySuffixPadding == other.mKeySuffixPadding
                && mDataPrefixPadding == other.mDataPrefixPadding
                && mDataSuffixPadding == other.mDataSuffixPadding;
        }
        return false;
    }

    /**
     * Returns all key properties in the form of an index.
     */
    protected StorableIndex<S> getPrimaryKeyIndex() {
        return mPkIndex;
    }

    /**
     * Returns all key properties as ordered properties, possibly with
     * unspecified directions.
     */
    protected OrderedProperty<S>[] gatherAllKeyProperties() {
        return mPkIndex.getOrderedProperties();
    }

    /**
     * Returns all non-derived data properties for storable.
     */
    @SuppressWarnings("unchecked")
    protected StorableProperty<S>[] gatherAllDataProperties() {
        Map<String, ? extends StorableProperty<S>> map =
            StorableIntrospector.examine(mType).getDataProperties();

        List<StorableProperty<S>> list = new ArrayList<StorableProperty<S>>(map.size());

        for (StorableProperty<S> property : map.values()) {
            if (!property.isDerived()) {
                list.add(property);
            }
        }

        return list.toArray(new StorableProperty[list.size()]);
    }

    /**
     * Returns all non-join, non-derived properties for storable.
     */
    @SuppressWarnings("unchecked")
    protected StorableProperty<S>[] gatherAllProperties() {
        Map<String, ? extends StorableProperty<S>> map =
            StorableIntrospector.examine(mType).getAllProperties();

        List<StorableProperty<S>> list = new ArrayList<StorableProperty<S>>(map.size());

        for (StorableProperty<S> property : map.values()) {
            if (!property.isJoin() && !property.isDerived()) {
                list.add(property);
            }
        }

        return list.toArray(new StorableProperty[list.size()]);
    }

    protected StorablePropertyInfo checkSupport(StorableProperty<S> property)
        throws SupportException
    {
        if (isSupported(property.getType())) {
            return new StorablePropertyInfo(property);
        }

        // Look for an adapter that will allow this property to be supported.
        if (property.getAdapter() != null) {
            StorablePropertyAdapter adapter = property.getAdapter();
            for (Class<?> storageType : adapter.getStorageTypePreferences()) {
                if (!isSupported(storageType)) {
                    continue;
                }

                if (property.isNullable() && storageType.isPrimitive()) {
                    continue;
                }

                Method fromStorage, toStorage;
                fromStorage = adapter.findAdaptMethod(storageType, property.getType());
                if (fromStorage == null) {
                    continue;
                }
                toStorage = adapter.findAdaptMethod(property.getType(), storageType);
                if (toStorage != null) {
                    return new StorablePropertyInfo(property, storageType, fromStorage, toStorage);
                }
            }
        }

        throw notSupported(property);
    }

    @SuppressWarnings("unchecked")
    protected StorablePropertyInfo[] checkSupport(StorableProperty<S>[] properties)
        throws SupportException
    {
        int length = properties.length;
        StorablePropertyInfo[] infos = new StorablePropertyInfo[length];
        for (int i=0; i<length; i++) {
            infos[i] = checkSupport(properties[i]);
        }
        return infos;
    }

    /**
     * Second phase encoding, which does nothing by default.
     *
     * @param dataVar local variable referencing a byte array with data
     * @param prefix prefix of byte array to preserve
     * @param suffix suffix of byte array to preserve
     */
    protected void extraDataEncoding(CodeAssembler a,
                                     LocalVariable dataVar, int prefix, int suffix)
    {
    }

    /**
     * Second phase decoding, which does nothing by default.
     *
     * @param dataVar local variable referencing a byte array with data
     */
    protected void extraDataDecoding(CodeAssembler a,
                                     LocalVariable dataVar, int prefix, int suffix)
    {
    }

    private SupportException notSupported(StorableProperty<S> property) {
        return notSupported(property.getName(),
                            TypeDesc.forClass(property.getType()).getFullName());
    }

    private SupportException notSupported(String propertyName, String typeName) {
        return new SupportException
            ("Type \"" + typeName +
             "\" not supported for property \"" + propertyName + '"');
    }

    private OrderedProperty<S>[] ensureKeyProperties(OrderedProperty<S>[] properties) {
        if (properties == null) {
            properties = gatherAllKeyProperties();
        } else {
            for (Object prop : properties) {
                if (prop == null) {
                    throw new IllegalArgumentException();
                }
            }
        }
        return properties;
    }

    @SuppressWarnings("unchecked")
    private StorableProperty<S>[] extractProperties(OrderedProperty<S>[] ordered) {
        StorableProperty<S>[] properties = new StorableProperty[ordered.length];
        for (int i=0; i<ordered.length; i++) {
            ChainedProperty chained = ordered[i].getChainedProperty();
            if (chained.getChainCount() > 0) {
                throw new IllegalArgumentException();
            }
            properties[i] = chained.getPrimeProperty();
        }
        return properties;
    }

    private Direction[] extractDirections(OrderedProperty<S>[] ordered) {
        Direction[] directions = new Direction[ordered.length];
        for (int i=0; i<ordered.length; i++) {
            directions[i] = ordered[i].getDirection();
        }
        return directions;
    }

    private StorableProperty<S>[] ensureDataProperties(StorableProperty<S>[] properties) {
        if (properties == null) {
            properties = gatherAllDataProperties();
        } else {
            for (Object prop : properties) {
                if (prop == null) {
                    throw new IllegalArgumentException();
                }
            }
        }
        return properties;
    }

    private StorableProperty<S>[] ensureAllProperties(StorableProperty<S>[] properties) {
        if (properties == null) {
            properties = gatherAllProperties();
        } else {
            for (Object prop : properties) {
                if (prop == null) {
                    throw new IllegalArgumentException();
                }
            }
            // Sort to generate more efficient code.
            properties = properties.clone();
            Arrays.sort(properties,
                        BeanComparator.forClass(StorableProperty.class).orderBy("number"));
        }
        return properties;
    }

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    private LocalVariable buildEncoding(Mode mode,
                                        CodeAssembler a,
                                        StorableProperty<S>[] properties,
                                        Direction[] directions,
                                        LocalVariable instanceVar,
                                        Class<?> adapterInstanceClass,
                                        boolean useReadMethods,
                                        int generation,
                                        LocalVariable partialStartVar,
                                        LocalVariable partialEndVar)
        throws SupportException
    {
        if (a == null) {
            throw new IllegalArgumentException();
        }
        if (partialStartVar != null && partialStartVar.getType() != TypeDesc.INT) {
            throw new IllegalArgumentException();
        }
        if (partialEndVar != null && partialEndVar.getType() != TypeDesc.INT) {
            throw new IllegalArgumentException();
        }

        // Encoding order is:
        //
        // 1. Prefix
        // 2. Generation
        // 3. Property states (if Mode.SERIAL)
        // 4. Properties
        // 5. Suffix

        final int prefix;
        switch (mode) {
        default: 
            prefix = 0;
            break;
        case KEY:
            prefix = mKeyPrefixPadding;
            break;
        case DATA:
            prefix = mDataPrefixPadding;
            break;
        }

        final int generationPrefix;
        if (generation < 0) {
            generationPrefix = 0;
        } else if (generation < 128) {
            generationPrefix = 1;
        } else {
            generationPrefix = 4;
        }

        final int suffix;
        switch (mode) {
        default: 
            suffix = 0;
            break;
        case KEY:
            suffix = mKeySuffixPadding;
            break;
        case DATA:
            suffix = mDataSuffixPadding;
            break;
        }

        final TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
        final LocalVariable encodedVar = a.createLocalVariable(null, byteArrayType);

        StorablePropertyInfo[] infos = checkSupport(properties);

        if (properties.length == 1) {
            // Ignore partial key encoding variables, since there can't be a
            // partial of one property.
            partialStartVar = null;
            partialEndVar = null;

            StorableProperty<S> property = properties[0];
            StorablePropertyInfo info = infos[0];

            if (mode != Mode.SERIAL && info.getStorageType().toClass() == byte[].class) {
                // Since there is only one property, and it is just a byte
                // array, optimize by not doing any fancy encoding. If the
                // property is optional, then a byte prefix is needed to
                // identify a null reference.

                loadPropertyValue(a, info, 0, useReadMethods,
                                  instanceVar, adapterInstanceClass, partialStartVar);

                boolean descending = mode == Mode.KEY
                    && directions != null && directions[0] == Direction.DESCENDING;

                TypeDesc[] params;
                if (prefix > 0 || generationPrefix > 0 || suffix > 0) {
                    a.loadConstant(prefix + generationPrefix);
                    a.loadConstant(suffix);
                    params = new TypeDesc[] {byteArrayType, TypeDesc.INT, TypeDesc.INT};
                } else {
                    params = new TypeDesc[] {byteArrayType};
                }

                if (property.isNullable()) {
                    if (descending) {
                        a.invokeStatic(KeyEncoder.class.getName(), "encodeSingleNullableDesc",
                                       byteArrayType, params);
                    } else {
                        a.invokeStatic(DataEncoder.class.getName(), "encodeSingleNullable",
                                       byteArrayType, params);
                    }
                } else if (descending) {
                    a.invokeStatic(KeyEncoder.class.getName(), "encodeSingleDesc",
                                   byteArrayType, params);
                } else if (prefix > 0 || generationPrefix > 0 || suffix > 0) {
                    a.invokeStatic(DataEncoder.class.getName(), "encodeSingle",
                                   byteArrayType, params);
                } else {
                    // Just return raw property value - no need to cache it either.
                }

                a.storeLocal(encodedVar);

                encodeGeneration(a, encodedVar, prefix, generation);

                if (mode == Mode.DATA) {
                    extraDataEncoding(a, encodedVar, prefix + generationPrefix, suffix);
                }

                return encodedVar;
            }
        }

        boolean doPartial =
            mode == Mode.SERIAL ||
            (mode == Mode.KEY && partialStartVar != null) ||
            (mode == Mode.KEY && (properties.length == 0 || partialEndVar != null));

        // Calculate exactly how many bytes are needed to encode. The length
        // is composed of a static and a variable amount. The variable amount
        // is determined at runtime.

        int staticLength = 0;
        if (mode != Mode.KEY || partialStartVar == null) {
            // Only include prefix as static if no runtime check is needed
            // against runtime partial start value.
            staticLength += prefix + generationPrefix;
        }
        if (mode != Mode.KEY || (properties.length != 0 && partialEndVar == null)) {
            // Only include suffix as static if no runtime check is needed
            // against runtime partial end value.
            staticLength += suffix;
        }

        if (mode == Mode.SERIAL) {
            // Need room to encode property states. Two bits per property, so
            // one byte can encode the state of four properties.
            staticLength += (properties.length + 3) / 4;
        }

        // Stash of properties which are loaded and locally stored before
        // entering the first loop. This avoids having to load them twice.
        LocalVariable[] stashedProperties = null;
        Boolean[] stashedFromInstances = null;

        boolean hasVariableLength;
        {
            hasVariableLength = doPartial;

            for (int i=0; i<infos.length; i++) {
                GenericPropertyInfo info = infos[i];
                int len = staticEncodingLength(info);
                if (len >= 0) {
                    if (!doPartial) {
                        staticLength += len;
                    }
                } else {
                    if (!doPartial) {
                        staticLength += ~len;
                        hasVariableLength = true;
                    }

                    if (info.getPropertyType() == info.getStorageType()) {
                        // Property won't be adapted, so loading it twice is no big deal.
                        continue;
                    }

                    if (stashedProperties == null) {
                        stashedProperties = new LocalVariable[infos.length];
                        stashedFromInstances = new Boolean[infos.length];
                    }

                    LocalVariable propVar = a.createLocalVariable(null, info.getStorageType());
                    stashedProperties[i] = propVar;

                    if (doPartial) {
                        // Initialize the stashed propery to null or zero to make
                        // the verifier happy.
                        loadBlankValue(a, propVar.getType());
                        a.storeLocal(propVar);
                    }
                }
            }
        }

        // Generate code that loops over all the properties that have a
        // variable length. Load each property and perform the necessary
        // tests to determine the exact encoding length.

        boolean hasStackVar = false;
        if (hasVariableLength) {
            Label[] entryPoints = null;

            if (mode == Mode.SERIAL || partialStartVar != null) {
                // Will jump into an arbitrary location, so always have a stack
                // variable available.
                a.loadConstant(0);
                hasStackVar = true;
            }

            if (partialStartVar != null) {
                entryPoints = jumpToPartialEntryPoints(a, partialStartVar, properties.length);
            }

            Label exitPoint = a.createLabel();

            LocalVariable stateFieldVar = null;
            for (int i=0; i<properties.length; i++) {
                StorableProperty<S> property = properties[i];
                StorablePropertyInfo info = infos[i];

                if (doPartial) {
                    if (entryPoints != null) {
                        entryPoints[i].setLocation();
                    }
                    if (partialEndVar != null) {
                        // Add code to jump out of partial.
                        a.loadConstant(i);
                        a.loadLocal(partialEndVar);
                        a.ifComparisonBranch(exitPoint, ">=");
                    }
                } else if (staticEncodingLength(info) >= 0) {
                    continue;
                }

                Label nextProperty = null;
                if (mode == Mode.SERIAL) {
                    // Skip uninitialized properties.
                    nextProperty = a.createLabel();

                    int fieldOrdinal = property.getNumber() >> 4;
                    if (stateFieldVar != null) {
                        a.loadLocal(stateFieldVar);
                    } else {
                        a.loadThis();
                        a.loadField(PROPERTY_STATE_FIELD_NAME + fieldOrdinal, TypeDesc.INT);
                    }

                    if (i + 1 < properties.length
                        && (properties[i + 1].getNumber() >> 4) == fieldOrdinal)
                    {
                        if (stateFieldVar == null) {
                            // Save for use by next property.
                            stateFieldVar = a.createLocalVariable(null, TypeDesc.INT);
                            a.storeLocal(stateFieldVar);
                            a.loadLocal(stateFieldVar);
                        }
                    } else {
                        stateFieldVar = null;
                    }

                    a.loadConstant(PROPERTY_STATE_MASK << ((property.getNumber() & 0xf) * 2));
                    a.math(Opcode.IAND);
                    a.ifZeroComparisonBranch(nextProperty, "==");
                }

                TypeDesc storageType = info.getStorageType();

                if (storageType.isPrimitive()) {
                    // This should only ever get executed if implementing
                    // partial support. Otherwise, the static encoding length
                    // would have been already calculated.
                    a.loadConstant(staticEncodingLength(info));
                    if (hasStackVar) {
                        a.math(Opcode.IADD);
                    } else {
                        hasStackVar = true;
                    }
                } else if (storageType.toPrimitiveType() != null) {
                    int amt = 0;
                    switch (storageType.toPrimitiveType().getTypeCode()) {
                    case TypeDesc.BYTE_CODE:
                    case TypeDesc.BOOLEAN_CODE:
                        amt = 1;
                        break;
                    case TypeDesc.SHORT_CODE:
                    case TypeDesc.CHAR_CODE:
                        amt = 2;
                        break;
                    case TypeDesc.INT_CODE:
                    case TypeDesc.FLOAT_CODE:
                        amt = 4;
                        break;
                    case TypeDesc.LONG_CODE:
                    case TypeDesc.DOUBLE_CODE:
                        amt = 8;
                        break;
                    }

                    int extra = 0;
                    if (doPartial) {
                        // If value is null, then there may be a one byte size
                        // adjust for the null value. Otherwise it is the extra
                        // amount plus the size to encode the raw primitive
                        // value. If doPartial is false, then this extra amount
                        // was already accounted for in the static encoding
                        // length.

                        switch (storageType.toPrimitiveType().getTypeCode()) {
                        case TypeDesc.BYTE_CODE:
                        case TypeDesc.SHORT_CODE:
                        case TypeDesc.CHAR_CODE:
                        case TypeDesc.INT_CODE:
                        case TypeDesc.LONG_CODE:
                            extra = 1;
                        }
                    }

                    if (!property.isNullable() || (doPartial && extra == 0)) {
                        a.loadConstant(amt);
                        if (hasStackVar) {
                            a.math(Opcode.IADD);
                        }
                        hasStackVar = true;
                    } else {
                        // Load property to test for null.
                        loadPropertyValue(stashedProperties, stashedFromInstances,
                                          a, info, i, useReadMethods,
                                          instanceVar, adapterInstanceClass, partialStartVar);

                        Label isNull = a.createLabel();
                        a.ifNullBranch(isNull, true);

                        a.loadConstant(amt);

                        if (hasStackVar) {
                            a.math(Opcode.IADD);
                            isNull.setLocation();
                            if (extra > 0) {
                                a.loadConstant(extra);
                                a.math(Opcode.IADD);
                            }
                        } else {
                            hasStackVar = true;
                            // Make sure that there is a zero (or extra) value on
                            // the stack if the isNull branch is taken.
                            Label notNull = a.createLabel();
                            a.branch(notNull);
                            isNull.setLocation();
                            a.loadConstant(extra);
                            notNull.setLocation();
                        }
                    }
                } else if (storageType == TypeDesc.STRING ||
                           storageType.toClass() == byte[].class ||
                           storageType.toClass() == BigInteger.class ||
                           storageType.toClass() == BigDecimal.class)
                {
                    loadPropertyValue(stashedProperties, stashedFromInstances,
                                      a, info, i, useReadMethods,
                                      instanceVar, adapterInstanceClass, partialStartVar);

                    String methodName;
                    if (storageType == TypeDesc.STRING) {
                        methodName = "calculateEncodedStringLength";
                    } else {
                        methodName = "calculateEncodedLength";
                    }

                    String className =
                        (mode == Mode.KEY ? KeyEncoder.class : DataEncoder.class).getName();
                    a.invokeStatic(className, methodName,
                                   TypeDesc.INT, new TypeDesc[] {storageType});
                    if (hasStackVar) {
                        a.math(Opcode.IADD);
                    } else {
                        hasStackVar = true;
                    }
                } else if (info.isLob()) {
                    // Lob locator is a long, or 8 bytes.
                    a.loadConstant(8);
                    if (hasStackVar) {
                        a.math(Opcode.IADD);
                    } else {
                        hasStackVar = true;
                    }
                } else {
                    throw notSupported(property);
                }

                if (nextProperty != null) {
                    nextProperty.setLocation();
                }
            }

            exitPoint.setLocation();

            if (mode == Mode.KEY
                && partialStartVar != null && (prefix > 0 || generationPrefix > 0))
            {
                // Prefix must be allocated only if runtime value of
                // partialStartVar is zero.
                a.loadLocal(partialStartVar);
                Label noPrefix = a.createLabel();
                a.ifZeroComparisonBranch(noPrefix, "!=");
                a.loadConstant(prefix + generationPrefix);
                if (hasStackVar) {
                    a.math(Opcode.IADD);
                } else {
                    hasStackVar = true;
                }
                noPrefix.setLocation();
            }

            if (mode == Mode.KEY && partialEndVar != null && suffix > 0) {
                // Suffix must be allocated only if runtime value of
                // partialEndVar is equal to property count.
                a.loadLocal(partialEndVar);
                Label noSuffix = a.createLabel();
                a.loadConstant(properties.length);
                a.ifComparisonBranch(noSuffix, "!=");
                a.loadConstant(suffix);
                if (hasStackVar) {
                    a.math(Opcode.IADD);
                } else {
                    hasStackVar = true;
                }
                noSuffix.setLocation();
            }
        }

        // Allocate a byte array of the exact size.
        if (hasStackVar) {
            if (staticLength > 0) {
                a.loadConstant(staticLength);
                a.math(Opcode.IADD);
            }
        } else {
            a.loadConstant(staticLength);
        }
        a.newObject(byteArrayType);
        a.storeLocal(encodedVar);

        // Now encode into the byte array.

        int constantOffset = 0;
        LocalVariable offsetVar = null;

        if (mode != Mode.KEY || partialStartVar == null) {
            // Only include prefix as constant offset if no runtime check is
            // needed against runtime partial start value.
            constantOffset += prefix;
            encodeGeneration(a, encodedVar, constantOffset, generation);
            constantOffset += generationPrefix;
        }

        if (mode == Mode.SERIAL) {
            encodePropertyStates(a, encodedVar, constantOffset, properties);
            constantOffset += (properties.length + 3) / 4;

            // Some properties are skipped at runtime, so offset variable is required.
            offsetVar = a.createLocalVariable(null, TypeDesc.INT);
            a.loadConstant(constantOffset);
            a.storeLocal(offsetVar);
        }

        Label[] entryPoints = null;

        if (mode == Mode.KEY && partialStartVar != null) {
            // Will jump into an arbitrary location, so put an initial value
            // into offset variable.

            offsetVar = a.createLocalVariable(null, TypeDesc.INT);
            a.loadConstant(constantOffset);
            if (prefix > 0) {
                // Prefix is allocated only if partial start is zero. Check if
                // offset should be adjusted to skip over it.
                a.loadLocal(partialStartVar);
                Label noPrefix = a.createLabel();
                a.ifZeroComparisonBranch(noPrefix, "!=");
                a.loadConstant(prefix + generationPrefix);
                a.math(Opcode.IADD);
                encodeGeneration(a, encodedVar, prefix, generation);
                noPrefix.setLocation();
            }
            a.storeLocal(offsetVar);

            entryPoints = jumpToPartialEntryPoints(a, partialStartVar, properties.length);
        }

        Label exitPoint = a.createLabel();

        LocalVariable stateFieldVar;
        if (mode != Mode.SERIAL) {
            stateFieldVar = null;
        } else {
            stateFieldVar = a.createLocalVariable(null, TypeDesc.INT);
        }
        int lastFieldOrdinal = -1;

        for (int i=0; i<properties.length; i++) {
            StorableProperty<S> property = properties[i];
            StorablePropertyInfo info = infos[i];

            Label nextProperty = a.createLabel();

            if (doPartial) {
                if (entryPoints != null) {
                    entryPoints[i].setLocation();
                }
                if (partialEndVar != null) {
                    // Add code to jump out of partial.
                    a.loadConstant(i);
                    a.loadLocal(partialEndVar);
                    a.ifComparisonBranch(exitPoint, ">=");
                }
            }

            if (mode == Mode.SERIAL) {
                // Skip uninitialized properties.

                int fieldOrdinal = property.getNumber() >> 4;

                if (fieldOrdinal == lastFieldOrdinal) {
                    a.loadLocal(stateFieldVar);
                } else {
                    a.loadThis();
                    a.loadField(PROPERTY_STATE_FIELD_NAME + fieldOrdinal, TypeDesc.INT);
                    a.storeLocal(stateFieldVar);
                    a.loadLocal(stateFieldVar);
                    lastFieldOrdinal = fieldOrdinal;
                }

                a.loadConstant(PROPERTY_STATE_MASK << ((property.getNumber() & 0xf) * 2));
                a.math(Opcode.IAND);
                a.ifZeroComparisonBranch(nextProperty, "==");
            }

            if (info.isLob()) {
                // Need RawSupport instance for getting locator from Lob.
                pushRawSupport(a, instanceVar);
            }

            boolean fromInstance = loadPropertyValue
                (stashedProperties, stashedFromInstances,
                 a, info, i, useReadMethods,
                 instanceVar, adapterInstanceClass, partialStartVar);

            TypeDesc storageType = info.getStorageType();
            if (!property.isNullable() && storageType.toPrimitiveType() != null) {
                // Since property type is a required primitive wrapper, convert
                // to a primitive rather than encoding using the form that
                // distinguishes null.

                // Property value that was passed in may be null, which is not
                // allowed.
                if (!fromInstance && !storageType.isPrimitive()) {
                    a.dup();
                    Label notNull = a.createLabel();
                    a.ifNullBranch(notNull, false);

                    TypeDesc errorType = TypeDesc.forClass(IllegalArgumentException.class);
                    a.newObject(errorType);
                    a.dup();
                    a.loadConstant("Value for property \"" + property.getName() +
                                   "\" cannot be null");
                    a.invokeConstructor(errorType, new TypeDesc[] {TypeDesc.STRING});
                    a.throwObject();

                    notNull.setLocation();
                }

                a.convert(storageType, storageType.toPrimitiveType());
                storageType = storageType.toPrimitiveType();
            }

            if (info.isLob()) {
                // Extract locator from RawSupport.
                getLobLocator(a, info);

                // Locator is a long, so switch the type to be encoded properly.
                storageType = TypeDesc.LONG;
            }

            // Fill out remaining parameters before calling specific method
            // to encode property value.
            a.loadLocal(encodedVar);
            if (offsetVar == null) {
                a.loadConstant(constantOffset);
            } else {
                a.loadLocal(offsetVar);
            }

            boolean descending = mode == Mode.KEY
                && directions != null && directions[i] == Direction.DESCENDING;

            int amt = encodeProperty(a, storageType, mode, descending);

            if (amt > 0) {
                if (i + 1 < properties.length) {
                    // Only adjust offset if there are more properties.

                    if (offsetVar == null) {
                        constantOffset += amt;
                    } else {
                        a.loadConstant(amt);
                        a.loadLocal(offsetVar);
                        a.math(Opcode.IADD);
                        a.storeLocal(offsetVar);
                    }
                }
            } else {
                if (i + 1 >= properties.length) {
                    // Don't need to keep track of offset anymore.
                    a.pop();
                } else {
                    // Only adjust offset if there are more properties.
                    if (offsetVar == null) {
                        if (constantOffset > 0) {
                            a.loadConstant(constantOffset);
                            a.math(Opcode.IADD);
                        }
                        offsetVar = a.createLocalVariable(null, TypeDesc.INT);
                    } else {
                        a.loadLocal(offsetVar);
                        a.math(Opcode.IADD);
                    }
                    a.storeLocal(offsetVar);
                }
            }

            nextProperty.setLocation();
        }

        exitPoint.setLocation();

        if (mode == Mode.DATA) {
            extraDataEncoding(a, encodedVar, prefix + generationPrefix, suffix);
        }

        return encodedVar;
    }

    /**
     * Generates code to load a property value onto the operand stack.
     *
     * @param info info for property to load
     * @param ordinal zero-based property ordinal, used only if instanceVar
     * refers to an object array.
     * @param useReadMethod when true, access property by public read method
     * instead of protected field
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are read from the runtime value of this array instead
     * of a Storable instance.
     * @param adapterInstanceClass class containing static references to
     * adapter instances - defaults to instanceVar
     * @param partialStartVar optional variable for supporting partial key
     * generation. It must be an int, whose runtime value must be less than the
     * properties array length. It marks the range start of the partial
     * property range.
     * @return true if property was loaded from instance, false if loaded from
     * value array
     */
    protected boolean loadPropertyValue(LocalVariable[] stashedProperties,
                                        Boolean[] stashedFromInstances,
                                        CodeAssembler a,
                                        StorablePropertyInfo info, int ordinal,
                                        boolean useReadMethod,
                                        LocalVariable instanceVar,
                                        Class<?> adapterInstanceClass,
                                        LocalVariable partialStartVar)
    {
        if (stashedFromInstances != null && stashedFromInstances[ordinal] != null) {
            a.loadLocal(stashedProperties[ordinal]);
            return stashedFromInstances[ordinal];
        }

        boolean fromInstance = loadPropertyValue
            (a, info, ordinal, useReadMethod, instanceVar, adapterInstanceClass, partialStartVar);

        if (stashedProperties != null) {
            LocalVariable propVar = stashedProperties[ordinal];
            // Stash it for the next time.
            if (propVar != null) {
                a.storeLocal(propVar);
                a.loadLocal(propVar);
                stashedFromInstances[ordinal] = fromInstance;
            }
        }

        return fromInstance;
    }

    /**
     * Generates code to load a property value onto the operand stack.
     *
     * @param info info for property to load
     * @param ordinal zero-based property ordinal, used only if instanceVar
     * refers to an object array.
     * @param useReadMethod when true, access property by public read method
     * instead of protected field
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are read from the runtime value of this array instead
     * of a Storable instance.
     * @param adapterInstanceClass class containing static references to
     * adapter instances - defaults to instanceVar
     * @param partialStartVar optional variable for supporting partial key
     * generation. It must be an int, whose runtime value must be less than the
     * properties array length. It marks the range start of the partial
     * property range.
     * @return true if property was loaded from instance, false if loaded from
     * value array
     */
    protected boolean loadPropertyValue(CodeAssembler a,
                                        StorablePropertyInfo info, int ordinal,
                                        boolean useReadMethod,
                                        LocalVariable instanceVar,
                                        Class<?> adapterInstanceClass,
                                        LocalVariable partialStartVar)
    {
        if (info.isDerived()) {
            useReadMethod = true;
        }

        final TypeDesc type = info.getPropertyType();
        final TypeDesc storageType = info.getStorageType();

        final boolean isObjectArrayInstanceVar = instanceVar != null
            && instanceVar.getType() == TypeDesc.forClass(Object[].class);

        final boolean useAdapterInstance = adapterInstanceClass != null
            && info.getToStorageAdapter() != null
            && (useReadMethod || isObjectArrayInstanceVar);

        if (useAdapterInstance) {
            // Push adapter instance to stack to be used later.
            String fieldName = info.getPropertyName() + ADAPTER_FIELD_ELEMENT + 0;
            TypeDesc adapterType = TypeDesc.forClass
                (info.getToStorageAdapter().getDeclaringClass());
            a.loadStaticField
                (TypeDesc.forClass(adapterInstanceClass), fieldName, adapterType);
        }

        if (instanceVar == null) {
            a.loadThis();
            if (useReadMethod) {
                info.addInvokeReadMethod(a);
            } else {
                // Access property value directly from protected field of "this".
                if (info.getToStorageAdapter() == null) {
                    a.loadField(info.getPropertyName(), type);
                } else {
                    // Invoke adapter method.
                    a.invokeVirtual(info.getReadMethodName() + '$', storageType, null);
                }
            }
        } else if (!isObjectArrayInstanceVar) {
            a.loadLocal(instanceVar);
            if (useReadMethod) {
                info.addInvokeReadMethod(a, instanceVar.getType());
            } else {
                // Access property value directly from protected field of
                // referenced instance. Assumes code is being defined in the
                // same package or a subclass.
                if (info.getToStorageAdapter() == null) {
                    a.loadField(instanceVar.getType(), info.getPropertyName(), type);
                } else {
                    // Invoke adapter method.
                    a.invokeVirtual(instanceVar.getType(),
                                    info.getReadMethodName() + '$', storageType, null);
                }
            }
        } else {
            // Access property value from object array.

            a.loadLocal(instanceVar);
            a.loadConstant(ordinal);
            if (ordinal > 0 && partialStartVar != null) {
                a.loadLocal(partialStartVar);
                a.math(Opcode.ISUB);
            }

            a.loadFromArray(TypeDesc.OBJECT);
            a.checkCast(type.toObjectType());
            if (type.isPrimitive()) {
                a.convert(type.toObjectType(), type);
            }
        }

        if (useAdapterInstance) {
            // Invoke adapter method on instance pushed earlier.
            a.invoke(info.getToStorageAdapter());
        }

        return !isObjectArrayInstanceVar;
    }

    /**
     * Generates code that loads zero, false, or null to the stack.
     */
    private void loadBlankValue(CodeAssembler a, TypeDesc type) {
        switch (type.getTypeCode()) {
        case TypeDesc.OBJECT_CODE:
            a.loadNull();
            break;
        case TypeDesc.LONG_CODE:
            a.loadConstant(0L);
            break;
        case TypeDesc.FLOAT_CODE:
            a.loadConstant(0.0f);
            break;
        case TypeDesc.DOUBLE_CODE:
            a.loadConstant(0.0d);
            break;
        case TypeDesc.INT_CODE: default:
            a.loadConstant(0);
            break;
        }
    }

    /**
     * Returns a negative value if encoding is variable. The minimum static
     * amount is computed from the one's compliment. Of the types with variable
     * encoding lengths, only for primitives is the minimum static amount
     * returned more than zero.
     */
    private int staticEncodingLength(GenericPropertyInfo info) {
        TypeDesc type = info.getStorageType();
        TypeDesc primType = type.toPrimitiveType();

        if (primType == null) {
            if (info.isLob()) {
                // Lob locator is stored as a long.
                return 8;
            }
        } else {
            if (info.isNullable()) {
                // Type is a primitive wrapper.
                switch (primType.getTypeCode()) {
                case TypeDesc.BYTE_CODE:
                    return ~1;
                case TypeDesc.BOOLEAN_CODE:
                    return 1;
                case TypeDesc.SHORT_CODE:
                case TypeDesc.CHAR_CODE:
                    return ~1;
                case TypeDesc.INT_CODE:
                    return ~1;
                case TypeDesc.FLOAT_CODE:
                    return 4;
                case TypeDesc.LONG_CODE:
                    return ~1;
                case TypeDesc.DOUBLE_CODE:
                    return 8;
                }
            } else {
                // Type is primitive or a required primitive wrapper.
                switch (type.getTypeCode()) {
                case TypeDesc.BYTE_CODE:
                case TypeDesc.BOOLEAN_CODE:
                    return 1;
                case TypeDesc.SHORT_CODE:
                case TypeDesc.CHAR_CODE:
                    return 2;
                case TypeDesc.INT_CODE:
                case TypeDesc.FLOAT_CODE:
                    return 4;
                case TypeDesc.LONG_CODE:
                case TypeDesc.DOUBLE_CODE:
                    return 8;
                }
            }
        }

        return ~0;
    }

    /**
     * @param partialStartVar must not be null
     */
    private Label[] jumpToPartialEntryPoints(CodeAssembler a, LocalVariable partialStartVar,
                                             int propertyCount) {
        // Create all the entry points for offset var, whose locations will be
        // set later.
        int[] cases = new int[propertyCount];
        Label[] entryPoints = new Label[propertyCount];
        for (int i=0; i<propertyCount; i++) {
            cases[i] = i;
            entryPoints[i] = a.createLabel();
        }

        // Now jump in!
        Label errorLoc = a.createLabel();
        a.loadLocal(partialStartVar);
        a.switchBranch(cases, entryPoints, errorLoc);

        errorLoc.setLocation();
        TypeDesc errorType = TypeDesc.forClass(IllegalArgumentException.class);
        a.newObject(errorType);
        a.dup();
        a.loadConstant("Illegal partial start offset");
        a.invokeConstructor(errorType, new TypeDesc[] {TypeDesc.STRING});
        a.throwObject();

        return entryPoints;
    }

    /**
     * Generates code that calls an encoding method in DataEncoder or
     * KeyEncoder. Parameters must already be on the stack.
     *
     * @return 0 if an int amount is pushed onto the stack, or a positive value
     * if offset adjust amount is constant
     */
    private int encodeProperty(CodeAssembler a, TypeDesc type, Mode mode, boolean descending) {
        TypeDesc[] params = new TypeDesc[] {
            type, TypeDesc.forClass(byte[].class), TypeDesc.INT
        };

        if (type.isPrimitive()) {
            if (mode == Mode.KEY && descending) {
                a.invokeStatic(KeyEncoder.class.getName(), "encodeDesc", null, params);
            } else {
                a.invokeStatic(DataEncoder.class.getName(), "encode", null, params);
            }

            switch (type.getTypeCode()) {
            case TypeDesc.BYTE_CODE:
            case TypeDesc.BOOLEAN_CODE:
                return 1;
            case TypeDesc.SHORT_CODE:
            case TypeDesc.CHAR_CODE:
                return 2;
            default:
            case TypeDesc.INT_CODE:
            case TypeDesc.FLOAT_CODE:
                return 4;
            case TypeDesc.LONG_CODE:
            case TypeDesc.DOUBLE_CODE:
                return 8;
            }
        } else if (type.toPrimitiveType() != null) {
            // Type is a primitive wrapper.

            int adjust;
            TypeDesc retType;

            switch (type.toPrimitiveType().getTypeCode()) {
            case TypeDesc.BOOLEAN_CODE:
                adjust = 1;
                retType = null;
                break;
            case TypeDesc.FLOAT_CODE:
                adjust = 4;
                retType = null;
                break;
            case TypeDesc.DOUBLE_CODE:
                adjust = 8;
                retType = null;
                break;
            default:
                adjust = 0;
                retType = TypeDesc.INT;
            }

            if (mode == Mode.KEY && descending) {
                a.invokeStatic(KeyEncoder.class.getName(), "encodeDesc", retType, params);
            } else {
                a.invokeStatic(DataEncoder.class.getName(), "encode", retType, params);
            }

            return adjust;
        } else {
            // Type is a String, byte array, BigInteger or BigDecimal.
            if (mode == Mode.KEY) {
                if (descending) {
                    a.invokeStatic
                        (KeyEncoder.class.getName(), "encodeDesc", TypeDesc.INT, params);
                } else {
                    a.invokeStatic(KeyEncoder.class.getName(), "encode", TypeDesc.INT, params);
                }
            } else {
                a.invokeStatic(DataEncoder.class.getName(), "encode", TypeDesc.INT, params);
            }
            return 0;
        }
    }

    /**
     * Generates code that stores a one or four byte generation value into a
     * byte array referenced by the local variable.
     *
     * @param encodedVar references a byte array
     * @param offset offset into byte array
     * @param generation if less than zero, no code is generated
     */
    private void encodeGeneration(CodeAssembler a, LocalVariable encodedVar,
                                  int offset, int generation)
    {
        if (offset < 0) {
            throw new IllegalArgumentException();
        }
        if (generation < 0) {
            return;
        }
        if (generation < 128) {
            a.loadLocal(encodedVar);
            a.loadConstant(offset);
            a.loadConstant((byte) generation);
            a.storeToArray(TypeDesc.BYTE);
        } else {
            generation |= 0x80000000;
            for (int i=0; i<4; i++) {
                a.loadLocal(encodedVar);
                a.loadConstant(offset + i);
                a.loadConstant((byte) (generation >> (8 * (3 - i))));
                a.storeToArray(TypeDesc.BYTE);
            }
        }
    }

    /**
     * Generates code that encodes property states using
     * ((properties.length + 3) / 4) bytes.
     *
     * @param encodedVar references a byte array
     * @param offset offset into byte array
     */
    private void encodePropertyStates(CodeAssembler a, LocalVariable encodedVar,
                                      int offset, StorableProperty<S>[] properties)
    {
        LocalVariable stateFieldVar = a.createLocalVariable(null, TypeDesc.INT);
        int lastFieldOrdinal = -1;

        LocalVariable accumVar = a.createLocalVariable(null, TypeDesc.INT);
        int accumShift = 0;

        for (int i=0; i<properties.length; i++) {
            StorableProperty<S> property = properties[i];

            int fieldOrdinal = property.getNumber() >> 4;

            if (fieldOrdinal == lastFieldOrdinal) {
                a.loadLocal(stateFieldVar);
            } else {
                a.loadThis();
                a.loadField(PROPERTY_STATE_FIELD_NAME + fieldOrdinal, TypeDesc.INT);
                a.storeLocal(stateFieldVar);
                a.loadLocal(stateFieldVar);
                lastFieldOrdinal = fieldOrdinal;
            }

            int stateShift = (property.getNumber() & 0xf) * 2;

            int accumPack = 2;
            int mask = PROPERTY_STATE_MASK << stateShift;

            // Try to pack more state properties into one operation.
            while ((accumShift + accumPack) < 8) {
                if (i + 1 >= properties.length) {
                    // No more properties to encode.
                    break;
                }
                StorableProperty<S> nextProperty = properties[i + 1];
                if (property.getNumber() + 1 != nextProperty.getNumber()) {
                    // Properties are not consecutive.
                    break;
                }
                if (fieldOrdinal != (nextProperty.getNumber() >> 4)) {
                    // Property states are stored in different fields.
                    break;
                }
                accumPack += 2;
                mask |= PROPERTY_STATE_MASK << ((nextProperty.getNumber() & 0xf) * 2);
                property = nextProperty;
                i++;
            }

            a.loadConstant(mask);
            a.math(Opcode.IAND);

            if (stateShift < accumShift) {
                a.loadConstant(accumShift - stateShift);
                a.math(Opcode.ISHL);
            } else if (stateShift > accumShift) {
                a.loadConstant(stateShift - accumShift);
                a.math(Opcode.IUSHR);
            }

            if (accumShift != 0) {
                a.loadLocal(accumVar);
                a.math(Opcode.IOR);
            }

            a.storeLocal(accumVar);

            if ((accumShift += accumPack) >= 8) {
                // Accumulator is full, so copy it to byte array.
                a.loadLocal(encodedVar);
                a.loadConstant(offset++);
                a.loadLocal(accumVar);
                a.storeToArray(TypeDesc.BYTE);
                accumShift = 0;
            }
        }

        if (accumShift > 0) {
            // Copy remaining states.
            a.loadLocal(encodedVar);
            a.loadConstant(offset++);
            a.loadLocal(accumVar);
            a.storeToArray(TypeDesc.BYTE);
        }
    }

    /**
     * Generates code to push RawSupport instance to the stack.  RawSupport is
     * available only in Storable instances. If instanceVar is an Object[], a
     * SupportException is thrown.
     *
     * @param instanceVar Storable instance or array of property values. Null
     * is storable instance of "this".
     */
    protected void pushRawSupport(CodeAssembler a, LocalVariable instanceVar)
        throws SupportException
    {
        boolean isObjectArrayInstanceVar = instanceVar != null
            && instanceVar.getType() == TypeDesc.forClass(Object[].class);

        if (isObjectArrayInstanceVar) {
            throw new SupportException("Lob properties not supported");
        }

        if (instanceVar == null) {
            a.loadThis();
        } else {
            a.loadLocal(instanceVar);
        }

        a.loadField(SUPPORT_FIELD_NAME, TypeDesc.forClass(TriggerSupport.class));
        a.checkCast(TypeDesc.forClass(RawSupport.class));
    }

    /**
     * Generates code to get a Lob locator value from RawSupport. RawSupport
     * instance and Lob instance must be on the stack. Result is a long locator
     * value on the stack.
     */
    private void getLobLocator(CodeAssembler a, StorablePropertyInfo info) {
        if (!info.isLob()) {
            throw new IllegalArgumentException();
        }
        a.invokeInterface(TypeDesc.forClass(RawSupport.class), "getLocator",
                          TypeDesc.LONG, new TypeDesc[] {info.getStorageType()});
    }

    /**
     * Generates code to get a Lob from a locator from RawSupport. RawSupport
     * instance, Storable instance, property name and long locator must be on
     * the stack. Result is a Lob on the stack, which may be null.
     */
    private void getLobFromLocator(CodeAssembler a, StorablePropertyInfo info) {
        if (!info.isLob()) {
            throw new IllegalArgumentException();
        }

        TypeDesc type = info.getStorageType();
        String name;
        if (Blob.class.isAssignableFrom(type.toClass())) {
            name = "getBlob";
        } else if (Clob.class.isAssignableFrom(type.toClass())) {
            name = "getClob";
        } else {
            throw new IllegalArgumentException();
        }

        a.invokeInterface(TypeDesc.forClass(RawSupport.class), name,
                          type, new TypeDesc[] {TypeDesc.forClass(Storable.class),
                                                TypeDesc.STRING, TypeDesc.LONG});
    }

    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////

    private void buildDecoding(Mode mode,
                               CodeAssembler a,
                               StorableProperty<S>[] properties,
                               Direction[] directions,
                               LocalVariable instanceVar,
                               Class<?> adapterInstanceClass,
                               boolean useWriteMethods,
                               int generation,
                               Label altGenerationHandler,
                               LocalVariable encodedVar)
        throws SupportException
    {
        if (a == null) {
            throw new IllegalArgumentException();
        }
        if (encodedVar == null || encodedVar.getType() != TypeDesc.forClass(byte[].class)) {
            throw new IllegalArgumentException();
        }

        // Decoding order is:
        //
        // 1. Prefix
        // 2. Generation prefix
        // 3. Property states (if Mode.SERIAL)
        // 4. Properties
        // 5. Suffix

        final int prefix;
        switch (mode) {
        default: 
            prefix = 0;
            break;
        case KEY:
            prefix = mKeyPrefixPadding;
            break;
        case DATA:
            prefix = mDataPrefixPadding;
            break;
        }

        decodeGeneration(a, encodedVar, prefix, generation, altGenerationHandler);

        final int generationPrefix;
        if (generation < 0) {
            generationPrefix = 0;
        } else if (generation < 128) {
            generationPrefix = 1;
        } else {
            generationPrefix = 4;
        }

        final int suffix;
        switch (mode) {
        default: 
            suffix = 0;
            break;
        case KEY:
            suffix = mKeySuffixPadding;
            break;
        case DATA:
            suffix = mDataSuffixPadding;
            extraDataDecoding(a, encodedVar, prefix + generationPrefix, suffix);
            break;
        }

        final TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);

        StorablePropertyInfo[] infos = checkSupport(properties);

        if (properties.length == 1) {
            StorableProperty<S> property = properties[0];
            StorablePropertyInfo info = infos[0];

            if (mode != Mode.SERIAL && info.getStorageType().toClass() == byte[].class) {
                // Since there is only one property, and it is just a byte
                // array, it doesn't have any fancy encoding.

                // Push to stack in preparation for storing a property.
                pushDecodingInstanceVar(a, 0, instanceVar);

                a.loadLocal(encodedVar);

                boolean descending = mode == Mode.KEY
                    && directions != null && directions[0] == Direction.DESCENDING;

                TypeDesc[] params;
                if (prefix > 0 || generationPrefix > 0 || suffix > 0) {
                    a.loadConstant(prefix + generationPrefix);
                    a.loadConstant(suffix);
                    params = new TypeDesc[] {byteArrayType, TypeDesc.INT, TypeDesc.INT};
                } else {
                    params = new TypeDesc[] {byteArrayType};
                }

                if (property.isNullable()) {
                    if (descending) {
                        a.invokeStatic(KeyDecoder.class.getName(), "decodeSingleNullableDesc",
                                       byteArrayType, params);
                    } else {
                        a.invokeStatic(DataDecoder.class.getName(), "decodeSingleNullable",
                                       byteArrayType, params);
                    }
                } else if (descending) {
                    a.invokeStatic(KeyDecoder.class.getName(), "decodeSingleDesc",
                                   byteArrayType, params);
                } else if (prefix > 0 || generationPrefix > 0 || suffix > 0) {
                    a.invokeStatic(DataDecoder.class.getName(), "decodeSingle",
                                   byteArrayType, params);
                } else {
                    // Always clone the byte array as some implementations
                    // reuse the byte array (e.g. iterating using a cursor).
                    a.invokeVirtual(TypeDesc.OBJECT, "clone", TypeDesc.OBJECT, null);
                    a.checkCast(byteArrayType);
                }

                storePropertyValue(a, info, useWriteMethods, instanceVar, adapterInstanceClass);
                return;
            }
        }

        // Now decode properties from the byte array.

        int constantOffset = prefix + generationPrefix;
        LocalVariable offsetVar = null;

        // References to local variables which will hold references.
        LocalVariable[] stringRefRef = new LocalVariable[1];
        LocalVariable[] byteArrayRefRef = new LocalVariable[1];
        LocalVariable[] bigIntegerRefRef = new LocalVariable[1];
        LocalVariable[] bigDecimalRefRef = new LocalVariable[1];
        LocalVariable[] valueRefRef = new LocalVariable[1];

        // Used by SERIAL mode.
        List<LocalVariable> stateVars = null;

        if (mode == Mode.SERIAL) {
            stateVars = decodePropertyStates(a, encodedVar, constantOffset, properties);
            constantOffset += (properties.length + 3) / 4;

            // Some properties are skipped at runtime, so offset variable is required.
            offsetVar = a.createLocalVariable(null, TypeDesc.INT);
            a.loadConstant(constantOffset);
            a.storeLocal(offsetVar);

            // References need to be initialized early because some properties
            // are skipped at runtime.

            for (int i=0; i<properties.length; i++) {
                TypeDesc storageType = infos[i].getStorageType();

                if (storageType == TypeDesc.STRING) {
                    if (stringRefRef[0] == null) {
                        TypeDesc refType = TypeDesc.forClass(String[].class);
                        stringRefRef[0] = a.createLocalVariable(null, refType);
                        a.loadConstant(1);
                        a.newObject(refType);
                        a.storeLocal(stringRefRef[0]);
                    }
                } else if (storageType.toClass() == byte[].class) {
                    if (byteArrayRefRef[0] == null) {
                        TypeDesc refType = TypeDesc.forClass(byte[][].class);
                        byteArrayRefRef[0] = a.createLocalVariable(null, refType);
                        a.loadConstant(1);
                        a.newObject(refType);
                        a.storeLocal(byteArrayRefRef[0]);
                    }
                } else if (storageType.toClass() == BigInteger.class) {
                    if (bigIntegerRefRef[0] == null) {
                        TypeDesc refType = TypeDesc.forClass(BigInteger[].class);
                        bigIntegerRefRef[0] = a.createLocalVariable(null, refType);
                        a.loadConstant(1);
                        a.newObject(refType);
                        a.storeLocal(bigIntegerRefRef[0]);
                    }
                } else if (storageType.toClass() == BigDecimal.class) {
                    if (bigDecimalRefRef[0] == null) {
                        TypeDesc refType = TypeDesc.forClass(BigDecimal[].class);
                        bigDecimalRefRef[0] = a.createLocalVariable(null, refType);
                        a.loadConstant(1);
                        a.newObject(refType);
                        a.storeLocal(bigDecimalRefRef[0]);
                    }
                }
            }
        }

        for (int i=0; i<properties.length; i++) {
            StorableProperty<S> property = properties[i];
            StorablePropertyInfo info = infos[i];

            Label storePropertyLocation = a.createLabel();
            Label nextPropertyLocation = a.createLabel();

            // Push to stack in preparation for storing a property.
            pushDecodingInstanceVar(a, i, instanceVar);

            if (mode == Mode.SERIAL) {
                // Load property if initialized, else reset it.

                a.loadLocal(stateVars.get(property.getNumber() >> 4));
                a.loadConstant(PROPERTY_STATE_MASK << ((property.getNumber() & 0xf) * 2));
                a.math(Opcode.IAND);
                Label isInitialized = a.createLabel();
                a.ifZeroComparisonBranch(isInitialized, "!=");

                // Reset property value to zero, false, or null.
                loadBlankValue(a, TypeDesc.forClass(property.getType()));

                if (info.getToStorageAdapter() != null) {
                    // Bypass adapter.
                    a.storeField(info.getPropertyName(), info.getPropertyType());
                    a.branch(nextPropertyLocation);
                } else {
                    a.branch(storePropertyLocation);
                }

                isInitialized.setLocation();
            }

            TypeDesc storageType = info.getStorageType();

            if (info.isLob()) {
                // Need RawSupport instance for getting Lob from locator.
                pushRawSupport(a, instanceVar);

                // Also need to pass this stuff along when getting Lob.
                a.loadThis();
                a.loadConstant(info.getPropertyName());

                // Locator is encoded as a long.
                storageType = TypeDesc.LONG;
            }

            a.loadLocal(encodedVar);
            if (offsetVar == null) {
                a.loadConstant(constantOffset);
            } else {
                a.loadLocal(offsetVar);
            }

            boolean descending = mode == Mode.KEY
                && directions != null && directions[i] == Direction.DESCENDING;

            int amt = decodeProperty(a, info, storageType, mode, descending,
                                     // TODO: do something better for passing these refs
                                     stringRefRef, byteArrayRefRef,
                                     bigIntegerRefRef, bigDecimalRefRef,
                                     valueRefRef);

            if (info.isLob()) {
                getLobFromLocator(a, info);
            }

            if (amt != 0) {
                if (i + 1 < properties.length) {
                    // Only adjust offset if there are more properties.

                    if (amt > 0) {
                        if (offsetVar == null) {
                            constantOffset += amt;
                        } else {
                            a.loadConstant(amt);
                            a.loadLocal(offsetVar);
                            a.math(Opcode.IADD);
                            a.storeLocal(offsetVar);
                        }
                    } else {
                        // Offset adjust is one if returned object is null.
                        a.dup();
                        Label notNull = a.createLabel();
                        a.ifNullBranch(notNull, false);
                        a.loadConstant(1 + (offsetVar == null ? constantOffset : 0));
                        Label cont = a.createLabel();
                        a.branch(cont);
                        notNull.setLocation();
                        a.loadConstant(~amt + (offsetVar == null ? constantOffset : 0));
                        cont.setLocation();

                        if (offsetVar == null) {
                            offsetVar = a.createLocalVariable(null, TypeDesc.INT);
                        } else {
                            a.loadLocal(offsetVar);
                            a.math(Opcode.IADD);
                        }
                        a.storeLocal(offsetVar);
                    }
                }
            } else {
                if (i + 1 >= properties.length) {
                    // Don't need to keep track of offset anymore.
                    a.pop();
                } else {
                    // Only adjust offset if there are more properties.
                    if (offsetVar == null) {
                        if (constantOffset > 0) {
                            a.loadConstant(constantOffset);
                            a.math(Opcode.IADD);
                        }
                        offsetVar = a.createLocalVariable(null, TypeDesc.INT);
                    } else {
                        a.loadLocal(offsetVar);
                        a.math(Opcode.IADD);
                    }
                    a.storeLocal(offsetVar);
                }

                // Get the value out of the ref array so that it can be stored.
                a.loadLocal(valueRefRef[0]);
                a.loadConstant(0);
                a.loadFromArray(valueRefRef[0].getType());
            }

            storePropertyLocation.setLocation();

            storePropertyValue(a, info, useWriteMethods, instanceVar, adapterInstanceClass);

            nextPropertyLocation.setLocation();
        }

        if (stateVars != null) {
            for (int i=0; i<stateVars.size(); i++) {
                LocalVariable stateVar = stateVars.get(i);
                if (stateVar != null) {
                    a.loadThis();
                    a.loadLocal(stateVar);
                    a.storeField(PROPERTY_STATE_FIELD_NAME + i, TypeDesc.INT);
                }
            }
        }
    }

    /**
     * Generates code that calls a decoding method in DataDecoder or
     * KeyDecoder. Parameters must already be on the stack.
     *
     * @return 0 if an int amount is pushed onto the stack, or a positive value
     * if offset adjust amount is constant, or a negative value if offset
     * adjust is constant or one more
     */
    private int decodeProperty(CodeAssembler a,
                               GenericPropertyInfo info, TypeDesc storageType,
                               Mode mode, boolean descending,
                               LocalVariable[] stringRefRef, LocalVariable[] byteArrayRefRef,
                               LocalVariable[] bigIntegerRefRef, LocalVariable[] bigDecimalRefRef,
                               LocalVariable[] valueRefRef)
        throws SupportException
    {
        TypeDesc primType = storageType.toPrimitiveType();

        if (primType != null) {
            String methodName;
            TypeDesc returnType;
            int adjust;

            if (primType != storageType && info.isNullable()) {
                // Property type is a nullable boxed primitive.
                returnType = storageType;

                switch (primType.getTypeCode()) {
                case TypeDesc.BYTE_CODE:
                    methodName = "decodeByteObj";
                    adjust = ~2;
                    break;
                case TypeDesc.BOOLEAN_CODE:
                    methodName = "decodeBooleanObj";
                    adjust = 1;
                    break;
                case TypeDesc.SHORT_CODE:
                    methodName = "decodeShortObj";
                    adjust = ~3;
                    break;
                case TypeDesc.CHAR_CODE:
                    methodName = "decodeCharacterObj";
                    adjust = ~3;
                    break;
                default:
                case TypeDesc.INT_CODE:
                    methodName = "decodeIntegerObj";
                    adjust = ~5;
                    break;
                case TypeDesc.FLOAT_CODE:
                    methodName = "decodeFloatObj";
                    adjust = 4;
                    break;
                case TypeDesc.LONG_CODE:
                    methodName = "decodeLongObj";
                    adjust = ~9;
                    break;
                case TypeDesc.DOUBLE_CODE:
                    methodName = "decodeDoubleObj";
                    adjust = 8;
                    break;
                }
            } else {
                // Property type is a primitive or a boxed primitive.
                returnType = primType;

                switch (primType.getTypeCode()) {
                case TypeDesc.BYTE_CODE:
                    methodName = "decodeByte";
                    adjust = 1;
                    break;
                case TypeDesc.BOOLEAN_CODE:
                    methodName = "decodeBoolean";
                    adjust = 1;
                    break;
                case TypeDesc.SHORT_CODE:
                    methodName = "decodeShort";
                    adjust = 2;
                    break;
                case TypeDesc.CHAR_CODE:
                    methodName = "decodeChar";
                    adjust = 2;
                    break;
                default:
                case TypeDesc.INT_CODE:
                    methodName = "decodeInt";
                    adjust = 4;
                    break;
                case TypeDesc.FLOAT_CODE:
                    methodName = "decodeFloat";
                    adjust = 4;
                    break;
                case TypeDesc.LONG_CODE:
                    methodName = "decodeLong";
                    adjust = 8;
                    break;
                case TypeDesc.DOUBLE_CODE:
                    methodName = "decodeDouble";
                    adjust = 8;
                    break;
                }
            }

            TypeDesc[] params = {TypeDesc.forClass(byte[].class), TypeDesc.INT};
            if (mode == Mode.KEY && descending) {
                a.invokeStatic
                    (KeyDecoder.class.getName(), methodName + "Desc", returnType, params);
            } else {
                a.invokeStatic
                    (DataDecoder.class.getName(), methodName, returnType, params);
            }

            if (returnType.isPrimitive()) {
                if (!storageType.isPrimitive()) {
                    // Wrap it.
                    a.convert(returnType, storageType);
                }
            }

            return adjust;
        } else {
            String className = (mode == Mode.KEY ? KeyDecoder.class : DataDecoder.class).getName();
            String methodName;
            TypeDesc refType;

            if (storageType == TypeDesc.STRING) {
                methodName = (mode == Mode.KEY && descending)
                    ? "decodeStringDesc" : "decodeString";
                refType = TypeDesc.forClass(String[].class);
                if (stringRefRef[0] == null) {
                    stringRefRef[0] = a.createLocalVariable(null, refType);
                    a.loadConstant(1);
                    a.newObject(refType);
                    a.storeLocal(stringRefRef[0]);
                }
                a.loadLocal(stringRefRef[0]);
                valueRefRef[0] = stringRefRef[0];
            } else if (storageType.toClass() == byte[].class) {
                methodName = (mode == Mode.KEY && descending) ? "decodeDesc" : "decode";
                refType = TypeDesc.forClass(byte[][].class);
                if (byteArrayRefRef[0] == null) {
                    byteArrayRefRef[0] = a.createLocalVariable(null, refType);
                    a.loadConstant(1);
                    a.newObject(refType);
                    a.storeLocal(byteArrayRefRef[0]);
                }
                a.loadLocal(byteArrayRefRef[0]);
                valueRefRef[0] = byteArrayRefRef[0];
            } else if (storageType.toClass() == BigInteger.class) {
                methodName = (mode == Mode.KEY && descending) ? "decodeDesc" : "decode";
                refType = TypeDesc.forClass(BigInteger[].class);
                if (bigIntegerRefRef[0] == null) {
                    bigIntegerRefRef[0] = a.createLocalVariable(null, refType);
                    a.loadConstant(1);
                    a.newObject(refType);
                    a.storeLocal(bigIntegerRefRef[0]);
                }
                a.loadLocal(bigIntegerRefRef[0]);
                valueRefRef[0] = bigIntegerRefRef[0];
            } else if (storageType.toClass() == BigDecimal.class) {
                methodName = (mode == Mode.KEY && descending) ? "decodeDesc" : "decode";
                refType = TypeDesc.forClass(BigDecimal[].class);
                if (bigDecimalRefRef[0] == null) {
                    bigDecimalRefRef[0] = a.createLocalVariable(null, refType);
                    a.loadConstant(1);
                    a.newObject(refType);
                    a.storeLocal(bigDecimalRefRef[0]);
                }
                a.loadLocal(bigDecimalRefRef[0]);
                valueRefRef[0] = bigDecimalRefRef[0];
            } else {
                throw notSupported(info.getPropertyName(), storageType.getFullName());
            }

            TypeDesc[] params = {TypeDesc.forClass(byte[].class), TypeDesc.INT, refType};
            a.invokeStatic(className, methodName, TypeDesc.INT, params);

            return 0;
        }
    }

    /**
     * Push decoding instanceVar to stack in preparation to calling
     * storePropertyValue.
     *
     * @param ordinal zero-based property ordinal, used only if instanceVar
     * refers to an object array.
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are written to the runtime value of this array instead
     * of a Storable instance.
     * @see #storePropertyValue storePropertyValue
     */
    protected void pushDecodingInstanceVar(CodeAssembler a, int ordinal,
                                           LocalVariable instanceVar) {
        if (instanceVar == null) {
            // Push this to stack in preparation for storing a property.
            a.loadThis();
        } else if (instanceVar.getType() != TypeDesc.forClass(Object[].class)) {
            // Push reference to stack in preparation for storing a property.
            a.loadLocal(instanceVar);
        } else {
            // Push array and index to stack in preparation for storing a property.
            a.loadLocal(instanceVar);
            a.loadConstant(ordinal);
        }
    }

    /**
     * Generates code to store a property value into an instance which is
     * already on the operand stack. If instance is an Object array, index into
     * array must also be on the operand stack.
     *
     * @param info info for property to store to
     * @param useWriteMethod when true, set property by public write method
     * instead of protected field
     * @param instanceVar local variable referencing Storable instance,
     * defaults to "this" if null. If variable type is an Object array, then
     * property values are written to the runtime value of this array instead
     * of a Storable instance.
     * @param adapterInstanceClass class containing static references to
     * adapter instances - defaults to instanceVar
     * @see #pushDecodingInstanceVar pushDecodingInstanceVar
     */
    protected void storePropertyValue(CodeAssembler a, StorablePropertyInfo info,
                                      boolean useWriteMethod,
                                      LocalVariable instanceVar,
                                      Class<?> adapterInstanceClass) {
        if (info.isDerived()) {
            useWriteMethod = true;
        }

        TypeDesc type = info.getPropertyType();
        TypeDesc storageType = info.getStorageType();

        boolean isObjectArrayInstanceVar = instanceVar != null
            && instanceVar.getType() == TypeDesc.forClass(Object[].class);

        boolean useAdapterInstance = adapterInstanceClass != null
            && info.getToStorageAdapter() != null
            && (useWriteMethod || isObjectArrayInstanceVar);

        if (useAdapterInstance) {
            // Push adapter instance to adapt property value. It must be on the
            // stack before the property value, so swap.

            // Store unadapted property to temp var in order to be swapped.
            LocalVariable temp = a.createLocalVariable(null, storageType);
            a.storeLocal(temp);

            String fieldName = info.getPropertyName() + ADAPTER_FIELD_ELEMENT + 0;
            TypeDesc adapterType = TypeDesc.forClass
                (info.getToStorageAdapter().getDeclaringClass());
            a.loadStaticField
                (TypeDesc.forClass(adapterInstanceClass), fieldName, adapterType);

            a.loadLocal(temp);
            a.invoke(info.getFromStorageAdapter());

            // Stack now contains property adapted to its publicly declared type.
        }

        if (instanceVar == null) {
            if (useWriteMethod) {
                info.addInvokeWriteMethod(a);
            } else {
                // Set property value directly to protected field of instance.
                if (info.getToStorageAdapter() == null) {
                    a.storeField(info.getPropertyName(), type);
                } else {
                    // Invoke adapter method.
                    a.invokeVirtual(info.getWriteMethodName() + '$',
                                    null, new TypeDesc[] {storageType});
                }
            }
        } else if (!isObjectArrayInstanceVar) {
            TypeDesc instanceVarType = instanceVar.getType();

            // Drop properties that are missing or whose types are incompatible.
            doDrop: {
                Class instanceVarClass = instanceVarType.toClass();
                if (instanceVarClass != null) {
                    Class propertyClass;
                    {
                        Class inferredType = StorableIntrospector.inferType(instanceVarClass);
                        if (inferredType != null) {
                            StorableInfo propInfo = StorableIntrospector.examine(inferredType);
                            Map<String, StorableProperty> props = propInfo.getAllProperties();
                            StorableProperty prop = props.get(info.getPropertyName());
                            propertyClass = prop == null ? null : prop.getType();
                        } else {
                            Map<String, BeanProperty> props =
                                BeanIntrospector.getAllProperties(instanceVarClass);
                            BeanProperty prop = props.get(info.getPropertyName());
                            propertyClass = prop == null ? null : prop.getType();
                        }
                    }

                    if (propertyClass != null) {
                        if (propertyClass == type.toClass()) {
                            break doDrop;
                        }

                        // Types differ, but if primitive types, perform conversion.
                        TypeDesc primType = type.toPrimitiveType();
                        if (primType != null) {
                            TypeDesc propType = TypeDesc.forClass(propertyClass);
                            TypeDesc primPropType = propType.toPrimitiveType();
                            if (primPropType != null) {
                                // Apply conversion and store property.

                                Label convertLabel = a.createLabel();
                                Label convertedLabel = a.createLabel();

                                if (!type.isPrimitive() && propType.isPrimitive()) {
                                    // Might attempt to unbox null, which
                                    // throws NullPointerException. Instead,
                                    // convert null to zero or false.
                                    a.dup();
                                    a.ifNullBranch(convertLabel, false);
                                    a.pop(); // pop null off stack
                                    loadBlankValue(a, propType);
                                    a.branch(convertedLabel);
                                }

                                convertLabel.setLocation();
                                a.convert(type, propType);

                                convertedLabel.setLocation();

                                type = propType;
                                break doDrop;
                            }
                        }
                    }
                }

                // Drop missing or incompatible property.
                if (storageType.isDoubleWord()) {
                    a.pop2();
                } else {
                    a.pop();
                }
                return;
            }

            if (useWriteMethod) {
                info.addInvokeWriteMethod(a, instanceVarType);
            } else {
                // Set property value directly to protected field of referenced
                // instance. Assumes code is being defined in the same package
                // or a subclass.
                if (info.getToStorageAdapter() == null) {
                    a.storeField(instanceVarType, info.getPropertyName(), type);
                } else {
                    // Invoke adapter method.
                    a.invokeVirtual(instanceVarType, info.getWriteMethodName() + '$',
                                    null, new TypeDesc[] {storageType});
                }
            }
        } else {
            // Set property value to object array. No need to check if we
            // should call a write method because arrays don't have write
            // methods.
            if (type.isPrimitive()) {
                a.convert(type, type.toObjectType());
            }
            a.storeToArray(TypeDesc.OBJECT);
        }
    }

    /**
     * Generates code that ensures a matching generation value exists in the
     * byte array referenced by the local variable, throwing a
     * CorruptEncodingException otherwise.
     *
     * @param generation if less than zero, no code is generated
     */
    private void decodeGeneration(CodeAssembler a, LocalVariable encodedVar,
                                  int offset, int generation, Label altGenerationHandler)
    {
        if (offset < 0) {
            throw new IllegalArgumentException();
        }
        if (generation < 0) {
            return;
        }

        LocalVariable actualGeneration = a.createLocalVariable(null, TypeDesc.INT);
        a.loadLocal(encodedVar);
        a.loadConstant(offset);
        a.loadFromArray(TypeDesc.BYTE);
        a.storeLocal(actualGeneration);
        a.loadLocal(actualGeneration);
        Label compareGeneration = a.createLabel();
        a.ifZeroComparisonBranch(compareGeneration, ">=");

        // Decode four byte generation format.
        a.loadLocal(actualGeneration);
        a.loadConstant(24);
        a.math(Opcode.ISHL);
        a.loadConstant(0x7fffffff);
        a.math(Opcode.IAND);
        for (int i=1; i<4; i++) {
            a.loadLocal(encodedVar);
            a.loadConstant(offset + i);
            a.loadFromArray(TypeDesc.BYTE);
            a.loadConstant(0xff);
            a.math(Opcode.IAND);
            int shift = 8 * (3 - i);
            if (shift > 0) {
                a.loadConstant(shift);
                a.math(Opcode.ISHL);
            }
            a.math(Opcode.IOR);
        }
        a.storeLocal(actualGeneration);

        compareGeneration.setLocation();

        a.loadConstant(generation);
        a.loadLocal(actualGeneration);
        Label generationMatches = a.createLabel();
        a.ifComparisonBranch(generationMatches, "==");

        if (altGenerationHandler != null) {
            a.loadLocal(actualGeneration);
            a.branch(altGenerationHandler);
        } else {
            // Throw CorruptEncodingException.

            TypeDesc corruptEncodingEx = TypeDesc.forClass(CorruptEncodingException.class);
            a.newObject(corruptEncodingEx);
            a.dup();
            a.loadConstant(generation);    // expected generation
            a.loadLocal(actualGeneration); // actual generation
            a.invokeConstructor(corruptEncodingEx, new TypeDesc[] {TypeDesc.INT, TypeDesc.INT});
            a.throwObject();
        }

        generationMatches.setLocation();
    }

    /**
     * Generates code that decodes property states from
     * ((properties.length + 3) / 4) bytes.
     *
     * @param encodedVar references a byte array
     * @param offset offset into byte array
     * @return local variables with state values
     */
    private List<LocalVariable> decodePropertyStates(CodeAssembler a, LocalVariable encodedVar,
                                                     int offset, StorableProperty<S>[] properties)
    {
        Vector<LocalVariable> stateVars = new Vector<LocalVariable>();
        LocalVariable accumVar = a.createLocalVariable(null, TypeDesc.INT);
        int accumShift = 8;

        for (int i=0; i<properties.length; i++) {
            StorableProperty<S> property = properties[i];

            int stateVarOrdinal = property.getNumber() >> 4;
            stateVars.setSize(Math.max(stateVars.size(), stateVarOrdinal + 1));

            if (stateVars.get(stateVarOrdinal) == null) {
                stateVars.set(stateVarOrdinal, a.createLocalVariable(null, TypeDesc.INT));
                a.loadThis();
                a.loadField(PROPERTY_STATE_FIELD_NAME + stateVarOrdinal, TypeDesc.INT);
                a.storeLocal(stateVars.get(stateVarOrdinal));
            }

            if (accumShift >= 8) {
                // Load accumulator byte.
                a.loadLocal(encodedVar);
                a.loadConstant(offset++);
                a.loadFromArray(TypeDesc.BYTE);
                a.loadConstant(0xff);
                a.math(Opcode.IAND);
                a.storeLocal(accumVar);
                accumShift = 0;
            }

            int stateShift = (property.getNumber() & 0xf) * 2;

            int accumPack = 2;
            int mask = PROPERTY_STATE_MASK << stateShift;

            // Try to pack more state properties into one operation.
            while ((accumShift + accumPack) < 8) {
                if (i + 1 >= properties.length) {
                    // No more properties to encode.
                    break;
                }
                StorableProperty<S> nextProperty = properties[i + 1];
                if (property.getNumber() + 1 != nextProperty.getNumber()) {
                    // Properties are not consecutive.
                    break;
                }
                if (stateVarOrdinal != (nextProperty.getNumber() >> 4)) {
                    // Property states are stored in different fields.
                    break;
                }
                accumPack += 2;
                mask |= PROPERTY_STATE_MASK << ((nextProperty.getNumber() & 0xf) * 2);
                property = nextProperty;
                i++;
            }

            a.loadLocal(accumVar);

            if (stateShift < accumShift) {
                a.loadConstant(accumShift - stateShift);
                a.math(Opcode.IUSHR);
            } else if (stateShift > accumShift) {
                a.loadConstant(stateShift - accumShift);
                a.math(Opcode.ISHL);
            }

            a.loadConstant(mask);
            a.math(Opcode.IAND);
            a.loadLocal(stateVars.get(stateVarOrdinal));
            a.loadConstant(~mask);
            a.math(Opcode.IAND);
            a.math(Opcode.IOR);
            a.storeLocal(stateVars.get(stateVarOrdinal));

            accumShift += accumPack;
        }

        return stateVars;
    }
}
