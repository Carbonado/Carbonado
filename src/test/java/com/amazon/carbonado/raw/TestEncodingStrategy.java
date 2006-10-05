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

import java.lang.reflect.*;
import java.util.*;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.cojen.classfile.*;
import org.cojen.util.*;

import com.amazon.carbonado.*;
import com.amazon.carbonado.info.*;
import com.amazon.carbonado.spi.*;

/**
 * Test case for {@link GenericEncodingStrategy}.
 * <p>
 * It generates random selections of properties, encodes with random values,
 * and checks that the decoding produces the original results. In addition, the
 * proper ordering of encoded keys is checked.
 *
 * @author Brian S O'Neill
 */
public class TestEncodingStrategy extends TestCase {
    private static final int SHORT_TEST = 100;
    private static final int MEDIUM_TEST = 500;
    private static final int LONG_TEST = 1000;

    private static final int ENCODE_OBJECT_ARRAY = 0;
    private static final int DECODE_OBJECT_ARRAY = 1;
    private static final int ENCODE_OBJECT_ARRAY_PARTIAL = 2;

    private static final int BOGUS_GENERATION = 99;

    // Make sure BOGUS_GENERATION is not included.
    private static final int[] GENERATIONS = {-1, 0, 1, 2, 127, 128, 129, Integer.MAX_VALUE};

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestEncodingStrategy.class);
    }

    private final long mSeed;
    private final StorableProperty<TestStorable>[] mProperties;

    private Random mRandom;

    public TestEncodingStrategy(String name) {
        super(name);
        mSeed = 986184829029842L;
        Collection<? extends StorableProperty<TestStorable>> properties =
            StorableIntrospector.examine(TestStorable.class).getAllProperties().values();
        mProperties = properties.toArray(new StorableProperty[0]);
    }

    protected void setUp() {
        mRandom = new Random(mSeed);
    }

    protected void tearDown() {
    }

    public void test_dataEncoding_noProperties() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_noProperties(0, 0, generation);
        }
    }

    public void test_dataEncoding_noProperties_prefix() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_noProperties(5, 0, generation);
        }
    }

    public void test_dataEncoding_noProperties_suffix() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_noProperties(0, 7, generation);
        }
    }

    public void test_dataEncoding_noProperties_prefixAndSuffix() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_noProperties(5, 7, generation);
        }
    }

    private void test_dataEncoding_noProperties(int prefix, int suffix, int generation)
        throws Exception
    {
        GenericEncodingStrategy strategy = new GenericEncodingStrategy
            (TestStorable.class, null, 0, 0, prefix, suffix);

        Method[] methods = generateCodecMethods
            (strategy, new StorableProperty[0], null, generation);

        byte[] encoded = (byte[]) methods[ENCODE_OBJECT_ARRAY]
            .invoke(null, new Object[] {new Object[0]});

        int generationPrefix;
        if (generation < 0) {
            generationPrefix = 0;
        } else if (generation < 128) {
            generationPrefix = 1;
        } else {
            generationPrefix = 4;
        }

        assertEquals(encoded.length, prefix + generationPrefix + suffix);

        if (generation >= 0) {
            if (generationPrefix == 1) {
                assertEquals(generation, encoded[prefix]);
            } else {
                int actualGeneration = DataDecoder.decodeInt(encoded, prefix);
                assertEquals(generation, actualGeneration);
            }
        }

        // Decode should not throw an exception.
        methods[DECODE_OBJECT_ARRAY].invoke(null, new Object[0], encoded);

        // Generation mismatch should throw an exception.
        if (generation >= 0) {
            encoded[prefix] = BOGUS_GENERATION;
            try {
                methods[DECODE_OBJECT_ARRAY].invoke(null, new Object[0], encoded);
                fail();
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof CorruptEncodingException) {
                    CorruptEncodingException cee = (CorruptEncodingException) cause;
                    // Make sure error message includes actual generation.
                    assertTrue(cee.getMessage().indexOf(String.valueOf(BOGUS_GENERATION)) >= 0);
                } else {
                    throw e;
                }
            }
        }
    }

    public void test_dataEncoding_multipleProperties() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_multipleProperties(0, 0, generation);
        }
    }

    public void test_dataEncoding_multipleProperties_prefix() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_multipleProperties(5, 0, generation);
        }
    }

    public void test_dataEncoding_multipleProperties_suffix() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_multipleProperties(0, 7, generation);
        }
    }

    public void test_dataEncoding_multipleProperties_prefixAndSuffix() throws Exception {
        for (int generation : GENERATIONS) {
            test_dataEncoding_multipleProperties(5, 7, generation);
        }
    }

    /**
     * @param generation when non-negative, encode a storable layout generation
     * value in one or four bytes.
     */
    private void test_dataEncoding_multipleProperties(int prefix, int suffix, int generation)
        throws Exception
    {
        GenericEncodingStrategy strategy = new GenericEncodingStrategy
            (TestStorable.class, null, 0, 0, prefix, suffix);

        for (int i=0; i<SHORT_TEST; i++) {
            StorableProperty<TestStorable>[] properties = selectProperties(1, 50);

            Method[] methods = generateCodecMethods(strategy, properties, null, generation);
            Object[] values = selectPropertyValues(properties);

            byte[] encoded = (byte[]) methods[ENCODE_OBJECT_ARRAY]
                .invoke(null, new Object[] {values});

            int generationPrefix;
            if (generation < 0) {
                generationPrefix = 0;
            } else if (generation < 128) {
                generationPrefix = 1;
            } else {
                generationPrefix = 4;
            }

            assertTrue(encoded.length > (prefix + generationPrefix + suffix));

            if (generation >= 0) {
                if (generationPrefix == 1) {
                    assertEquals(generation, encoded[prefix]);
                } else {
                    int actualGeneration = DataDecoder.decodeInt(encoded, prefix);
                    assertEquals(generation, actualGeneration);
                }
            }

            if (prefix > 0) {
                // Fill in with data which should be ignored by decoder.
                for (int p=0; p<prefix; p++) {
                    encoded[p] = (byte) mRandom.nextInt();
                }
            }

            if (suffix > 0) {
                // Fill in with data which should be ignored by decoder.
                for (int p=0; p<suffix; p++) {
                    encoded[encoded.length - p - 1] = (byte) mRandom.nextInt();
                }
            }

            Object[] decodedValues = new Object[values.length];
            methods[DECODE_OBJECT_ARRAY].invoke(null, decodedValues, encoded);

            for (int j=0; j<properties.length; j++) {
                Object a = values[j];
                Object b = decodedValues[j];
                if (properties[j].getType() == byte[].class) {
                    assertTrue(0 == TestDataEncoding.byteArrayOrNullCompare
                               ((byte[]) a, (byte[]) b));
                } else {
                    assertEquals(a, b);
                }
            }

            // Generation mismatch should throw an exception.
            if (generation >= 0) {
                encoded[prefix] = BOGUS_GENERATION;
                try {
                    methods[DECODE_OBJECT_ARRAY].invoke(null, new Object[0], encoded);
                    fail();
                } catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof CorruptEncodingException) {
                        CorruptEncodingException cee = (CorruptEncodingException) cause;
                        // Make sure error message includes actual generation.
                        assertTrue
                            (cee.getMessage().indexOf(String.valueOf(BOGUS_GENERATION)) >= 0);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    public void test_keyEncoding_noProperties() throws Exception {
        test_keyEncoding_noProperties(0, 0);
    }

    public void test_keyEncoding_noProperties_prefix() throws Exception {
        test_keyEncoding_noProperties(5, 0);
    }

    public void test_keyEncoding_noProperties_suffix() throws Exception {
        test_keyEncoding_noProperties(0, 7);
    }

    public void test_keyEncoding_noProperties_prefixAndSuffix() throws Exception {
        test_keyEncoding_noProperties(5, 7);
    }

    private void test_keyEncoding_noProperties(int prefix, int suffix) throws Exception {
        GenericEncodingStrategy strategy = new GenericEncodingStrategy
            (TestStorable.class, null, prefix, suffix, 0, 0);

        Method[] methods = generateCodecMethods
            (strategy, new StorableProperty[0], new Direction[0], -1);

        // Encode should return an empty array.
        byte[] encoded = (byte[]) methods[ENCODE_OBJECT_ARRAY]
            .invoke(null, new Object[] {new Object[0]});
        assertEquals(encoded.length, prefix + suffix);

        // Decode should not throw an exception.
        methods[DECODE_OBJECT_ARRAY].invoke(null, new Object[0], encoded);
    }

    public void test_keyEncoding_multipleProperties() throws Exception {
        test_keyEncoding_multipleProperties(0, 0);
    }

    public void test_keyEncoding_multipleProperties_prefix() throws Exception {
        test_keyEncoding_multipleProperties(5, 0);
    }

    public void test_keyEncoding_multipleProperties_suffix() throws Exception {
        test_keyEncoding_multipleProperties(0, 7);
    }

    public void test_keyEncoding_multipleProperties_prefixAndSuffix() throws Exception {
        test_keyEncoding_multipleProperties(5, 7);
    }

    private void test_keyEncoding_multipleProperties(int prefix, int suffix) throws Exception {
        GenericEncodingStrategy strategy = new GenericEncodingStrategy
            (TestStorable.class, null, prefix, suffix, 0, 0);

        for (int i=0; i<MEDIUM_TEST; i++) {
            StorableProperty<TestStorable>[] properties = selectProperties(1, 50);
            Direction[] directions = selectDirections(properties.length);

            Method[] methods = generateCodecMethods(strategy, properties, directions, -1);
            Object[] values = selectPropertyValues(properties);

            byte[] encoded = (byte[]) methods[ENCODE_OBJECT_ARRAY]
                .invoke(null, new Object[] {values});

            assertTrue(encoded.length > (prefix + suffix));

            // Encode using partial encoding method, but do all
            // properties. Ensure that the encoding is exactly the same.
            byte[] encoded2 = (byte[]) methods[ENCODE_OBJECT_ARRAY_PARTIAL]
                .invoke(null, new Object[] {values, 0, properties.length});
            assertTrue(Arrays.equals(encoded, encoded2));

            if (prefix > 0) {
                // Fill in with data which should be ignored by decoder.
                for (int p=0; p<prefix; p++) {
                    encoded[p] = (byte) mRandom.nextInt();
                }
            }

            if (suffix > 0) {
                // Fill in with data which should be ignored by decoder.
                for (int p=0; p<suffix; p++) {
                    encoded[encoded.length - p - 1] = (byte) mRandom.nextInt();
                }
            }

            Object[] decodedValues = new Object[values.length];
            methods[DECODE_OBJECT_ARRAY].invoke(null, decodedValues, encoded);

            for (int j=0; j<properties.length; j++) {
                Object a = values[j];
                Object b = decodedValues[j];
                if (properties[j].getType() == byte[].class) {
                    assertTrue(0 == TestDataEncoding.byteArrayOrNullCompare
                               ((byte[]) a, (byte[]) b));
                } else {
                    assertEquals(a, b);
                }
            }

            // Now test partial encoding of keys.

            // Clear out random affixes, since we don't need specific values
            // anymore and it interferes with next test.
            if (prefix > 0) {
                for (int p=0; p<prefix; p++) {
                    encoded[p] = 0;
                }
            }
            if (suffix > 0) {
                for (int p=0; p<suffix; p++) {
                    encoded[encoded.length - p - 1] = 0;
                }
            }

            for (int j=0; j<SHORT_TEST; j++) {
                int start, end;
                if (properties.length == 1) {
                    start = 0;
                    end = 1;
                } else {
                    start = mRandom.nextInt(properties.length - 1);
                    // Partial encoding doesn't support zero properties, so
                    // ensure randomly selected stride is more than zero.
                    int stride;
                    do {
                        stride = mRandom.nextInt(properties.length - start);
                    } while (stride == 0);
                    end = start + stride + 1;
                }

                Object[] partialValues = new Object[end - start];
                System.arraycopy(values, start, partialValues, 0, partialValues.length);
                
                byte[] partial = (byte[]) methods[ENCODE_OBJECT_ARRAY_PARTIAL]
                    .invoke(null, new Object[] {partialValues, start, end});
                
                // Partial key must be substring of full key.
                int searchStart = start == 0 ? 0 : prefix;
                int index = indexOf(encoded, searchStart, encoded.length - searchStart,
                                    partial, 0, partial.length, 0);

                if (start == 0) {
                    assertEquals(0, index);
                } else {
                    assertTrue(index > 0);
                }

                if (properties.length == 1) {
                    break;
                }
            }
        }
    }

    public void test_keyEncoding_ordering() throws Exception {
        GenericEncodingStrategy strategy =
            new GenericEncodingStrategy(TestStorable.class, null, 0, 0, 0, 0);

        for (int i=0; i<MEDIUM_TEST; i++) {
            StorableProperty<TestStorable>[] properties = selectProperties(1, 50);
            Direction[] directions = selectDirections(properties.length);
            Method[] methods = generateCodecMethods(strategy, properties, directions, -1);

            Object[] values_1 = selectPropertyValues(properties);
            byte[] encoded_1 = (byte[]) methods[ENCODE_OBJECT_ARRAY]
                .invoke(null, new Object[] {values_1});

            Object[] values_2 = selectPropertyValues(properties);
            byte[] encoded_2 = (byte[]) methods[ENCODE_OBJECT_ARRAY]
                .invoke(null, new Object[] {values_2});

            int byteOrder = TestDataEncoding.byteArrayCompare(encoded_1, encoded_2);
            int valueOrder = compareValues(properties, directions, values_1, values_2);

            assertEquals(valueOrder, byteOrder);
        }
    }

    private int compareValues(StorableProperty<?>[] properties, Direction[] directions,
                              Object[] values_1, Object[] values_2) {
        int length = directions.length;
        for (int i=0; i<length; i++) {
            StorableProperty<?> property = properties[i];
            Direction direction = directions[i];

            Object value_1 = values_1[i];
            Object value_2 = values_2[i];

            int result;
            if (property.getType() == byte[].class) {
                result = TestDataEncoding.byteArrayOrNullCompare
                    ((byte[]) value_1, (byte[]) value_2);
            } else {
                if (value_1 == null) {
                    if (value_2 == null) {
                        result = 0;
                    } else {
                        result = 1;
                    }
                } else if (value_2 == null) {
                    result = -1;
                } else {
                    result = Integer.signum(((Comparable) value_1).compareTo(value_2));
                }
            }

            if (result != 0) {
                if (direction == Direction.DESCENDING) {
                    result = -result;
                }
                return result;
            }
        }

        return 0;
    }

    // Method taken from String class and modified a bit.
    private static int indexOf(byte[] source, int sourceOffset, int sourceCount,
                               byte[] target, int targetOffset, int targetCount,
                               int fromIndex) {
        if (fromIndex >= sourceCount) {
            return (targetCount == 0 ? sourceCount : -1);
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        if (targetCount == 0) {
            return fromIndex;
        }

        byte first  = target[targetOffset];
        int max = sourceOffset + (sourceCount - targetCount);
        
        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            // Look for first byte.
            if (source[i] != first) {
                while (++i <= max && source[i] != first);
            }
            
            // Found first byte, now look at the rest of v2
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && source[j] == target[k]; j++, k++);
                
                if (j == end) {
                    // Found whole string.
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

    /**
     * First method is the encoder, second is the decoder. Both methods are
     * static. Encoder accepts an object array of property values, and it
     * returns a byte array. Decoder accepts an object array to receive
     * property values, an encoded byte array, and it returns void.
     *
     * <p>If generating key encoding and the property count is more than zero,
     * then third element in array is a key encoder that supports partial
     * encoding. In addition to the object array argument, it also accepts an
     * int start and an int end argument. The start of the range is inclusive,
     * and the end is exclusive.
     *
     * @param directions when supplied, build key encoding/decoding. Otherwise,
     * build data encoding/decoding.
     * @param generation when non-negative, encode a storable layout generation
     * value in one or four bytes.
     */
    private Method[] generateCodecMethods(GenericEncodingStrategy strategy,
                                          StorableProperty<TestStorable>[] properties,
                                          Direction[] directions,
                                          int generation)
        throws Exception
    {
        ClassInjector ci = ClassInjector.create(TestStorable.class.getName(), null);
        ClassFile cf = new ClassFile(ci.getClassName());
        cf.markSynthetic();
        cf.setTarget("1.5");

        cf.addDefaultConstructor();

        TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
        TypeDesc objectArrayType = TypeDesc.forClass(Object[].class);

        // Build encode method.
        MethodInfo mi = cf.addMethod(Modifiers.PUBLIC_STATIC, "encode", byteArrayType,
                                     new TypeDesc[] {objectArrayType});
        CodeBuilder b = new CodeBuilder(mi);
        LocalVariable encodedVar;
        if (directions != null) {
            OrderedProperty<TestStorable>[] ordered =
                makeOrderedProperties(properties, directions);
            encodedVar =
                strategy.buildKeyEncoding(b, ordered, b.getParameter(0), null, false, null, null);
        } else {
            encodedVar = strategy.buildDataEncoding
                (b, properties, b.getParameter(0), null, false, generation);
        }
        b.loadLocal(encodedVar);
        b.returnValue(byteArrayType);

        // Build decode method.
        mi = cf.addMethod(Modifiers.PUBLIC_STATIC, "decode", null,
                          new TypeDesc[] {objectArrayType, byteArrayType});
        b = new CodeBuilder(mi);
        if (directions != null) {
            OrderedProperty<TestStorable>[] ordered =
                makeOrderedProperties(properties, directions);
            strategy.buildKeyDecoding
                (b, ordered, b.getParameter(0), null, false, b.getParameter(1));
        } else {
            strategy.buildDataDecoding
                (b, properties, b.getParameter(0), null, false,
                 generation, null, b.getParameter(1));
        }
        b.returnVoid();

        if (directions != null && properties.length > 0) {
            // Build encode partial key method.
            mi = cf.addMethod
                (Modifiers.PUBLIC_STATIC, "encodePartial", byteArrayType,
                 new TypeDesc[] {objectArrayType, TypeDesc.INT, TypeDesc.INT});
            b = new CodeBuilder(mi);
            OrderedProperty<TestStorable>[] ordered =
                makeOrderedProperties(properties, directions);
            encodedVar =
                strategy.buildKeyEncoding(b, ordered, b.getParameter(0), null, false,
                                          b.getParameter(1), b.getParameter(2));
            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        }

        Class<?> clazz = ci.defineClass(cf);

        Method encode = clazz.getMethod("encode", Object[].class);
        Method decode = clazz.getMethod("decode", Object[].class, byte[].class);
        Method encodePartial = null;
        if (directions != null && properties.length > 0) {
            encodePartial = clazz.getMethod("encodePartial", Object[].class, int.class, int.class);
        }

        Method[] methods = new Method[3];
        methods[ENCODE_OBJECT_ARRAY] = encode;
        methods[DECODE_OBJECT_ARRAY] = decode;
        methods[ENCODE_OBJECT_ARRAY_PARTIAL] = encodePartial;

        return methods;
    }

    private StorableProperty<TestStorable>[] selectProperties(int minCount, int maxCount) {
        int length = (minCount == maxCount) ? minCount
            : (mRandom.nextInt(maxCount - minCount + 1) + minCount);

        StorableProperty<TestStorable>[] selection = new StorableProperty[length];

        for (int i=length; --i>=0; ) {
            selection[i] = mProperties[mRandom.nextInt(mProperties.length)];
        }

        return selection;
    }

    private Direction[] selectDirections(int count) {
        Direction[] directions = new Direction[count];

        for (int i=count; --i>=0; ) {
            Direction dir;
            switch (mRandom.nextInt(3)) {
            default:
                dir = Direction.UNSPECIFIED;
                break;
            case 1:
                dir = Direction.ASCENDING;
                break;
            case 2:
                dir = Direction.DESCENDING;
                break;
            }
            directions[i] = dir;
        }

        return directions;
    }

    private OrderedProperty<TestStorable>[] makeOrderedProperties
        (StorableProperty<TestStorable>[] properties, Direction[] directions) {

        int length = properties.length;
        OrderedProperty<TestStorable>[] ordered = new OrderedProperty[length];
        for (int i=length; --i>=0; ) {
            ordered[i] = OrderedProperty.get(properties[i], directions[i]);
        }

        return ordered;
    }

    /**
     * Returns an array of the same size with randomly selected values filled
     * in that match the property type.
     */
    private Object[] selectPropertyValues(StorableProperty<?>[] properties) {
        int length = properties.length;
        Object[] values = new Object[length];

        for (int i=length; --i>=0; ) {
            StorableProperty<?> property = properties[i];
            TypeDesc type = TypeDesc.forClass(property.getType());

            Object value;

            if (property.isNullable() && mRandom.nextInt(100) == 0) {
                value = null;
            } else {
                TypeDesc prim = type.toPrimitiveType();
                if (prim != null) {
                    switch (prim.getTypeCode()) {
                    case TypeDesc.BOOLEAN_CODE: default:
                        value = mRandom.nextBoolean();
                        break;
                    case TypeDesc.CHAR_CODE:
                        value = (char) mRandom.nextInt();
                        break;
                    case TypeDesc.FLOAT_CODE:
                        value = Float.intBitsToFloat(mRandom.nextInt());
                        break;
                    case TypeDesc.DOUBLE_CODE:
                        value = Double.longBitsToDouble(mRandom.nextLong());
                        break;
                    case TypeDesc.BYTE_CODE:
                        value = (byte) mRandom.nextInt();
                        break;
                    case TypeDesc.SHORT_CODE:
                        value = (short) mRandom.nextInt();
                        break;
                    case TypeDesc.INT_CODE:
                        value = mRandom.nextInt();
                        break;
                    case TypeDesc.LONG_CODE:
                        value = mRandom.nextLong();
                        break;
                    }
                } else if (type == TypeDesc.STRING) {
                    int len = mRandom.nextInt(100);
                    StringBuilder sb = new StringBuilder(len);
                    for (int j=0; j<len; j++) {
                        sb.append((char) mRandom.nextInt());
                    }
                    value = sb.toString();
                } else {
                    int len = mRandom.nextInt(100);
                    byte[] bytes = new byte[len];
                    for (int j=0; j<len; j++) {
                        bytes[j] = (byte) mRandom.nextInt();
                    }
                    value = bytes;
                }
            }

            values[i] = value;
        }

        return values;
    }

    /**
     * Just a collection of storable properties. At least one property defined
     * per supported type.
     */
    @PrimaryKey("byte") // I don't really care what the primary key is.
    public static interface TestStorable extends Storable {
        byte getByte();

        void setByte(byte b);

        Byte getByteObj();

        void setByteObj(Byte b);

        @Nullable
        Byte getNullableByteObj();

        void setNullableByteObj(Byte b);

        short getShort();

        void setShort(short s);

        Short getShortObj();

        void setShortObj(Short s);

        @Nullable
        Short getNullableShortObj();

        void setNullableShortObj(Short s);

        char getChar();

        void setChar(char c);

        Character getCharacterObj();

        void setCharacterObj(Character c);

        @Nullable
        Character getNullableCharacterObj();

        void setNullableCharacterObj(Character c);

        int getInt();

        void setInt(int i);

        Integer getIntegerObj();

        void setIntegerObj(Integer obj);

        @Nullable
        Integer getNullableIntegerObj();

        void setNullableIntegerObj(Integer obj);

        long getLong();

        void setLong(long i);

        Long getLongObj();

        void setLongObj(Long i);

        @Nullable
        Long getNullableLongObj();

        void setNullableLongObj(Long i);

        float getFloat();

        void setFloat(float f);

        Float getFloatObj();

        void setFloatObj(Float f);

        @Nullable
        Float getNullableFloatObj();

        void setNullableFloatObj(Float f);

        double getDouble();

        void setDouble(double d);

        Double getDoubleObj();

        void setDoubleObj(Double d);

        @Nullable
        Double getNullableDoubleObj();

        void setNullableDoubleObj(Double d);

        byte[] getByteArray();

        void setByteArray(byte[] b);

        @Nullable
        byte[] getNullableByteArray();

        void setNullableByteArray(byte[] b);

        String getString();

        void setString(String s);

        @Nullable
        String getNullableString();

        void setNullableString(String s);
    }
}
