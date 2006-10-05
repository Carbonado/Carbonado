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

import java.util.Random;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test case for {@link DataEncoder} and {@link DataDecoder}. 
 * <p>
 * It generates random data values, checks that the decoding produces the
 * original results, and it checks that the order of the encoded bytes matches
 * the order of the original data values.
 *
 * @author Brian S O'Neill
 */
public class TestDataEncoding extends TestCase {
    private static final int SHORT_TEST = 100;
    private static final int MEDIUM_TEST = 500;
    private static final int LONG_TEST = 1000;

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestDataEncoding.class);
    }

    /**
     * @return -1, 0, or 1
     */
    static int byteArrayCompare(byte[] aa, byte[] ab) {
        int len = Math.min(aa.length, ab.length);
        int result = byteArrayCompare(aa, ab, len);
        if (result == 0 && aa.length != ab.length) {
            if (aa.length == len) {
                return -1;
            }
            if (ab.length == len) {
                return 1;
            }
        }
        return result;
    }

    /**
     * @return -1, 0, or 1
     */
    static int byteArrayCompare(byte[] aa, byte[] ab, int len) {
        for (int i=0; i<len; i++) {
            int a = aa[i] & 0xff;
            int b = ab[i] & 0xff;
            if (a < b) {
                return -1;
            }
            if (a > b) {
                return 1;
            }
        }
        return 0;
    }

    /**
     * @return -1, 0, or 1
     */
    static int byteArrayOrNullCompare(byte[] aa, byte[] ab) {
        if (aa == null) {
            if (ab == null) {
                return 0;
            } else {
                return 1;
            }
        } else if (ab == null) {
            return -1;
        } else {
            return byteArrayCompare(aa, ab);
        }
    }

    /**
     * @return -1, 0, or 1
     */
    static int byteArrayOrNullCompare(byte[] aa, byte[] ab, int len) {
        if (aa == null) {
            if (ab == null) {
                return 0;
            } else {
                return 1;
            }
        } else if (ab == null) {
            return -1;
        } else {
            return byteArrayCompare(aa, ab, len);
        }
    }

    /**
     * @return -1, 0, or 1
     */
    static <C extends Comparable> int compare(C a, C b) {
        if (a == null) {
            if (b == null) {
                return 0;
            } else {
                return 1;
            }
        } else if (b == null) {
            return -1;
        } else {
            return Integer.signum(a.compareTo(b));
        }
    }

    private final long mSeed;

    private Random mRandom;

    public TestDataEncoding(String name) {
        super(name);
        mSeed = 5399777425345431L;
    }

    protected void setUp() {
        mRandom = new Random(mSeed);
    }

    protected void tearDown() {
    }

    public void test_boolean() throws Exception {
        byte[] bytes = new byte[1];
        boolean lastValue = false;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            boolean value = mRandom.nextBoolean();
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeBoolean(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Boolean() throws Exception {
        byte[] bytes = new byte[1];
        Boolean lastValue = false;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Boolean value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                value = mRandom.nextBoolean();
            }
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeBooleanObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_byte() throws Exception {
        byte[] bytes = new byte[1];
        byte lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            byte value = (byte) mRandom.nextInt();
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeByte(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Byte() throws Exception {
        byte[] bytes = new byte[2];
        Byte lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Byte value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, DataEncoder.encode(value, bytes, 0));
            } else {
                value = (byte) mRandom.nextInt();
                assertEquals(2, DataEncoder.encode(value, bytes, 0));
            }
            assertEquals(value, DataDecoder.decodeByteObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_short() throws Exception {
        byte[] bytes = new byte[2];
        short lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            short value = (short) mRandom.nextInt();
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeShort(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Short() throws Exception {
        byte[] bytes = new byte[3];
        Short lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Short value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, DataEncoder.encode(value, bytes, 0));
            } else {
                value = (short) mRandom.nextInt();
                assertEquals(3, DataEncoder.encode(value, bytes, 0));
            }
            assertEquals(value, DataDecoder.decodeShortObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_char() throws Exception {
        byte[] bytes = new byte[2];
        char lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            char value = (char) mRandom.nextInt();
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeChar(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Character() throws Exception {
        byte[] bytes = new byte[3];
        Character lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Character value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, DataEncoder.encode(value, bytes, 0));
            } else {
                value = (char) mRandom.nextInt();
                assertEquals(3, DataEncoder.encode(value, bytes, 0));
            }
            assertEquals(value, DataDecoder.decodeCharacterObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_int() throws Exception {
        byte[] bytes = new byte[4];
        int lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            int value = mRandom.nextInt();
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeInt(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Integer() throws Exception {
        byte[] bytes = new byte[5];
        Integer lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Integer value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, DataEncoder.encode(value, bytes, 0));
            } else {
                value = mRandom.nextInt();
                assertEquals(5, DataEncoder.encode(value, bytes, 0));
            }
            assertEquals(value, DataDecoder.decodeIntegerObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_long() throws Exception {
        byte[] bytes = new byte[8];
        long lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            long value = mRandom.nextLong();
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeLong(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Long() throws Exception {
        byte[] bytes = new byte[9];
        Long lastValue = 0L;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Long value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, DataEncoder.encode(value, bytes, 0));
            } else {
                value = mRandom.nextLong();
                assertEquals(9, DataEncoder.encode(value, bytes, 0));
            }
            assertEquals(value, DataDecoder.decodeLongObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_float() throws Exception {
        byte[] bytes = new byte[4];
        float lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            float value = Float.intBitsToFloat(mRandom.nextInt());
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeFloat(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Float() throws Exception {
        byte[] bytes = new byte[4];
        Float lastValue = 0f;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            Float value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                value = Float.intBitsToFloat(mRandom.nextInt());
            }
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeFloatObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_double() throws Exception {
        byte[] bytes = new byte[8];
        double lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            double value = Double.longBitsToDouble(mRandom.nextLong());
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeDouble(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_Double() throws Exception {
        byte[] bytes = new byte[8];
        Double lastValue = 0d;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            Double value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                value = Double.longBitsToDouble(mRandom.nextLong());
            }
            DataEncoder.encode(value, bytes, 0);
            assertEquals(value, DataDecoder.decodeDoubleObj(bytes, 0));
            if (lastBytes != null) {
                int sgn = compare(value, lastValue);
                assertEquals(sgn, byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_String() throws Exception {
        String[] ref = new String[1];
        for (int i=0; i<SHORT_TEST; i++) {
            String value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                int length;
                switch (mRandom.nextInt(15)) {
                default:
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    length = mRandom.nextInt(100);
                    break;
                case 8: case 9: case 10: case 11:
                    length = mRandom.nextInt(200);
                    break;
                case 12: case 13:
                    length = mRandom.nextInt(20000);
                    break;
                case 14:
                    length = mRandom.nextInt(3000000);
                    break;
                }
                char[] chars = new char[length];
                for (int j=0; j<length; j++) {
                    char c;
                    switch (mRandom.nextInt(7)) {
                    default:
                    case 0: case 1: case 2: case 3:
                        c = (char) mRandom.nextInt(128);
                        break;
                    case 4: case 5:
                        c = (char) mRandom.nextInt(4000);
                        break;
                    case 6:
                        c = (char) mRandom.nextInt();
                        break;
                    }
                    chars[j] = c;
                }
                value = new String(chars);
            }

            byte[] bytes = new byte[DataEncoder.calculateEncodedStringLength(value)];
            assertEquals(bytes.length, DataEncoder.encode(value, bytes, 0));
            assertEquals(bytes.length, DataDecoder.decodeString(bytes, 0, ref));
            assertEquals(value, ref[0]);
        }
    }
}
