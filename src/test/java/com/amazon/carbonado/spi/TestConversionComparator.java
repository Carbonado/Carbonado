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

package com.amazon.carbonado.spi;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.spi.ConversionComparator;

/**
 * Test cases for {@link ConversionComparator}.
 *
 * @author Brian S O'Neill
 */
public class TestConversionComparator extends TestCase {
    private static final int BOOLEAN_CODE = 0;
    private static final int BYTE_CODE = 1;
    private static final int SHORT_CODE = 2;
    private static final int CHAR_CODE = 3;
    private static final int INT_CODE = 4;
    private static final int FLOAT_CODE = 5;
    private static final int LONG_CODE = 6;
    private static final int DOUBLE_CODE = 7;

    private static final Class[] PRIMITIVE_CLASSES = {
        boolean.class, byte.class, short.class, char.class,
        int.class, float.class, long.class, double.class
    };

    private static final Class[] BOXED_PRIMITIVE_CLASSES = {
        Boolean.class, Byte.class, Short.class, Character.class,
        Integer.class, Float.class, Long.class, Double.class
    };

    // States which primitive conversions are allowed.
    private static final boolean[][] PRIMITIVE_MATRIX = {
        // from...
        // boolean  byte   short  char   int    float  long   double
        {  true,    false, false, false, false, false, false, false }, // to boolean
        {  false,   true,  false, false, false, false, false, false }, // to byte
        {  false,   true,  true,  false, false, false, false, false }, // to short
        {  false,   false, false, true,  false, false, false, false }, // to char
        {  false,   true,  true,  false, true,  false, false, false }, // to int
        {  false,   true,  true,  false, false, true,  false, false }, // to float
        {  false,   true,  true,  false, true,  false, true,  false }, // to long
        {  false,   true,  true,  false, true,  true,  false, true  }, // to double
    };

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestConversionComparator.class);
    }

    public TestConversionComparator(String name) {
        super(name);
    }

    public void test_isConversionPossible_basics() {
        ConversionComparator cc = new ConversionComparator(Object.class);
        assertEquals(true, cc.isConversionPossible(Object.class));
        assertEquals(false, cc.isConversionPossible(String.class));
        assertEquals(false, cc.isConversionPossible(boolean.class));
        assertEquals(false, cc.isConversionPossible(Integer.class));
        assertEquals(false, cc.isConversionPossible(int.class));

        cc = new ConversionComparator(String.class);
        assertEquals(true, cc.isConversionPossible(Object.class));
        assertEquals(true, cc.isConversionPossible(String.class));
        assertEquals(false, cc.isConversionPossible(boolean.class));
        assertEquals(false, cc.isConversionPossible(Integer.class));
        assertEquals(false, cc.isConversionPossible(int.class));

        cc = new ConversionComparator(boolean.class);
        assertEquals(true, cc.isConversionPossible(Object.class));
        assertEquals(false, cc.isConversionPossible(String.class));
        assertEquals(true, cc.isConversionPossible(boolean.class));
        assertEquals(false, cc.isConversionPossible(Integer.class));
        assertEquals(false, cc.isConversionPossible(int.class));

        cc = new ConversionComparator(Integer.class);
        assertEquals(true, cc.isConversionPossible(Object.class));
        assertEquals(false, cc.isConversionPossible(String.class));
        assertEquals(false, cc.isConversionPossible(boolean.class));
        assertEquals(true, cc.isConversionPossible(Integer.class));
        assertEquals(true, cc.isConversionPossible(int.class));

        cc = new ConversionComparator(int.class);
        assertEquals(true, cc.isConversionPossible(Object.class));
        assertEquals(false, cc.isConversionPossible(String.class));
        assertEquals(false, cc.isConversionPossible(boolean.class));
        assertEquals(true, cc.isConversionPossible(Integer.class));
        assertEquals(true, cc.isConversionPossible(int.class));
    }

    public void test_isConversionPossible_primitives() {
        test_isConversionPossible_primitives(false, false);
        test_isConversionPossible_primitives(false, true);
        test_isConversionPossible_primitives(true, false);
        test_isConversionPossible_primitives(true, true);
    }

    private void test_isConversionPossible_primitives(boolean fromBoxed, boolean toBoxed) {
        for (int fromCode = BOOLEAN_CODE; fromCode <= DOUBLE_CODE; fromCode++) {
            ConversionComparator cc = new ConversionComparator
                (fromBoxed ? BOXED_PRIMITIVE_CLASSES[fromCode] : PRIMITIVE_CLASSES[fromCode]);
            for (int toCode = BOOLEAN_CODE; toCode <= DOUBLE_CODE; toCode++) {
                boolean expected = PRIMITIVE_MATRIX[toCode][fromCode];
                Class toType = toBoxed ? BOXED_PRIMITIVE_CLASSES[toCode] : PRIMITIVE_CLASSES[toCode];
                assertEquals(expected, cc.isConversionPossible(toType));
            }
        }
    }

    public void test_compare() {
        ConversionComparator cc = new ConversionComparator(Object.class);
        assertEquals(true, cc.compare(Object.class, String.class) < 0);
        assertEquals(true, cc.compare(String.class, Object.class) > 0);
        assertEquals(0, cc.compare(Object.class, Object.class));
        assertEquals(0, cc.compare(String.class, String.class));
        assertEquals(0, cc.compare(String.class, String.class));
        assertEquals(0, cc.compare(int.class, Number.class));

        cc = new ConversionComparator(String.class);
        assertEquals(true, cc.compare(String.class, Object.class) < 0);
        assertEquals(true, cc.compare(Object.class, String.class) > 0);
        assertEquals(0, cc.compare(String.class, String.class));
        assertEquals(true, cc.compare(int.class, String.class) > 0);

        cc = new ConversionComparator(Integer.class);
        assertEquals(true, cc.compare(String.class, Object.class) > 0);
        assertEquals(true, cc.compare(Object.class, String.class) < 0);
        assertEquals(true, cc.compare(Object.class, Number.class) > 0);
        assertEquals(true, cc.compare(Integer.class, Number.class) < 0);
        assertEquals(true, cc.compare(int.class, Number.class) > 0);
        assertEquals(true, cc.compare(long.class, Number.class) > 0);
        assertEquals(true, cc.compare(long.class, Long.class) < 0);

        cc = new ConversionComparator(int.class);
        assertEquals(true, cc.compare(String.class, Object.class) > 0);
        assertEquals(true, cc.compare(Object.class, String.class) < 0);
        assertEquals(true, cc.compare(Object.class, Number.class) > 0);
        assertEquals(true, cc.compare(Integer.class, Number.class) < 0);
        assertEquals(true, cc.compare(int.class, Number.class) < 0);
        assertEquals(true, cc.compare(long.class, Number.class) < 0);
        assertEquals(true, cc.compare(long.class, Long.class) < 0);

        cc = new ConversionComparator(Byte.class);
        assertEquals(true, cc.compare(int.class, Number.class) > 0);
        assertEquals(true, cc.compare(long.class, Number.class) > 0);
        assertEquals(true, cc.compare(long.class, Integer.class) < 0);

        cc = new ConversionComparator(byte.class);
        assertEquals(true, cc.compare(short.class, int.class) < 0);
        assertEquals(true, cc.compare(long.class, int.class) > 0);

        cc = new ConversionComparator(java.util.Date.class);
        assertEquals(true, cc.compare(Object.class, Comparable.class) > 0);
        assertEquals(0, cc.compare(java.io.Serializable.class, Comparable.class));
    }
}
