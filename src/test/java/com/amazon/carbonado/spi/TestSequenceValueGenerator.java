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

import java.util.Random;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.repo.toy.ToyRepository;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestSequenceValueGenerator extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestSequenceValueGenerator.class);
    }

    private Repository mRepository;

    public TestSequenceValueGenerator(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        mRepository = new ToyRepository();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void test_basics() throws Exception {
        SequenceValueGenerator generator = new SequenceValueGenerator(mRepository, "foo");

        for (int i=1; i<=950; i++) {
            assertEquals(i, generator.nextLongValue());
        }

        generator.reset(1);

        for (int i=1; i<=950; i++) {
            assertEquals(i, generator.nextIntValue());
        }

        generator.reset(1);

        for (int i=1; i<=950; i++) {
            assertEquals(String.valueOf(i), generator.nextDecimalValue());
        }

        // Make sure data is persisted

        generator = new SequenceValueGenerator(mRepository, "foo");

        assertTrue(generator.nextLongValue() > 950);

        // Make sure data is isolated

        generator = new SequenceValueGenerator(mRepository, "another");

        for (int i=1; i<=1050; i++) {
            assertEquals(i, generator.nextLongValue());
        }
        
        // Make sure reserved values can be returned

        generator.returnReservedValues();
        generator = new SequenceValueGenerator(mRepository, "another");

        assertEquals(1051, generator.nextLongValue());
    }

    public void test_highIncrement() throws Exception {
        SequenceValueGenerator generator =
            new SequenceValueGenerator(mRepository, "foo", 1, 125);

        for (int i=0; i<950; i++) {
            assertEquals(i * 125 + 1, generator.nextLongValue());
        }
    }

    public void test_highInitialAndHighIncrement() throws Exception {
        SequenceValueGenerator generator =
            new SequenceValueGenerator(mRepository, "foo", 0x500000000L, 125);

        for (int i=0; i<950; i++) {
            assertEquals(i * 125 + 0x500000000L, generator.nextLongValue());
        }

        try {
            // Doesn't fit in an int.
            generator.nextIntValue();
            fail();
        } catch (PersistException e) {
        }
    }

    public void test_lowReserve() throws Exception {
        SequenceValueGenerator generator =
            new SequenceValueGenerator(mRepository, "goo", 1, 1, 1);

        for (int i=1; i<=950; i++) {
            assertEquals(i, generator.nextLongValue());
        }
    }

    public void test_overflow() throws Exception {
        Storage<StoredSequence> storage = mRepository.storageFor(StoredSequence.class);
        StoredSequence seq = storage.prepare();
        seq.setName("overflow");
        seq.setInitialValue(1);
        seq.setNextValue(Long.MAX_VALUE - 50);
        seq.insert();

        SequenceValueGenerator generator = new SequenceValueGenerator(mRepository, "overflow");

        for (int i=-50; i<=-1; i++) {
            assertEquals(i, generator.nextLongValue());
        }

        // Although next value could be zero, overflow logic doesn't work this
        // way. Its not really worth the trouble to allow zero to be returned
        // before overflowing.

        try {
            // Overflow.
            generator.nextLongValue();
            fail();
        } catch (PersistException e) {
        }
    }

    public void test_largeNumericalValue() throws Exception {
        // Tests string conversion to ensure large unsigned values are properly
        // generated.

        SequenceValueGenerator generator =
            new SequenceValueGenerator(mRepository, "goo", Long.MAX_VALUE, 1);

        assertEquals("9223372036854775807", generator.nextDecimalValue());
        // Next values are too large to fit in an unsigned long
        assertEquals("9223372036854775808", generator.nextDecimalValue());
        assertEquals("9223372036854775809", generator.nextDecimalValue());
    }

    public void test_radix() throws Exception {
        SequenceValueGenerator generator = new SequenceValueGenerator(mRepository, "goo");

        for (int i=1; i<=1000; i++) {
            assertEquals(Integer.toString(i, 36), generator.nextNumericalValue(36, 1));
        }
    }

    public void test_pad() throws Exception {
        SequenceValueGenerator generator = new SequenceValueGenerator(mRepository, "goo");

        for (int i=1; i<=2000; i++) {
            String next = generator.nextNumericalValue(10, 3);
            assertTrue(next.length() >= 3);
            int value = Integer.parseInt(next);
            assertEquals(i, value);
        }
    }

    public void test_concurrentAccess() throws Exception {
        // Simple test ensuring that values are reserved properly even when
        // multiple processes may be sharing the sequence.

        SequenceValueGenerator g1 = new SequenceValueGenerator(mRepository, "goo", 1, 1, 100);
        SequenceValueGenerator g2 = new SequenceValueGenerator(mRepository, "goo", 1, 1, 100);

        for (int i=1; i<=100; i++) {
            assertEquals(i, g1.nextLongValue());
            assertEquals(i + 100, g2.nextLongValue());
        }

        for (int i=201; i<=300; i++) {
            assertEquals(i, g2.nextLongValue());
            assertEquals(i + 100, g1.nextLongValue());
        }

        assertTrue(g1.returnReservedValues());
        assertFalse(g2.returnReservedValues());
    }

    // FIXME: move this test somewhere else
    /* Takes too long
    public void test_heavyConcurrentAccess() throws Exception {
        // Heavy test with multiple processes sharing the sequence.

        final Storage<StorableTestBasic> storage =
            mRepository.storageFor(StorableTestBasic.class);
        final Random rnd = new Random(376296292);
        final int loopCount = 10000;

        Thread[] threads = new Thread[10];
        for (int i=0; i<threads.length; i++) {
            threads[i] = new Thread() {
                public void run() {
                    try {
                        SequenceValueGenerator generator =
                            new SequenceValueGenerator(mRepository, "seq");
                        for (int i=0; i<loopCount; i++) {
                            StorableTestBasic stb = storage.prepare();
                            stb.setId(generator.nextIntValue());
                            stb.initBasicProperties();
                            stb.insert();
                            if (rnd.nextInt(500) == 0) {
                                generator.returnReservedValues();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                        fail(e.toString());
                    }
                }
            };

            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }
    */
}
