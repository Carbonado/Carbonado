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

package com.amazon.carbonado.util;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestBelatedCreator extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestBelatedCreator.class);
    }

    public TestBelatedCreator(String name) {
        super(name);
    }

    public void test_noProblems() {
        Creator c = new Creator(0, 0, false);
        TheObject obj = c.get(1000);
        assertTrue(obj.isReal());
        assertEquals("real", obj.toString());

        obj.doSomething();
        assertEquals(100, obj.getValue());
        assertEquals("12a", obj.doSomething(1, 2, "a"));

        assertEquals(0, c.mTimedOutCount);
        assertEquals(1, c.mCreatedCount);
    }

    public void test_immediateFailure() {
        Creator c = new Creator(60000, 0, true);
        TheObject obj = c.get(1000);
        assertFalse(obj.isReal());
        assertEquals("bogus", obj.toString());

        try {
            obj.doSomething();
            fail();
        } catch (RuntimeException e) {
        }
        assertEquals(-1, obj.getValue());
        assertNull(obj.doSomething(1, 2, "a"));

        assertEquals(0, c.mTimedOutCount);
        assertEquals(0, c.mCreatedCount);

        assertTrue(obj == c.get(1000));
        assertTrue(obj.equals(c.get(1000)));
    }

    public void test_longDelay() {
        Creator c = new Creator(60000, 5000, false);

        long start, end;
        TheObject obj;

        start = System.nanoTime();
        obj = c.get(1000);
        end = System.nanoTime();
        assertFalse(obj.isReal());
        assertEquals("bogus", obj.toString());
        assertTrue((end - start) >= 1000L * 1000000);

        start = System.nanoTime();
        obj = c.get(1000);
        end = System.nanoTime();
        assertFalse(obj.isReal());
        assertEquals("bogus", obj.toString());
        assertTrue((end - start) >= 1000L * 1000000);

        assertEquals(-1, obj.getValue());
        assertNull(obj.doSomething(1, 2, "a"));

        assertEquals(2, c.mTimedOutCount);
        assertEquals(0, c.mCreatedCount);

        start = System.nanoTime();
        TheObject obj2 = c.get(5000);
        end = System.nanoTime();
        assertTrue(obj2.isReal());
        assertEquals("real", obj.toString());
        assertTrue((end - start) <= 5000L * 1000000);

        assertFalse(obj == obj2);
        assertTrue(obj.isReal());
        assertEquals("real", obj.toString());

        assertEquals(100, obj.getValue());
        assertEquals("12a", obj.doSomething(1, 2, "a"));
        assertEquals(100, obj2.getValue());
        assertEquals("23b", obj2.doSomething(2, 3, "b"));

        assertEquals(2, c.mTimedOutCount);
        assertEquals(1, c.mCreatedCount);

        start = System.nanoTime();
        TheObject obj3 = c.get(1000);
        end = System.nanoTime();
        assertTrue(obj3.isReal());
        assertEquals("real", obj3.toString());
        assertTrue((end - start) <= 1000L * 1000000);

        assertTrue(obj2 == obj3);
        assertEquals(2, c.mTimedOutCount);
        assertEquals(1, c.mCreatedCount);
    }

    public void test_retry() throws Exception {
        Creator c = new Creator(5000, 0, true);
        TheObject obj = c.get(1000);
        assertFalse(obj.isReal());
        assertEquals("bogus", obj.toString());

        Thread.sleep(6000);

        assertTrue(obj.isReal());
        assertEquals("real", obj.toString());
        obj = c.get(1000);
        assertTrue(obj.isReal());
        assertEquals("real", obj.toString());
    }

    public interface TheObject {
        public boolean isReal();

        public void doSomething();

        public int getValue();

        public String doSomething(int a, long b, Object q);

        public String toString();
    }

    private class Creator extends BelatedCreator<TheObject, RuntimeException> {
        final int mCreateDelay;
        boolean mFailOnce;

        int mTimedOutCount;
        int mCreatedCount;

        Creator(int retryMillis, int createDelay, boolean failOnce) {
            super(TheObject.class, retryMillis);
            mCreateDelay = createDelay;
            mFailOnce = failOnce;
        }

        protected TheObject createReal() {
            assertTrue(mCreatedCount == 0);
            try {
                Thread.sleep(mCreateDelay);
            } catch (InterruptedException e) {
            }

            if (mFailOnce) {
                mFailOnce = false;
                return null;
            }

            return new TheObject() {
                public boolean isReal() {
                    return true;
                }

                public void doSomething() {
                }

                public int getValue() {
                    return 100;
                }

                public String doSomething(int a, long b, Object q) {
                    return "" + a + b + q;
                }

                public String toString() {
                    return "real";
                }
            };
        }

        protected TheObject createBogus() {
            return new TheObject() {
                public boolean isReal() {
                    return false;
                }

                public void doSomething() {
                    throw new RuntimeException();
                }

                public int getValue() {
                    return -1;
                }

                public String doSomething(int a, long b, Object q) {
                    return null;
                }

                public String toString() {
                    return "bogus";
                }
            };
        }

        protected void timedOutNotification(long timedOutMillis) {
            assertTrue(timedOutMillis >= 0);
            mTimedOutCount++;
        }

        @Override
        protected void createdNotification(TheObject object) {
            assertNotNull(object);
            mCreatedCount++;
        }
    }
}

