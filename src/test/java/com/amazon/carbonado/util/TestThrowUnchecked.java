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
public class TestThrowUnchecked extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestThrowUnchecked.class);
    }

    public TestThrowUnchecked(String name) {
        super(name);
    }

    public void test() {
        ThrowUnchecked.fire(null);

        Exception e = new java.io.IOException();

        try {
            ThrowUnchecked.fire(e);
            fail();
        } catch (Exception e2) {
            assertEquals(e, e2);
        }
    }
}
