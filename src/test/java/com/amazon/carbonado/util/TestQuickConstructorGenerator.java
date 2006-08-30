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
public class TestQuickConstructorGenerator extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestQuickConstructorGenerator.class);
    }

    public TestQuickConstructorGenerator(String name) {
        super(name);
    }

    public void testStringMaker() throws Exception {
        StringMaker maker = QuickConstructorGenerator.getInstance(String.class, StringMaker.class);
        assertEquals("", maker.newEmptyString());
        assertEquals("hello", maker.newStringFromChars(new char[] {'h', 'e', 'l', 'l', 'o'}));
        assertEquals("hello",
                     maker.newStringFromBytes(new byte[] {'h', 'e', 'l', 'l', 'o'}, "US-ASCII"));
    }

    public void testIllegalArgs() {
        try {
            QuickConstructorGenerator.getInstance(String.class, String.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            QuickConstructorGenerator.getInstance(String.class, BadStringMaker.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            QuickConstructorGenerator.getInstance(String.class, BadStringMaker2.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            QuickConstructorGenerator.getInstance(String.class, BadStringMaker3.class);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            QuickConstructorGenerator.getInstance(byte[].class, StringMaker.class);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    public static interface StringMaker {
        Object newEmptyString();

        String newStringFromChars(char[] chars);

        String newStringFromBytes(byte[] bytes, String charsetName)
            throws java.io.UnsupportedEncodingException;
    }

    public static interface BadStringMaker {
        String newStringFromBytes(byte[] bytes, String charsetName);
    }

    public static interface BadStringMaker2 {
        String newStringFromClass(Class clazz);
    }

    public static interface BadStringMaker3 {
        Class newEmptyString();
    }
}
