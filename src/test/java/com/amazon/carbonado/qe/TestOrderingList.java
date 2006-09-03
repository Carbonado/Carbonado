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

package com.amazon.carbonado.qe;

import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.info.OrderedProperty;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestOrderingList extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestOrderingList.class);
    }

    public TestOrderingList(String name) {
        super(name);
    }

    public void testEmpty() throws Exception {
        assertEquals(0, OrderingList.get(StorableTestBasic.class).size());
    }

    public void testSingle() throws Exception {
        List<OrderedProperty<StorableTestBasic>> list_1 =
            OrderingList.get(StorableTestBasic.class, "date");

        assertEquals(1, list_1.size());
        assertEquals("+date", list_1.get(0).toString());

        List<OrderedProperty<StorableTestBasic>> list_2 =
            OrderingList.get(StorableTestBasic.class, "+date");

        assertEquals(1, list_2.size());
        assertEquals("+date", list_2.get(0).toString());
        assertEquals(list_1, list_2);
        assertTrue(list_1 == list_2);

        List<OrderedProperty<StorableTestBasic>> list_3 =
            OrderingList.get(StorableTestBasic.class, "-date");

        assertEquals(1, list_3.size());
        assertEquals("-date", list_3.get(0).toString());
        assertFalse(list_2.equals(list_3));
        assertFalse(list_2 == list_3);
    }

    public void testDouble() throws Exception {
        List<OrderedProperty<StorableTestBasic>> list_1 =
            OrderingList.get(StorableTestBasic.class, "date", "intProp");

        assertEquals(2, list_1.size());
        assertEquals("+date", list_1.get(0).toString());
        assertEquals("+intProp", list_1.get(1).toString());

        List<OrderedProperty<StorableTestBasic>> list_2 =
            OrderingList.get(StorableTestBasic.class, "+date", "+intProp");

        assertEquals(2, list_2.size());
        assertEquals("+date", list_2.get(0).toString());
        assertEquals("+intProp", list_2.get(1).toString());
        assertEquals(list_1, list_2);
        assertTrue(list_1 == list_2);

        List<OrderedProperty<StorableTestBasic>> list_3 =
            OrderingList.get(StorableTestBasic.class, "-date", "-intProp");

        assertEquals(2, list_3.size());
        assertEquals("-date", list_3.get(0).toString());
        assertEquals("-intProp", list_3.get(1).toString());
        assertFalse(list_2.equals(list_3));
        assertFalse(list_2 == list_3);
    }

    public void testIllegal() throws Exception {
        try {
            OrderingList.get(StorableTestBasic.class, "foo");
            fail();
        } catch (IllegalArgumentException e) {
        }
    }
}
