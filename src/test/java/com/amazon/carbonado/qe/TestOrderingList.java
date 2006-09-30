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

    public void testImmutable() throws Exception {
        List<OrderedProperty<StorableTestBasic>> list =
            OrderingList.get(StorableTestBasic.class, "~date");
        try {
            list.set(0, list.get(0).reverse());
            fail();
        } catch (UnsupportedOperationException e) {
        }
    }

    public void testConcatList() throws Exception {
        OrderingList<StorableTestBasic> list_1 =
            OrderingList.get(StorableTestBasic.class, "date", "-intProp", "~stringProp");

        OrderingList<StorableTestBasic> list_2 =
            OrderingList.get(StorableTestBasic.class, "longProp", "doubleProp");

        OrderingList<StorableTestBasic> list_3 = list_1.concat(list_2);

        assertEquals(5, list_3.size());
        assertEquals("+date", list_3.get(0).toString());
        assertEquals("-intProp", list_3.get(1).toString());
        assertEquals("~stringProp", list_3.get(2).toString());
        assertEquals("+longProp", list_3.get(3).toString());
        assertEquals("+doubleProp", list_3.get(4).toString());

        OrderingList<StorableTestBasic> list_4 =
            OrderingList.get(StorableTestBasic.class,
                             "+date", "-intProp", "~stringProp", "longProp", "+doubleProp");

        assertEquals(list_3, list_4);
        assertTrue(list_3 == list_4);
    }

    public void testReverseDirections() throws Exception {
        OrderingList<StorableTestBasic> list_1 =
            OrderingList.get(StorableTestBasic.class, "date", "-intProp", "~stringProp");

        list_1 = list_1.reverseDirections();

        assertEquals(3, list_1.size());
        assertEquals("-date", list_1.get(0).toString());
        assertEquals("+intProp", list_1.get(1).toString());
        assertEquals("~stringProp", list_1.get(2).toString());

        OrderingList<StorableTestBasic> list_2 =
            OrderingList.get(StorableTestBasic.class, "-date", "intProp", "~stringProp");

        assertEquals(list_1, list_2);
        assertTrue(list_1 == list_2);
    }

    public void testReplace() throws Exception {
        OrderingList<StorableTestBasic> list_1 =
            OrderingList.get(StorableTestBasic.class, "date", "-intProp", "~stringProp");

        OrderedProperty<StorableTestBasic> op_0 = list_1.get(0);
        OrderedProperty<StorableTestBasic> op_1 = list_1.get(1);
        OrderedProperty<StorableTestBasic> op_2 = list_1.get(2);

        list_1 = list_1.replace(0, op_1);
        list_1 = list_1.replace(1, op_2);
        list_1 = list_1.replace(2, op_0);

        assertEquals(3, list_1.size());
        assertEquals("-intProp", list_1.get(0).toString());
        assertEquals("~stringProp", list_1.get(1).toString());
        assertEquals("+date", list_1.get(2).toString());

        OrderingList<StorableTestBasic> list_2 =
            OrderingList.get(StorableTestBasic.class, "-intProp", "~stringProp", "+date");

        assertEquals(list_1, list_2);
        assertTrue(list_1 == list_2);
    }

    public void testSubList() throws Exception {
        OrderingList<StorableTestBasic> list_1 =
            OrderingList.get(StorableTestBasic.class, "date", "-intProp", "~stringProp");

        assertEquals(0, list_1.subList(0, 0).size());
        assertEquals(list_1, list_1.subList(0, 3));

        OrderingList<StorableTestBasic> sub = list_1.subList(0, 1);
        assertEquals(1, sub.size());
        assertEquals("+date", sub.get(0).toString());

        sub = list_1.subList(1, 3);
        assertEquals(2, sub.size());
    }

    public void testAsArray() throws Exception {
        OrderingList<StorableTestBasic> list =
            OrderingList.get(StorableTestBasic.class, "date", "intProp", "stringProp");

        OrderedProperty<StorableTestBasic>[] array = list.asArray();

        assertEquals(3, array.length);
        assertEquals("+date", array[0].toString());
        assertEquals("+intProp", array[1].toString());
        assertEquals("+stringProp", array[2].toString());
    }

    public void testAsStringArray() throws Exception {
        OrderingList<StorableTestBasic> list =
            OrderingList.get(StorableTestBasic.class, "date", "intProp", "stringProp");

        String[] array = list.asStringArray();

        assertEquals(3, array.length);
        assertEquals("+date", array[0]);
        assertEquals("+intProp", array[1]);
        assertEquals("+stringProp", array[2]);
    }
}
