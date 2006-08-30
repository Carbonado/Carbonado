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

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.PropertyFilter;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestPropertyFilterList extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestPropertyFilterList.class);
    }

    public TestPropertyFilterList(String name) {
        super(name);
    }

    public void testNull() throws Exception {
        assertEquals(0, PropertyFilterList.get(null).size());
    }

    public void testOpen() throws Exception {
        Filter<StorableTestBasic> filter = Filter.getOpenFilter(StorableTestBasic.class);
        assertEquals(0, PropertyFilterList.get(filter).size());
    }

    public void testClosed() throws Exception {
        Filter<StorableTestBasic> filter = Filter.getClosedFilter(StorableTestBasic.class);
        assertEquals(0, PropertyFilterList.get(filter).size());
    }

    public void testSingleton() throws Exception {
        Filter<StorableTestBasic> filter = Filter.filterFor(StorableTestBasic.class, "id = ?");

        List<PropertyFilter<StorableTestBasic>> list = PropertyFilterList.get(filter);

        assertEquals(1, list.size());
        assertEquals(filter, list.get(0));

        List<PropertyFilter<StorableTestBasic>> list2 = PropertyFilterList.get(filter);

        assertTrue(list == list2);
    }

    public void testMultiple() throws Exception {
        Filter<StorableTestBasic> filter =
            Filter.filterFor(StorableTestBasic.class, "id = ? & intProp > ?");

        List<PropertyFilter<StorableTestBasic>> list = PropertyFilterList.get(filter);

        assertEquals(2, list.size());

        Filter<StorableTestBasic> subFilter =
            Filter.filterFor(StorableTestBasic.class, "id = ?");

        assertEquals(subFilter, list.get(0));

        subFilter = Filter.filterFor(StorableTestBasic.class, "intProp > ?");

        assertEquals(subFilter, list.get(1));

        List<PropertyFilter<StorableTestBasic>> list2 = PropertyFilterList.get(filter);

        assertTrue(list == list2);
    }

    public void testIllegal() throws Exception {
        Filter<StorableTestBasic> filter =
            Filter.filterFor(StorableTestBasic.class, "id = ? | intProp > ?");

        try {
            List<PropertyFilter<StorableTestBasic>> list = PropertyFilterList.get(filter);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }
}
