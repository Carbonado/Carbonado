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

import junit.framework.TestSuite;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;

import com.amazon.carbonado.stored.Address;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestSortedQueryExecutor extends TestQueryExecutor {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestSortedQueryExecutor.class);
    }

    public void testBasicSorting() throws Exception {
        QueryExecutor<Address> unsorted = createExecutor(4, 2, 3, 1);
        Filter<Address> filter = Filter.getOpenFilter(Address.class);
        FilterValues<Address> values = filter.initialFilterValues();
        OrderingList<Address> ordered = createOrdering("addressCountry");

        QueryExecutor<Address> executor =
            new SortedQueryExecutor<Address>(null, unsorted, null, ordered);

        assertEquals(filter, executor.getFilter());

        assertEquals(4, executor.count(values));

        assertEquals(ordered, executor.getOrdering());

        compareElements(executor.fetch(values), 1, 2, 3, 4);
    }

    public void testBasicFinisherSorting() throws Exception {
        QueryExecutor<Address> unsorted = createExecutor(1, 2, 3, 4);
        Filter<Address> filter = Filter.getOpenFilter(Address.class);
        FilterValues<Address> values = filter.initialFilterValues();
        OrderingList<Address> handled = createOrdering("addressCountry");
        OrderingList<Address> finisher = createOrdering("addressState");

        QueryExecutor<Address> executor =
            new SortedQueryExecutor<Address>(null, unsorted, handled, finisher);

        assertEquals(filter, executor.getFilter());

        assertEquals(4, executor.count(values));

        assertEquals(2, executor.getOrdering().size());
        assertEquals(handled.get(0), executor.getOrdering().get(0));
        assertEquals(finisher.get(0), executor.getOrdering().get(1));

        compareElements(executor.fetch(values), 1, 2, 3, 4);
    }
}
