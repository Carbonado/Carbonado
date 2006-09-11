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
public class TestUnionQueryExecutor extends TestQueryExecutor {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestUnionQueryExecutor.class);
    }

    public void testBasicUnion() throws Exception {
        QueryExecutor<Address> primary = new SortedQueryExecutor<Address>
            (null, createExecutor(1, 2, 3, 4, 5, 6, 7, 8), null, createOrdering("addressID"));

        Filter<Address> filter_1 = Filter.filterFor(Address.class, "addressCountry > ?");
        FilterValues<Address> values_1 = filter_1.initialFilterValues();
        QueryExecutor<Address> executor_1 = new FilteredQueryExecutor<Address>(primary, filter_1);

        Filter<Address> filter_2 = Filter.filterFor(Address.class, "addressState <= ?");
        FilterValues<Address> values_2 = filter_2.initialFilterValues();
        QueryExecutor<Address> executor_2 = new FilteredQueryExecutor<Address>(primary, filter_2);

        QueryExecutor<Address> union = new UnionQueryExecutor<Address>(executor_1, executor_2);

        Filter<Address> filter = Filter
            .filterFor(Address.class, "addressCountry > ? | addressState <= ?");
        FilterValues<Address> values = filter.initialFilterValues();
        filter = values.getFilter();

        assertEquals(filter, union.getFilter());

        values = values.with("country_6").with("state_3");

        assertEquals(5, union.count(values));

        assertEquals(primary.getOrdering(), union.getOrdering());

        compareElements(union.fetch(values), 1, 2, 3, 7, 8);
    }
}
