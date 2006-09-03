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

import junit.framework.TestSuite;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.stored.Address;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestFilteredQueryExecutor extends TestQueryExecutor {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilteredQueryExecutor.class);
    }

    public void testBasicFiltering() throws Exception {
        QueryExecutor<Address> unfiltered = createExecutor(1, 2, 3, 4);
        Filter<Address> filter = Filter.filterFor(Address.class, "addressCountry > ?");
        FilterValues<Address> values = filter.initialFilterValues();

        QueryExecutor<Address> executor = new FilteredQueryExecutor<Address>(unfiltered, filter);

        assertEquals(values.getFilter(), executor.getFilter());

        assertEquals(2, executor.count(values.with("country_2")));

        assertEquals(0, executor.getOrdering().size());

        compareElements(executor.fetch(values.with("country_2")), 3, 4);
    }
}
