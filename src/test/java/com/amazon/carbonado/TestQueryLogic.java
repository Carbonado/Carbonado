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

package com.amazon.carbonado;

import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.repo.toy.ToyRepository;

import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.StorableTestBasicIndexed;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestQueryLogic extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestQueryLogic.class);
        return suite;
    }

    private Repository mRepository;

    protected void setUp() throws Exception {
        mRepository = new ToyRepository();
    }

    protected void tearDown() throws Exception {
        if (mRepository != null) {
            mRepository.close();
            mRepository = null;
        }
    }

    protected Repository getRepository() {
        return mRepository;
    }

    public TestQueryLogic(String name) {
        super(name);
    }

    public void test_not() throws Exception {
        final int count = 10;
        populate(count);

        final Storage<StorableTestBasic> storage =
            getRepository().storageFor(StorableTestBasic.class);

        StorableTestBasic stb;
        Query<StorableTestBasic> query;
        List<StorableTestBasic> results;

        {
            // Create query that returns nothing and verify.
            query = storage.query();
            assertEquals(Filter.getOpenFilter(StorableTestBasic.class), query.getFilter());
            query = query.not();
            assertEquals(Filter.getClosedFilter(StorableTestBasic.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(0, results.size());
            
            // Verify that "not" again produces everything.
            query = query.not();
            results = query.fetch().toList();
            assertEquals(count, results.size());
            assertOrder(results, true);
        }

        {
            // Create ordered query that returns nothing and verify.
            query = storage.query();
            query = query.orderBy("-intProp");
            assertEquals(Filter.getOpenFilter(StorableTestBasic.class), query.getFilter());
            query = query.not();
            assertEquals(Filter.getClosedFilter(StorableTestBasic.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(0, results.size());
            
            // Verify that order is preserved when "not" again.
            query = query.not();
            assertEquals(Filter.getOpenFilter(StorableTestBasic.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(count, results.size());
            assertOrder(results, false);
        }

        {
            // Do the ordered "not" test again, but change the sequence to build the query.
            query = storage.query().not(); // do "not" before orderBy.
            query = query.orderBy("-intProp");
            assertEquals(Filter.getClosedFilter(StorableTestBasic.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(0, results.size());
            
            // Verify that order is preserved when "not" again.
            query = query.not();
            assertEquals(Filter.getOpenFilter(StorableTestBasic.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(count, results.size());
            assertOrder(results, false);
        }
    }

    public void test_demorgans() throws Exception {
        final int count = 10;
        populate(count);

        final Storage<StorableTestBasic> storage =
            getRepository().storageFor(StorableTestBasic.class);

        Query<StorableTestBasic> query; 
        List<StorableTestBasic> results;

        {
            query = storage.query("intProp = ? & (stringProp >= ? | longProp < ?)");
            Filter<StorableTestBasic> filter1 = query.getFilter();
            query = query.not();
            Filter<StorableTestBasic> filter2 = query.getFilter();
            query = storage.query("intProp != ? | (stringProp < ? & longProp >= ?)");
            Filter<StorableTestBasic> filter3 = query.getFilter();
            
            assertEquals(filter3, filter2);
        }

        // Try again, making sure that values are preserved in the query.
        {
            query = storage.query("intProp = ? & (stringProp >= ? | longProp < ?)");
            query = query.with(5);

            results = query.with("a").with(10).fetch().toList();
            assertEquals(1, results.size());

            query = query.not();
            results = query.with("a").with(10).fetch().toList();
            assertEquals(count - 1, results.size());

            // Make sure record 5 is the one that's missing.
            for (StorableTestBasic stb : results) {
                assertFalse(5 == stb.getId());
            }

            // Should be back to the original query.
            query = query.not();

            results = query.with("a").with(10).fetch().toList();
            assertEquals(1, results.size());
        }

        // Try again, making sure that a full set of values is preserved in the query.
        {
            query = storage.query("intProp = ? & (stringProp >= ? | longProp < ?)");
            query = query.with(2).with("a").with(10);

            results = query.fetch().toList();
            assertEquals(1, results.size());

            query = query.not();
            results = query.fetch().toList();
            assertEquals(count - 1, results.size());

            // Make sure record 2 is the one that's missing.
            for (StorableTestBasic stb : results) {
                assertFalse(2 == stb.getId());
            }

            // Should be back to the original query.
            query = query.not();

            results = query.fetch().toList();
            assertEquals(1, results.size());
        }
    }

    public void test_and() throws Exception {
        final int count = 10;
        populate(count, new long[] {45, 12, 34, 12, 12, 45, 0, 0, 10, 2});

        final Storage<StorableTestBasic> storage =
            getRepository().storageFor(StorableTestBasic.class);

        Query<StorableTestBasic> query; 
        List<StorableTestBasic> results;

        {
            query = storage.query("longProp = ?");
            try {
                query = query.and("stringProp != ?");
                fail();
            } catch (IllegalStateException e) {
                // Good. Blank params exist.
            }
        }

        {
            query = storage.query("longProp = ?").with(12);
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(2, results.get(0).getId());
            assertEquals(5, results.get(1).getId());
        }

        // Different value.
        {
            query = storage.query("longProp = ?").with(45);
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(1, results.get(0).getId());
            assertEquals(6, results.get(1).getId());
        }

        // Different value with ordering.
        {
            query = storage.query("longProp = ?").with(45);
            query = query.orderBy("-intProp");
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(1, results.get(1).getId());
            assertEquals(6, results.get(0).getId());
        }

        // Try again with ordering.
        {
            query = storage.query("longProp = ?").with(12);
            query = query.orderBy("-intProp");
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(2, results.get(1).getId());
            assertEquals(5, results.get(0).getId());
        }

        // Invert the selection
        {
            query = storage.query("longProp = ?").with(12);
            query = query.and("stringProp != ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 2, results.size());
            assertEquals(1, results.get(0).getId());
            assertEquals(3, results.get(1).getId());
            assertEquals(4, results.get(2).getId());
            assertEquals(6, results.get(3).getId());
            assertEquals(7, results.get(4).getId());
            assertEquals(8, results.get(5).getId());
            assertEquals(9, results.get(6).getId());
            assertEquals(10, results.get(7).getId());
        }

        // Invert the ordered selection
        {
            query = storage.query("longProp = ?").with(12);
            query = query.orderBy("-intProp");
            query = query.and("stringProp != ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 2, results.size());
            assertEquals(1, results.get(7).getId());
            assertEquals(3, results.get(6).getId());
            assertEquals(4, results.get(5).getId());
            assertEquals(6, results.get(4).getId());
            assertEquals(7, results.get(3).getId());
            assertEquals(8, results.get(2).getId());
            assertEquals(9, results.get(1).getId());
            assertEquals(10, results.get(0).getId());
        }
    }

    public void test_or() throws Exception {
        final int count = 10;
        populate(count, new long[] {45, 12, 34, 12, 12, 45, 0, 0, 10, 2});

        final Storage<StorableTestBasic> storage =
            getRepository().storageFor(StorableTestBasic.class);

        Query<StorableTestBasic> query; 
        List<StorableTestBasic> results;

        {
            query = storage.query("longProp = ?");
            try {
                query = query.or("stringProp = ?");
                fail();
            } catch (IllegalStateException e) {
                // Good. Blank params exist.
            }
        }

        {
            query = storage.query("longProp = ?").with(45);
            query = query.or("stringProp = ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(3, results.size());
            assertEquals(1, results.get(0).getId());
            assertEquals(4, results.get(1).getId());
            assertEquals(6, results.get(2).getId());
        }

        // Different value.
        {
            query = storage.query("longProp = ?").with(12);
            query = query.or("stringProp = ?").with("str_3");

            results = query.fetch().toList();
            assertEquals(4, results.size());
            assertEquals(2, results.get(0).getId());
            assertEquals(3, results.get(1).getId());
            assertEquals(4, results.get(2).getId());
            assertEquals(5, results.get(3).getId());
        }

        // Different value with ordering.
        {
            query = storage.query("longProp = ?").with(12);
            query = query.orderBy("-intProp");
            query = query.or("stringProp = ?").with("str_3");

            results = query.fetch().toList();
            assertEquals(4, results.size());
            assertEquals(2, results.get(3).getId());
            assertEquals(3, results.get(2).getId());
            assertEquals(4, results.get(1).getId());
            assertEquals(5, results.get(0).getId());
        }

        // Try again with ordering.
        {
            query = storage.query("longProp = ?").with(45);
            query = query.orderBy("-intProp");
            query = query.or("stringProp = ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(3, results.size());
            assertEquals(1, results.get(2).getId());
            assertEquals(4, results.get(1).getId());
            assertEquals(6, results.get(0).getId());
        }

        // Invert the selection
        {
            query = storage.query("longProp = ?").with(45);
            query = query.or("stringProp = ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 3, results.size());
            assertEquals(2, results.get(0).getId());
            assertEquals(3, results.get(1).getId());
            assertEquals(5, results.get(2).getId());
            assertEquals(7, results.get(3).getId());
            assertEquals(8, results.get(4).getId());
            assertEquals(9, results.get(5).getId());
            assertEquals(10, results.get(6).getId());
        }

        // Invert the ordered selection
        {
            query = storage.query("longProp = ?").with(45);
            query = query.orderBy("-intProp");
            query = query.or("stringProp = ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 3, results.size());
            assertEquals(2, results.get(6).getId());
            assertEquals(3, results.get(5).getId());
            assertEquals(5, results.get(4).getId());
            assertEquals(7, results.get(3).getId());
            assertEquals(8, results.get(2).getId());
            assertEquals(9, results.get(1).getId());
            assertEquals(10, results.get(0).getId());
        }
    }

    //
    // Same tests again, except against an indexed storable.
    //

    public void test_indexed_not() throws Exception {
        final int count = 10;
        populateIndexed(count);

        final Storage<StorableTestBasicIndexed> storage =
            getRepository().storageFor(StorableTestBasicIndexed.class);

        StorableTestBasicIndexed stb;
        Query<StorableTestBasicIndexed> query;
        List<StorableTestBasicIndexed> results;

        {
            // Create query that returns nothing and verify.
            query = storage.query();
            assertEquals(Filter.getOpenFilter(StorableTestBasicIndexed.class), query.getFilter());
            query = query.not();
            assertEquals(Filter.getClosedFilter(StorableTestBasicIndexed.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(0, results.size());
            
            // Verify that "not" again produces everything.
            query = query.not();
            results = query.fetch().toList();
            assertEquals(count, results.size());
            assertOrderIndexed(results, true);
        }

        {
            // Create ordered query that returns nothing and verify.
            query = storage.query();
            query = query.orderBy("-intProp");
            assertEquals(Filter.getOpenFilter(StorableTestBasicIndexed.class), query.getFilter());
            query = query.not();
            assertEquals(Filter.getClosedFilter(StorableTestBasicIndexed.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(0, results.size());
            
            // Verify that order is preserved when "not" again.
            query = query.not();
            assertEquals(Filter.getOpenFilter(StorableTestBasicIndexed.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(count, results.size());
            assertOrderIndexed(results, false);
        }

        {
            // Do the ordered "not" test again, but change the sequence to build the query.
            query = storage.query().not(); // do "not" before orderBy.
            query = query.orderBy("-intProp");
            assertEquals(Filter.getClosedFilter(StorableTestBasicIndexed.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(0, results.size());
            
            // Verify that order is preserved when "not" again.
            query = query.not();
            assertEquals(Filter.getOpenFilter(StorableTestBasicIndexed.class), query.getFilter());
            results = query.fetch().toList();
            assertEquals(count, results.size());
            assertOrderIndexed(results, false);
        }
    }

    public void test_indexed_demorgans() throws Exception {
        final int count = 10;
        populateIndexed(count);

        final Storage<StorableTestBasicIndexed> storage =
            getRepository().storageFor(StorableTestBasicIndexed.class);

        Query<StorableTestBasicIndexed> query; 
        List<StorableTestBasicIndexed> results;

        {
            query = storage.query("intProp = ? & (stringProp >= ? | longProp < ?)");
            Filter<StorableTestBasicIndexed> filter1 = query.getFilter();
            query = query.not();
            Filter<StorableTestBasicIndexed> filter2 = query.getFilter();
            query = storage.query("intProp != ? | (stringProp < ? & longProp >= ?)");
            Filter<StorableTestBasicIndexed> filter3 = query.getFilter();
            
            assertEquals(filter3, filter2);
        }

        // Try again, making sure that values are preserved in the query.
        {
            query = storage.query("intProp = ? & (stringProp >= ? | longProp < ?)");
            query = query.with(5);

            results = query.with("a").with(10).fetch().toList();
            assertEquals(1, results.size());

            query = query.not();
            results = query.with("a").with(10).fetch().toList();
            assertEquals(count - 1, results.size());

            // Make sure record 5 is the one that's missing.
            for (StorableTestBasicIndexed stb : results) {
                assertFalse(5 == stb.getId());
            }

            // Should be back to the original query.
            query = query.not();

            results = query.with("a").with(10).fetch().toList();
            assertEquals(1, results.size());
        }

        // Try again, making sure that a full set of values is preserved in the query.
        {
            query = storage.query("intProp = ? & (stringProp >= ? | longProp < ?)");
            query = query.with(2).with("a").with(10);

            results = query.fetch().toList();
            assertEquals(1, results.size());

            query = query.not();
            results = query.fetch().toList();
            assertEquals(count - 1, results.size());

            // Make sure record 2 is the one that's missing.
            for (StorableTestBasicIndexed stb : results) {
                assertFalse(2 == stb.getId());
            }

            // Should be back to the original query.
            query = query.not();

            results = query.fetch().toList();
            assertEquals(1, results.size());
        }
    }

    public void test_indexed_and() throws Exception {
        final int count = 10;
        populateIndexed(count, new long[] {45, 12, 34, 12, 12, 45, 0, 0, 10, 2});

        final Storage<StorableTestBasicIndexed> storage =
            getRepository().storageFor(StorableTestBasicIndexed.class);

        Query<StorableTestBasicIndexed> query; 
        List<StorableTestBasicIndexed> results;

        {
            query = storage.query("longProp = ?");
            try {
                query = query.and("stringProp != ?");
                fail();
            } catch (IllegalStateException e) {
                // Good. Blank params exist.
            }
        }

        {
            query = storage.query("longProp = ?").with(12);
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(2, results.get(0).getId());
            assertEquals(5, results.get(1).getId());
        }

        // Different value.
        {
            query = storage.query("longProp = ?").with(45);
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(1, results.get(0).getId());
            assertEquals(6, results.get(1).getId());
        }

        // Different value with ordering.
        {
            query = storage.query("longProp = ?").with(45);
            query = query.orderBy("-intProp");
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(1, results.get(1).getId());
            assertEquals(6, results.get(0).getId());
        }

        // Try again with ordering.
        {
            query = storage.query("longProp = ?").with(12);
            query = query.orderBy("-intProp");
            query = query.and("stringProp != ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(2, results.size());
            assertEquals(2, results.get(1).getId());
            assertEquals(5, results.get(0).getId());
        }

        // Invert the selection
        {
            query = storage.query("longProp = ?").with(12);
            query = query.and("stringProp != ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 2, results.size());
            assertEquals(1, results.get(0).getId());
            assertEquals(3, results.get(1).getId());
            assertEquals(4, results.get(2).getId());
            assertEquals(6, results.get(3).getId());
            assertEquals(7, results.get(4).getId());
            assertEquals(8, results.get(5).getId());
            assertEquals(9, results.get(6).getId());
            assertEquals(10, results.get(7).getId());
        }

        // Invert the ordered selection
        {
            query = storage.query("longProp = ?").with(12);
            query = query.orderBy("-intProp");
            query = query.and("stringProp != ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 2, results.size());
            assertEquals(1, results.get(7).getId());
            assertEquals(3, results.get(6).getId());
            assertEquals(4, results.get(5).getId());
            assertEquals(6, results.get(4).getId());
            assertEquals(7, results.get(3).getId());
            assertEquals(8, results.get(2).getId());
            assertEquals(9, results.get(1).getId());
            assertEquals(10, results.get(0).getId());
        }
    }

    public void test_indexed_or() throws Exception {
        final int count = 10;
        populateIndexed(count, new long[] {45, 12, 34, 12, 12, 45, 0, 0, 10, 2});

        final Storage<StorableTestBasicIndexed> storage =
            getRepository().storageFor(StorableTestBasicIndexed.class);

        Query<StorableTestBasicIndexed> query; 
        List<StorableTestBasicIndexed> results;

        {
            query = storage.query("longProp = ?");
            try {
                query = query.or("stringProp = ?");
                fail();
            } catch (IllegalStateException e) {
                // Good. Blank params exist.
            }
        }

        {
            query = storage.query("longProp = ?").with(45);
            query = query.or("stringProp = ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(3, results.size());
            assertEquals(1, results.get(0).getId());
            assertEquals(4, results.get(1).getId());
            assertEquals(6, results.get(2).getId());
        }

        // Different value.
        {
            query = storage.query("longProp = ?").with(12);
            query = query.or("stringProp = ?").with("str_3");

            results = query.fetch().toList();
            assertEquals(4, results.size());
            assertEquals(2, results.get(0).getId());
            assertEquals(3, results.get(1).getId());
            assertEquals(4, results.get(2).getId());
            assertEquals(5, results.get(3).getId());
        }

        // Different value with ordering.
        {
            query = storage.query("longProp = ?").with(12);
            query = query.orderBy("-intProp");
            query = query.or("stringProp = ?").with("str_3");

            results = query.fetch().toList();
            assertEquals(4, results.size());
            assertEquals(2, results.get(3).getId());
            assertEquals(3, results.get(2).getId());
            assertEquals(4, results.get(1).getId());
            assertEquals(5, results.get(0).getId());
        }

        // Try again with ordering.
        {
            query = storage.query("longProp = ?").with(45);
            query = query.orderBy("-intProp");
            query = query.or("stringProp = ?").with("str_4");

            results = query.fetch().toList();
            assertEquals(3, results.size());
            assertEquals(1, results.get(2).getId());
            assertEquals(4, results.get(1).getId());
            assertEquals(6, results.get(0).getId());
        }

        // Invert the selection
        {
            query = storage.query("longProp = ?").with(45);
            query = query.or("stringProp = ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 3, results.size());
            assertEquals(2, results.get(0).getId());
            assertEquals(3, results.get(1).getId());
            assertEquals(5, results.get(2).getId());
            assertEquals(7, results.get(3).getId());
            assertEquals(8, results.get(4).getId());
            assertEquals(9, results.get(5).getId());
            assertEquals(10, results.get(6).getId());
        }

        // Invert the ordered selection
        {
            query = storage.query("longProp = ?").with(45);
            query = query.orderBy("-intProp");
            query = query.or("stringProp = ?").with("str_4").not();

            results = query.fetch().toList();
            assertEquals(count - 3, results.size());
            assertEquals(2, results.get(6).getId());
            assertEquals(3, results.get(5).getId());
            assertEquals(5, results.get(4).getId());
            assertEquals(7, results.get(3).getId());
            assertEquals(8, results.get(2).getId());
            assertEquals(9, results.get(1).getId());
            assertEquals(10, results.get(0).getId());
        }
    }

    private void populate(int count) throws Exception {
        populate(count, null);
    }

    private void populate(int count, long[] longValues) throws Exception {
        final Storage<StorableTestBasic> storage =
            getRepository().storageFor(StorableTestBasic.class);

        StorableTestBasic stb;

        // Insert some test data
        for (int i=1; i<=count; i++) {
            stb = storage.prepare();
            stb.initBasicProperties();
            stb.setId(i);
            stb.setIntProp(i);
            stb.setStringProp("str_" + i);
            if (longValues != null && i <= longValues.length) {
                stb.setLongProp(longValues[i - 1]);
            }
            stb.insert();
        }
    }

    private void populateIndexed(int count) throws Exception {
        populateIndexed(count, null);
    }

    private void populateIndexed(int count, long[] longValues) throws Exception {
        final Storage<StorableTestBasicIndexed> storage =
            getRepository().storageFor(StorableTestBasicIndexed.class);

        StorableTestBasicIndexed stb;

        // Insert some test data
        for (int i=1; i<=count; i++) {
            stb = storage.prepare();
            stb.initBasicProperties();
            stb.setId(i);
            stb.setIntProp(i);
            stb.setStringProp("str_" + i);
            if (longValues != null && i <= longValues.length) {
                stb.setLongProp(longValues[i - 1]);
            }
            stb.insert();
        }
    }

    private void assertOrder(List<StorableTestBasic> list, boolean ascending) throws Exception {
        StorableTestBasic last = null;
        for (StorableTestBasic stb : list) {
            if (last != null) {
                if (ascending) {
                    if (stb.getIntProp() <= last.getIntProp()) {
                        fail();
                    }
                } else {
                    if (stb.getIntProp() >= last.getIntProp()) {
                        fail();
                    }
                }
            }
            last = stb;
        }
    }

    private void assertOrderIndexed(List<StorableTestBasicIndexed> list, boolean ascending)
        throws Exception
    {
        StorableTestBasicIndexed last = null;
        for (StorableTestBasicIndexed stb : list) {
            if (last != null) {
                if (ascending) {
                    if (stb.getIntProp() <= last.getIntProp()) {
                        fail();
                    }
                } else {
                    if (stb.getIntProp() >= last.getIntProp()) {
                        fail();
                    }
                }
            }
            last = stb;
        }
    }
}
