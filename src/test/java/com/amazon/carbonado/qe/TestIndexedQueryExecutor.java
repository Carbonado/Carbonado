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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.IteratorCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestIndexedQueryExecutor extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestIndexedQueryExecutor.class);
    }

    static <S extends Storable> StorableIndex<S> makeIndex(Class<S> type, String... props) {
        return TestOrderingScore.makeIndex(type, props);
    }

    static <S extends Storable> OrderingList<S> makeOrdering(Class<S> type, String... props) {
        return TestOrderingScore.makeOrdering(type, props);
    }

    public TestIndexedQueryExecutor(String name) {
        super(name);
    }

    public void testIdentityMatch() throws Exception {
        StorableIndex<StorableTestBasic> index =
            makeIndex(StorableTestBasic.class, "id", "-intProp", "doubleProp");

        Filter<StorableTestBasic> filter =
            Filter.filterFor(StorableTestBasic.class, "id = ?");
        FilterValues<StorableTestBasic> values = filter.initialFilterValues();
        filter = values.getFilter();

        CompositeScore<StorableTestBasic> score = CompositeScore.evaluate(index, filter, null);

        Mock<StorableTestBasic> executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(1, executor.mIdentityValues.length);
        assertEquals(100, executor.mIdentityValues[0]);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp = ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100).with(5));

        assertEquals(2, executor.mIdentityValues.length);
        assertEquals(100, executor.mIdentityValues[0]);
        assertEquals(5, executor.mIdentityValues[1]);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp = ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(200));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);
    }

    public void testOpenRangeStartMatch() throws Exception {
        StorableIndex<StorableTestBasic> index = makeIndex(StorableTestBasic.class, "intProp");

        Filter<StorableTestBasic> filter =
            Filter.filterFor(StorableTestBasic.class, "intProp > ?");
        FilterValues<StorableTestBasic> values = filter.initialFilterValues();
        filter = values.getFilter();

        CompositeScore<StorableTestBasic> score = CompositeScore.evaluate(index, filter, null);

        Mock<StorableTestBasic> executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp >= ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.INCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp > ? & intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(10).with(30));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(30, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp >= ? & intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(10).with(30));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(30, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp >= ? & intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(10).with(10));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(10, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp >= ? & intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(30).with(10));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.INCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(30, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp > ? & intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "-intProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100).with(30));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertTrue(executor.mReverseOrder);

        ///////
        index = makeIndex(StorableTestBasic.class, "-intProp");

        filter = Filter.filterFor(StorableTestBasic.class, "intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "-intProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "intProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, executor.mRangeEndBoundary);
        assertEquals(null, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertTrue(executor.mReverseOrder);
    }

    public void testOpenRangeEndMatch() throws Exception {
        StorableIndex<StorableTestBasic> index = makeIndex(StorableTestBasic.class, "intProp");

        Filter<StorableTestBasic> filter =
            Filter.filterFor(StorableTestBasic.class, "intProp < ?");
        FilterValues<StorableTestBasic> values = filter.initialFilterValues();
        filter = values.getFilter();

        CompositeScore<StorableTestBasic> score = CompositeScore.evaluate(index, filter, null);

        Mock<StorableTestBasic> executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(100, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp <= ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.INCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(100, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp < ? & intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(10).with(30));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(10, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp <= ? & intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(10).with(30));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.INCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(10, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp <= ? & intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(10).with(10));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(10, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp <= ? & intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(30).with(10));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(10, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp < ? & intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "-intProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100).with(30));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(30, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertTrue(executor.mReverseOrder);

        ///////
        index = makeIndex(StorableTestBasic.class, "-intProp");

        filter = Filter.filterFor(StorableTestBasic.class, "intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(100, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "-intProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(100, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "intProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(100, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertTrue(executor.mReverseOrder);
    }

    public void testClosedRangeMatch() throws Exception {
        // These tests are not as exhaustive, as I don't expect the combination
        // of start and end ranges to interfere with each other.

        StorableIndex<StorableTestBasic> index = makeIndex(StorableTestBasic.class, "intProp");

        Filter<StorableTestBasic> filter =
            Filter.filterFor(StorableTestBasic.class, "intProp > ? & intProp < ?");
        FilterValues<StorableTestBasic> values = filter.initialFilterValues();
        filter = values.getFilter();

        CompositeScore<StorableTestBasic> score = CompositeScore.evaluate(index, filter, null);

        Mock<StorableTestBasic> executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100).with(200));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(200, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class, "intProp >= ? & intProp <= ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(100).with(10));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.INCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(100, executor.mRangeStartValue);
        assertEquals(BoundaryType.INCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(10, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class,
                                  "intProp > ? & intProp < ? & intProp > ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(10).with(100).with(30));

        assertEquals(null, executor.mIdentityValues);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(30, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(100, executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);
    }

    public void testIdentityAndRangeMatch() throws Exception {
        // These tests are not as exhaustive, as I don't expect the combination
        // of identity and ranges to interfere with each other.

        StorableIndex<StorableTestBasic> index;
        Filter<StorableTestBasic> filter;
        FilterValues<StorableTestBasic> values;
        CompositeScore<StorableTestBasic> score;
        Mock<StorableTestBasic> executor;

        index = makeIndex(StorableTestBasic.class, "intProp", "-doubleProp", "stringProp");

        filter = Filter.filterFor(StorableTestBasic.class,
                                  "intProp = ? & doubleProp > ? & doubleProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter, null);

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(3).with(56.5).with(200.2));

        assertEquals(3, executor.mIdentityValues[0]);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(56.5, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(200.2, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "doubleProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(3).with(56.5).with(200.2));

        assertEquals(3, executor.mIdentityValues[0]);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(56.5, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(200.2, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertTrue(executor.mReverseOrder);

        ///////
        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "stringProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(3).with(56.5).with(200.2));

        assertEquals(3, executor.mIdentityValues[0]);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeStartBoundary);
        assertEquals(56.5, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals(200.2, executor.mRangeEndValue);
        assertTrue(executor.mReverseRange);
        assertFalse(executor.mReverseOrder);

        ///////
        filter = Filter.filterFor(StorableTestBasic.class,
                                  "intProp = ? & doubleProp = ? & stringProp < ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate(index, filter,
                                        makeOrdering(StorableTestBasic.class, "-stringProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        executor.fetch(values.with(3).with(56.5).with("foo"));

        assertEquals(3, executor.mIdentityValues[0]);
        assertEquals(56.5, executor.mIdentityValues[1]);
        assertEquals(BoundaryType.OPEN, executor.mRangeStartBoundary);
        assertEquals(null, executor.mRangeStartValue);
        assertEquals(BoundaryType.EXCLUSIVE, executor.mRangeEndBoundary);
        assertEquals("foo", executor.mRangeEndValue);
        assertFalse(executor.mReverseRange);
        assertTrue(executor.mReverseOrder);

        assertEquals(values.getFilter(), executor.getFilter());
        List<OrderedProperty<StorableTestBasic>> expectedOrdering =
            makeOrdering(StorableTestBasic.class, "-stringProp");
        assertEquals(expectedOrdering, executor.getOrdering());
    }

    public void testHandledOrdering() throws Exception {
        // Tests that ordering of executor only reveals what it actually uses.

        StorableIndex<StorableTestBasic> index;
        Filter<StorableTestBasic> filter;
        FilterValues<StorableTestBasic> values;
        CompositeScore<StorableTestBasic> score;
        Mock<StorableTestBasic> executor;

        index = makeIndex(StorableTestBasic.class, "intProp", "-doubleProp", "stringProp");

        filter = Filter.filterFor(StorableTestBasic.class, "intProp = ?");
        values = filter.initialFilterValues();
        filter = values.getFilter();

        score = CompositeScore.evaluate
            (index, filter,
             makeOrdering(StorableTestBasic.class, "intProp", "doubleProp"));

        executor = new Mock<StorableTestBasic>(index, score);

        assertEquals(values.getFilter(), executor.getFilter());
        List<OrderedProperty<StorableTestBasic>> expectedOrdering =
            makeOrdering(StorableTestBasic.class, "+doubleProp");
        assertEquals(expectedOrdering, executor.getOrdering());
    }

    /**
     * Mock object doesn't really open a cursor -- it just captures the passed
     * parameters.
     */
    static class Mock<S extends Storable> extends IndexedQueryExecutor<S>
        implements IndexedQueryExecutor.Support<S>
    {
        Object[] mIdentityValues;
        BoundaryType mRangeStartBoundary;
        Object mRangeStartValue;
        BoundaryType mRangeEndBoundary;
        Object mRangeEndValue;
        boolean mReverseRange;
        boolean mReverseOrder;

        public Mock(StorableIndex<S> index, CompositeScore<S> score) {
            super(null, index, score);
        }

        public Cursor<S> fetchSubset(StorableIndex<S> index,
                                     Object[] identityValues,
                                     BoundaryType rangeStartBoundary,
                                     Object rangeStartValue,
                                     BoundaryType rangeEndBoundary,
                                     Object rangeEndValue,
                                     boolean reverseRange,
                                     boolean reverseOrder)
        {
            mIdentityValues = identityValues;
            mRangeStartBoundary = rangeStartBoundary;
            mRangeStartValue = rangeStartValue;
            mRangeEndBoundary = rangeEndBoundary;
            mRangeEndValue = rangeEndValue;
            mReverseRange = reverseRange;
            mReverseOrder = reverseOrder;

            Collection<S> empty = Collections.emptyList();
            return new IteratorCursor<S>(empty);
        }
    }
}
