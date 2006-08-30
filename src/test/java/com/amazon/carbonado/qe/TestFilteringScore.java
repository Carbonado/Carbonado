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

import java.util.Comparator;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.Direction;
import static com.amazon.carbonado.info.Direction.*;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.PropertyFilter;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestFilteringScore extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilteringScore.class);
    }

    static <S extends Storable> StorableIndex<S> makeIndex(Class<S> type, String... props) {
        return TestOrderingScore.makeIndex(type, props);
    }

    public TestFilteringScore(String name) {
        super(name);
    }

    public void testNoFilter() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class, "id");
        Filter<StorableTestBasic> filter = Filter.getOpenFilter(StorableTestBasic.class);
        FilteringScore score = FilteringScore.evaluate(ix, filter);

        assertFalse(score.isIndexClustered());
        assertEquals(1, score.getIndexPropertyCount());

        assertEquals(0, score.getIdentityCount());
        assertEquals(0, score.getIdentityFilters().size());
        assertFalse(score.hasRangeStart());
        assertEquals(0, score.getRangeStartFilters().size());
        assertFalse(score.hasRangeEnd());
        assertEquals(0, score.getRangeEndFilters().size());
        assertFalse(score.hasRangeMatch());
        assertFalse(score.hasAnyMatches());
        assertEquals(0, score.getRemainderCount());
        assertEquals(0, score.getRemainderFilters().size());
    }

    public void testSimpleIndexMisses() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class, "intProp");
        // Filter by a property not in index.
        Filter<StorableTestBasic> filter = Filter.filterFor(StorableTestBasic.class, "id = ?");

        FilteringScore score = FilteringScore.evaluate(ix, filter);

        assertFalse(score.isIndexClustered());
        assertEquals(1, score.getIndexPropertyCount());

        assertEquals(0, score.getIdentityCount());
        assertEquals(0, score.getIdentityFilters().size());
        assertFalse(score.hasRangeStart());
        assertEquals(0, score.getRangeStartFilters().size());
        assertFalse(score.hasRangeEnd());
        assertEquals(0, score.getRangeEndFilters().size());
        assertFalse(score.hasRangeMatch());
        assertFalse(score.hasAnyMatches());
        assertEquals(1, score.getRemainderCount());
        assertEquals(1, score.getRemainderFilters().size());
        assertEquals(filter, score.getRemainderFilters().get(0));
        assertEquals(filter, score.getRemainderFilter());

        // Try again with matching property, but with an operator that cannot be used by index.
        filter = Filter.filterFor(StorableTestBasic.class, "intProp != ?");

        score = FilteringScore.evaluate(ix, filter);

        assertFalse(score.isIndexClustered());
        assertEquals(1, score.getIndexPropertyCount());

        assertEquals(0, score.getIdentityCount());
        assertEquals(0, score.getIdentityFilters().size());
        assertFalse(score.hasRangeStart());
        assertEquals(0, score.getRangeStartFilters().size());
        assertFalse(score.hasRangeEnd());
        assertEquals(0, score.getRangeEndFilters().size());
        assertFalse(score.hasRangeMatch());
        assertFalse(score.hasAnyMatches());
        assertEquals(1, score.getRemainderCount());
        assertEquals(1, score.getRemainderFilters().size());
        assertEquals(filter, score.getRemainderFilters().get(0));
        assertEquals(filter, score.getRemainderFilter());
    }

    public void testSimpleIndexMatches() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class, "id");
        // Filter by a property in index.
        Filter<StorableTestBasic> filter = Filter.filterFor(StorableTestBasic.class, "id = ?");

        FilteringScore score = FilteringScore.evaluate(ix, filter);

        assertFalse(score.isIndexClustered());
        assertEquals(1, score.getIndexPropertyCount());

        assertEquals(1, score.getIdentityCount());
        assertEquals(1, score.getArrangementScore());
        assertEquals(filter, score.getIdentityFilters().get(0));
        assertFalse(score.hasRangeStart());
        assertEquals(0, score.getRangeStartFilters().size());
        assertFalse(score.hasRangeEnd());
        assertEquals(0, score.getRangeEndFilters().size());
        assertFalse(score.hasRangeMatch());
        assertTrue(score.hasAnyMatches());
        assertEquals(0, score.getRemainderCount());
        assertEquals(0, score.getRemainderFilters().size());
        assertEquals(null, score.getRemainderFilter());

        // Try again with open ranges.
        for (int i=0; i<4; i++) {
            String expr;
            switch (i) {
            default: expr = "id > ?"; break;
            case 1: expr = "id >= ?"; break;
            case 2: expr = "id < ?"; break;
            case 3: expr = "id <= ?"; break;
            }

            filter = Filter.filterFor(StorableTestBasic.class, expr);

            score = FilteringScore.evaluate(ix, filter);

            assertEquals(0, score.getIdentityCount());
            assertEquals(0, score.getIdentityFilters().size());
            assertFalse(score.hasRangeMatch());
            assertTrue(score.hasAnyMatches());
            assertEquals(0, score.getRemainderCount());
            assertEquals(0, score.getRemainderFilters().size());
            assertEquals(null, score.getRemainderFilter());

            if (i < 2) {
                assertTrue(score.hasRangeStart());
                assertEquals(1, score.getRangeStartFilters().size());
                assertEquals(filter, score.getRangeStartFilters().get(0));
                assertFalse(score.hasRangeEnd());
                assertEquals(0, score.getRangeEndFilters().size());
            } else {
                assertFalse(score.hasRangeStart());
                assertEquals(0, score.getRangeStartFilters().size());
                assertTrue(score.hasRangeEnd());
                assertEquals(1, score.getRangeEndFilters().size());
                assertEquals(filter, score.getRangeEndFilters().get(0));
            }
        }

        // Try with duplicate open ranges.
        filter = Filter.filterFor(StorableTestBasic.class, "id > ? & id > ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(0, score.getIdentityCount());
        assertEquals(0, score.getIdentityFilters().size());
        assertFalse(score.hasRangeMatch());
        assertTrue(score.hasAnyMatches());
        assertEquals(0, score.getRemainderCount());
        assertEquals(0, score.getRemainderFilters().size());
        assertEquals(null, score.getRemainderFilter());

        assertTrue(score.hasRangeStart());
        assertEquals(2, score.getRangeStartFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id > ?"),
                     score.getRangeStartFilters().get(0));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id > ?"),
                     score.getRangeStartFilters().get(1));
        assertFalse(score.hasRangeEnd());
        assertEquals(0, score.getRangeEndFilters().size());

        // Try with duplicate end ranges and slightly different operator.
        filter = Filter.filterFor(StorableTestBasic.class, "id < ? & id <= ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(0, score.getIdentityCount());
        assertEquals(0, score.getIdentityFilters().size());
        assertFalse(score.hasRangeMatch());
        assertTrue(score.hasAnyMatches());
        assertEquals(0, score.getRemainderCount());
        assertEquals(0, score.getRemainderFilters().size());
        assertEquals(null, score.getRemainderFilter());

        assertFalse(score.hasRangeStart());
        assertEquals(0, score.getRangeStartFilters().size());
        assertTrue(score.hasRangeEnd());
        assertEquals(2, score.getRangeEndFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id < ?"),
                     score.getRangeEndFilters().get(0));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id <= ?"),
                     score.getRangeEndFilters().get(1));

        // Try with complete range.
        filter = Filter.filterFor(StorableTestBasic.class, "id > ? & id < ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(0, score.getIdentityCount());
        assertEquals(0, score.getIdentityFilters().size());
        assertTrue(score.hasRangeMatch());
        assertTrue(score.hasAnyMatches());
        assertEquals(0, score.getRemainderCount());
        assertEquals(0, score.getRemainderFilters().size());
        assertEquals(null, score.getRemainderFilter());

        assertTrue(score.hasRangeStart());
        assertEquals(1, score.getRangeStartFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id > ?"),
                     score.getRangeStartFilters().get(0));
        assertTrue(score.hasRangeEnd());
        assertEquals(1, score.getRangeEndFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id < ?"),
                     score.getRangeEndFilters().get(0));

        // Try with an identity filter consuming others.
        filter = Filter.filterFor(StorableTestBasic.class, "id > ? & id = ? & id = ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(1, score.getIdentityCount());
        assertEquals(1, score.getIdentityFilters().size());
        assertEquals(1, score.getArrangementScore());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id = ?"),
                     score.getIdentityFilters().get(0));

        assertFalse(score.hasRangeStart());
        assertFalse(score.hasRangeEnd());
        assertFalse(score.hasRangeMatch());
        assertTrue(score.hasAnyMatches());
        assertEquals(2, score.getRemainderCount());
        assertEquals(2, score.getRemainderFilters().size());
        // Order of properties in filter accounts for sorting by PropertyFilterList.
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id = ? & id > ?"),
                     score.getRemainderFilter());

        // Try with complex mix of non-identity matches.
        filter = Filter.filterFor
            (StorableTestBasic.class,
             "id > ? & id != ? & id <= ? & id >= ? & id != ? & id > ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(0, score.getIdentityCount());
        assertEquals(0, score.getIdentityFilters().size());

        assertTrue(score.hasRangeMatch());
        assertTrue(score.hasAnyMatches());

        assertTrue(score.hasRangeStart());
        assertEquals(3, score.getRangeStartFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id > ?"),
                     score.getRangeStartFilters().get(0));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id >= ?"),
                     score.getRangeStartFilters().get(1));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id > ?"),
                     score.getRangeStartFilters().get(2));

        assertTrue(score.hasRangeEnd());
        assertEquals(1, score.getRangeEndFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id <= ?"),
                     score.getRangeEndFilters().get(0));

        assertEquals(2, score.getRemainderCount());
        assertEquals(2, score.getRemainderFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id != ? & id != ?"),
                     score.getRemainderFilter());
    }

    public void testCompositeIndexMatches() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class,
                                                        "id", "intProp", "stringProp");
        // Filter by a property in index.
        Filter<StorableTestBasic> filter = Filter.filterFor(StorableTestBasic.class, "id = ?");

        FilteringScore score = FilteringScore.evaluate(ix, filter);

        assertFalse(score.isIndexClustered());
        assertEquals(3, score.getIndexPropertyCount());

        assertEquals(1, score.getIdentityCount());
        assertEquals(1, score.getArrangementScore());
        assertEquals(filter, score.getIdentityFilters().get(0));
        assertFalse(score.hasRangeStart());
        assertEquals(0, score.getRangeStartFilters().size());
        assertFalse(score.hasRangeEnd());
        assertEquals(0, score.getRangeEndFilters().size());
        assertFalse(score.hasRangeMatch());
        assertTrue(score.hasAnyMatches());
        assertEquals(0, score.getRemainderCount());
        assertEquals(0, score.getRemainderFilters().size());
        assertEquals(null, score.getRemainderFilter());

        // Filter by a property with a gap. (filter must include "id" to use index)
        filter = Filter.filterFor(StorableTestBasic.class, "intProp = ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(0, score.getIdentityCount());
        assertFalse(score.hasAnyMatches());
        assertEquals(1, score.getRemainderCount());
        assertEquals(1, score.getRemainderFilters().size());
        assertEquals(filter, score.getRemainderFilter());

        // Filter by a property with a gap and a range operator. (index still cannot be used)
        filter = Filter.filterFor(StorableTestBasic.class, "intProp = ? & stringProp < ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(0, score.getIdentityCount());
        assertFalse(score.hasAnyMatches());
        assertEquals(2, score.getRemainderCount());
        assertEquals(2, score.getRemainderFilters().size());
        assertEquals(filter, score.getRemainderFilter());

        // Filter with range match before identity match. Identity cannot be used.
        filter = Filter.filterFor(StorableTestBasic.class, "intProp = ? & id < ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(0, score.getIdentityCount());
        assertTrue(score.hasAnyMatches());

        assertTrue(score.hasRangeEnd());
        assertEquals(1, score.getRangeEndFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id < ?"),
                     score.getRangeEndFilters().get(0));

        assertEquals(1, score.getRemainderCount());
        assertEquals(1, score.getRemainderFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "intProp = ?"),
                     score.getRemainderFilter());

        // Filter with fully specified identity match, but a few remaining.
        filter = Filter.filterFor
            (StorableTestBasic.class,
             "intProp = ? & id = ? & stringProp = ? & stringProp > ? & doubleProp = ?");

        score = FilteringScore.evaluate(ix, filter);

        assertTrue(score.hasAnyMatches());
        assertEquals(3, score.getIdentityCount());
        assertEquals(2, score.getArrangementScore());

        assertEquals(Filter.filterFor(StorableTestBasic.class, "id = ?"),
                     score.getIdentityFilters().get(0));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "intProp = ?"),
                     score.getIdentityFilters().get(1));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "stringProp = ?"),
                     score.getIdentityFilters().get(2));

        assertEquals(2, score.getRemainderCount());
        assertEquals(2, score.getRemainderFilters().size());
        // Order of remainder properties accounts for sorting by PropertyFilterList.
        assertEquals(Filter.filterFor(StorableTestBasic.class, "doubleProp = ?"),
                     score.getRemainderFilters().get(0));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "stringProp > ?"),
                     score.getRemainderFilters().get(1));

        // Filter with identity and range matches.
        filter = Filter.filterFor
            (StorableTestBasic.class,
             "intProp > ? & id = ? & stringProp = ? & intProp <= ? & " +
             "stringProp < ? & doubleProp = ?");

        score = FilteringScore.evaluate(ix, filter);

        assertTrue(score.hasAnyMatches());
        assertEquals(1, score.getIdentityCount());
        assertEquals(1, score.getArrangementScore());

        assertEquals(3, score.getRemainderCount());
        assertEquals(3, score.getRemainderFilters().size());
        // Order of remainder properties accounts for sorting by PropertyFilterList.
        assertEquals(Filter.filterFor(StorableTestBasic.class, "stringProp = ?"),
                     score.getRemainderFilters().get(0));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "doubleProp = ?"),
                     score.getRemainderFilters().get(1));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "stringProp < ?"),
                     score.getRemainderFilters().get(2));

        assertTrue(score.hasRangeMatch());
        assertTrue(score.hasRangeStart());
        assertEquals(1, score.getRangeStartFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "intProp > ?"),
                     score.getRangeStartFilters().get(0));

        assertTrue(score.hasRangeEnd());
        assertEquals(1, score.getRangeEndFilters().size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "intProp <= ?"),
                     score.getRangeEndFilters().get(0));

        // Filter with fully specified identity match, with backwards arrangement.
        filter = Filter.filterFor
            (StorableTestBasic.class, "stringProp = ? & intProp = ? & id = ?");

        score = FilteringScore.evaluate(ix, filter);

        assertTrue(score.hasAnyMatches());
        assertEquals(3, score.getIdentityCount());
        // Arrangement score is always at least one if identity count is not zero.
        assertEquals(1, score.getArrangementScore());
    }

    public void testReverseRange() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class,
                                                        "id", "intProp");
        Filter<StorableTestBasic> filter =
            Filter.filterFor(StorableTestBasic.class, "id = ? & intProp > ?");

        FilteringScore score = FilteringScore.evaluate(ix, filter);

        assertFalse(score.shouldReverseRange());

        ix = makeIndex(StorableTestBasic.class, "id", "-intProp");
        score = FilteringScore.evaluate(ix, filter);

        assertTrue(score.shouldReverseRange());
    }

    public void testRangeComparator() throws Exception {
        StorableIndex<StorableTestBasic> ix_1, ix_2;
        Filter<StorableTestBasic> filter;
        FilteringScore score_1, score_2;
        Comparator<FilteringScore<?>> comp = FilteringScore.rangeComparator();

        ix_1 = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp", "-stringProp");
        ix_2 = makeIndex(StorableTestBasic.class, "id", "stringProp", "intProp");

        filter = Filter.filterFor(StorableTestBasic.class, "id = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        score_1 = FilteringScore.evaluate(ix_1.clustered(true), filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(-1, comp.compare(score_1, score_2));

        filter = Filter.filterFor(StorableTestBasic.class, "stringProp = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "stringProp = ?");
        score_1 = FilteringScore.evaluate(ix_1.clustered(true), filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & stringProp != ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & stringProp = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        // Both indexes are as good, since range is open.
        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & stringProp > ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class,
                                  "id = ? & intProp = ? & stringProp > ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        // Test range match with tie resolved by clustered index.
        filter = Filter.filterFor(StorableTestBasic.class, "id >= ? & id < ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id >= ? & id < ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2.clustered(true), filter);

        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        // Test open range match with tie still not resolved by clustered index.
        filter = Filter.filterFor(StorableTestBasic.class, "id >= ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id >= ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2.clustered(true), filter);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));
    }

    public void testFullComparator() throws Exception {
        StorableIndex<StorableTestBasic> ix_1, ix_2;
        Filter<StorableTestBasic> filter;
        FilteringScore score_1, score_2;
        Comparator<FilteringScore<?>> comp = FilteringScore.fullComparator();

        ix_1 = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp", "-stringProp");
        ix_2 = makeIndex(StorableTestBasic.class, "id", "stringProp", "intProp");

        filter = Filter.filterFor(StorableTestBasic.class, "id = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        // Second is better because it has fewer properties.
        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        score_1 = FilteringScore.evaluate(ix_1.clustered(true), filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(-1, comp.compare(score_1, score_2));

        filter = Filter.filterFor(StorableTestBasic.class, "stringProp = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        // Although no property matches, second is better just because it has fewer properties.
        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "stringProp = ?");
        score_1 = FilteringScore.evaluate(ix_1.clustered(true), filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        // Although no property matches, first is better just because it is clustered.
        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & stringProp != ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        // Second is better because it has fewer properties.
        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & stringProp = ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        // Second index is better since the open range matches.
        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & stringProp > ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class,
                                  "id = ? & intProp = ? & stringProp > ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        // Test range match with tie resolved by clustered index.
        filter = Filter.filterFor(StorableTestBasic.class, "id >= ? & id < ?");
        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        // Second is better because it has fewer properties.
        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id >= ? & id < ?");
        score_1 = FilteringScore.evaluate(ix_1.clustered(true), filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        // First is better because it is clusted.
        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        // Test range match with tie resolved by clustered index.
        filter = Filter.filterFor(StorableTestBasic.class, "id >= ? & id < ?");
        score_1 = FilteringScore.evaluate(ix_1.clustered(true), filter);
        score_2 = FilteringScore.evaluate(ix_2.clustered(true), filter);

        // Second is better because it has fewer properties.
        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));
    }

    public void testArrangementFullComparator() throws Exception {
        StorableIndex<StorableTestBasic> ix_1, ix_2;
        Filter<StorableTestBasic> filter;
        FilteringScore score_1, score_2;

        ix_1 = makeIndex(StorableTestBasic.class, "id", "intProp", "-stringProp");
        ix_2 = makeIndex(StorableTestBasic.class, "id", "stringProp", "intProp");

        filter = Filter.filterFor(StorableTestBasic.class,
                                  "id = ? & intProp = ? & stringProp = ?");

        score_1 = FilteringScore.evaluate(ix_1, filter);
        score_2 = FilteringScore.evaluate(ix_2, filter);

        Comparator<FilteringScore<?>> comp = FilteringScore.rangeComparator();

        // With just range comparison, either index works.
        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));

        comp = FilteringScore.fullComparator();

        // First index is better because property arrangement matches.
        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));
    }

    public void testRangeFilterSubset() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class, "id");
        Filter<StorableTestBasic> filter;
        FilteringScore score;

        filter = Filter.filterFor(StorableTestBasic.class, "id >= ? & id >= ? & id < ? & id <= ?");

        score = FilteringScore.evaluate(ix, filter);

        assertEquals(2, score.getRangeStartFilters().size());

        List<PropertyFilter<?>> exStart = score.getExclusiveRangeStartFilters();
        assertEquals(0, exStart.size());

        List<PropertyFilter<?>> inStart = score.getInclusiveRangeStartFilters();
        assertEquals(2, inStart.size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id >= ?"), inStart.get(0));
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id >= ?"), inStart.get(1));

        assertEquals(2, score.getRangeEndFilters().size());

        List<PropertyFilter<?>> exEnd = score.getExclusiveRangeEndFilters();
        assertEquals(1, exEnd.size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id < ?"), exEnd.get(0));

        List<PropertyFilter<?>> inEnd = score.getInclusiveRangeEndFilters();
        assertEquals(1, inEnd.size());
        assertEquals(Filter.filterFor(StorableTestBasic.class, "id <= ?"), inEnd.get(0));
    }

    public void testKeyMatch() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class, "id", "intProp");
        ix = ix.unique(true);
        Filter<StorableTestBasic> filter;
        FilteringScore score;

        filter = Filter.filterFor(StorableTestBasic.class, "id = ?");
        score = FilteringScore.evaluate(ix, filter);
        assertFalse(score.isKeyMatch());

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp > ?");
        score = FilteringScore.evaluate(ix, filter);
        assertFalse(score.isKeyMatch());

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp = ?");
        score = FilteringScore.evaluate(ix, filter);
        assertTrue(score.isKeyMatch());

        filter = Filter.filterFor(StorableTestBasic.class,
                                  "id = ? & intProp = ? & doubleProp = ?");
        score = FilteringScore.evaluate(ix, filter);
        assertTrue(score.isKeyMatch());
    }
}
