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

import java.util.ArrayList;
import java.util.Collections;
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

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestOrderingScore extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestOrderingScore.class);
    }

    static <S extends Storable> StorableIndex<S> makeIndex(Class<S> type, String... props) {
        List<OrderedProperty<S>> list = makeOrderings(type, props);
        OrderedProperty<S>[] array = list.toArray(new OrderedProperty[0]);

        return new StorableIndex<S>(array, UNSPECIFIED);
    }

    static <S extends Storable> List<OrderedProperty<S>> makeOrderings(Class<S> type,
                                                                       String... props)
    {
        StorableInfo<S> info = StorableIntrospector.examine(type);
        List<OrderedProperty<S>> ops = new ArrayList<OrderedProperty<S>>(props.length);
        for (int i=0; i<props.length; i++) {
            ops.add(OrderedProperty.parse(info, props[i]));
        }
        return ops;
    }

    public TestOrderingScore(String name) {
        super(name);
    }

    public void testEmpty() throws Exception {
        StorableIndex<StorableTestBasic> ix = makeIndex(StorableTestBasic.class, "id");

        OrderingScore score = OrderingScore.evaluate(ix, null, null);

        assertEquals(0, score.getHandledCount());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());
    }

    public void testOneProp() throws Exception {
        StorableIndex<StorableTestBasic> ix;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore<StorableTestBasic> score;

        /////////////
        ix = makeIndex(StorableTestBasic.class, "id");

        ops = makeOrderings(StorableTestBasic.class, "id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "+id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        /////////////
        ix = makeIndex(StorableTestBasic.class, "+id");

        ops = makeOrderings(StorableTestBasic.class, "id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "+id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        /////////////
        ix = makeIndex(StorableTestBasic.class, "-id");

        ops = makeOrderings(StorableTestBasic.class, "id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "+id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(1, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        /////////////
        ix = makeIndex(StorableTestBasic.class, "intProp");

        ops = makeOrderings(StorableTestBasic.class, "id");
        score = OrderingScore.evaluate(ix, null, ops);

        assertEquals(0, score.getHandledCount());
        assertEquals(1, score.getRemainderCount());
        assertEquals("+id", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());
    }

    public void testMultipleProps() throws Exception {
        final StorableIndex<StorableTestBasic> ix;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore<StorableTestBasic> score;

        ix = makeIndex(StorableTestBasic.class, "id", "intProp");

        ops = makeOrderings(StorableTestBasic.class, "id");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-id");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "id", "intProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(2, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals("+intProp", score.getHandledOrderings().get(1).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-id", "-intProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(2, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals("-intProp", score.getHandledOrderings().get(1).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-id", "+intProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("+intProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(true, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "+id", "-intProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("-intProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "intProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(0, score.getHandledCount());
        assertEquals(1, score.getRemainderCount());
        assertEquals("+intProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        // Gap is allowed if identity filtered.

        Filter<StorableTestBasic> filter;

        filter = Filter.filterFor(StorableTestBasic.class, "id = ?");

        ops = makeOrderings(StorableTestBasic.class, "intProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+intProp", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-intProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("-intProp", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "intProp", "id");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+intProp", score.getHandledOrderings().get(0).toString());
        // Since "id" is filtered, don't count as remainder.
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "-intProp", "id");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("-intProp", score.getHandledOrderings().get(0).toString());
        // Since "id" is filtered, don't count as remainder.
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "intProp", "doubleProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+intProp", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("+doubleProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "intProp", "-doubleProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+intProp", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("-doubleProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        filter = Filter.filterFor(StorableTestBasic.class, "id > ? & doubleProp = ?");

        ops = makeOrderings(StorableTestBasic.class, "intProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(0, score.getHandledCount());
        assertEquals(1, score.getRemainderCount());
        assertEquals("+intProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        ops = makeOrderings(StorableTestBasic.class, "doubleProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(0, score.getHandledCount());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        filter = Filter.filterFor(StorableTestBasic.class, "doubleProp = ? & id = ?");

        ops = makeOrderings(StorableTestBasic.class, "doubleProp", "-intProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("-intProp", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());
    }

    public void testMidGap() throws Exception {
        final StorableIndex<StorableTestBasic> ix;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore score;
        Filter<StorableTestBasic> filter;

        ix = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp", "-stringProp");

        ops = makeOrderings(StorableTestBasic.class, "id", "intProp", "doubleProp", "-stringProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(4, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals("+intProp", score.getHandledOrderings().get(1).toString());
        assertEquals("+doubleProp", score.getHandledOrderings().get(2).toString());
        assertEquals("-stringProp", score.getHandledOrderings().get(3).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        // Now ignore mid index properties, creating a gap.

        ops = makeOrderings(StorableTestBasic.class, "id", "-stringProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("-stringProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        // Gap can be bridged if property is filtered out. First test with
        // incomplete bridges.

        filter = Filter.filterFor(StorableTestBasic.class, "doubleProp = ? & intProp > ?");

        ops = makeOrderings(StorableTestBasic.class, "id", "-stringProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("-stringProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        filter = Filter.filterFor(StorableTestBasic.class, "doubleProp >= ? & intProp = ?");

        ops = makeOrderings(StorableTestBasic.class, "id", "-stringProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("-stringProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(false, score.shouldReverseOrder());

        // Now a complete bridge.

        filter = Filter.filterFor(StorableTestBasic.class, "doubleProp = ? & intProp = ?");

        ops = makeOrderings(StorableTestBasic.class, "id", "-stringProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(2, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals("-stringProp", score.getHandledOrderings().get(1).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        // Again in reverse.

        ops = makeOrderings(StorableTestBasic.class, "-id", "stringProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(2, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals("+stringProp", score.getHandledOrderings().get(1).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        // Failed double reverse.

        ops = makeOrderings(StorableTestBasic.class, "-id", "-stringProp");
        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals(1, score.getRemainderCount());
        assertEquals("-stringProp", score.getRemainderOrderings().get(0).toString());
        assertEquals(true, score.shouldReverseOrder());
    }

    public void testComparator() throws Exception {
        StorableIndex<StorableTestBasic> ix_1, ix_2;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore score_1, score_2;
        Filter<StorableTestBasic> filter;
        Comparator<OrderingScore<?>> comp = OrderingScore.fullComparator();

        ix_1 = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp", "-stringProp");
        ix_2 = makeIndex(StorableTestBasic.class, "intProp", "doubleProp", "id");

        ops = makeOrderings(StorableTestBasic.class, "-id", "-intProp");
        score_1 = OrderingScore.evaluate(ix_1, null, ops);
        score_2 = OrderingScore.evaluate(ix_2, null, ops);

        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        filter = Filter.filterFor(StorableTestBasic.class, "id = ?");
        score_1 = OrderingScore.evaluate(ix_1, filter, ops);
        score_2 = OrderingScore.evaluate(ix_2, filter, ops);

        // Index 2 has less properties, so it is better.
        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        // Keep ix_2 slightly better by matching desired order.
        ix_2 = makeIndex(StorableTestBasic.class, "-intProp", "doubleProp", "id", "stringProp");

        score_1 = OrderingScore.evaluate(ix_1, filter, ops);
        score_2 = OrderingScore.evaluate(ix_2, filter, ops);

        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        // Make ix_1 slightly better by making it clustered.
        ix_1 = ix_1.clustered(true);

        score_1 = OrderingScore.evaluate(ix_1, filter, ops);
        score_2 = OrderingScore.evaluate(ix_2, filter, ops);

        assertEquals(-1, comp.compare(score_1, score_2));
        assertEquals(1, comp.compare(score_2, score_1));

        // Make ix_2 better when clustered.
        ix_2 = ix_2.clustered(true);

        score_1 = OrderingScore.evaluate(ix_1, filter, ops);
        score_2 = OrderingScore.evaluate(ix_2, filter, ops);

        assertEquals(1, comp.compare(score_1, score_2));
        assertEquals(-1, comp.compare(score_2, score_1));

        // Make ix_1 same by reversing order.
        ix_1 = ix_1.reverse();

        score_1 = OrderingScore.evaluate(ix_1, filter, ops);
        score_2 = OrderingScore.evaluate(ix_2, filter, ops);

        assertEquals(0, comp.compare(score_1, score_2));
        assertEquals(0, comp.compare(score_2, score_1));
    }

    public void testIndexNotNeeded() throws Exception {
        // Test an index which matches desired orderings, but ordering
        // properties are filtered out. Thus the index is not needed.

        final StorableIndex<StorableTestBasic> ix;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore score;
        Filter<StorableTestBasic> filter;

        ix = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp");

        ops = makeOrderings(StorableTestBasic.class, "id", "intProp", "doubleProp");
        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(3, score.getHandledCount());
        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals("+intProp", score.getHandledOrderings().get(1).toString());
        assertEquals("+doubleProp", score.getHandledOrderings().get(2).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp = ?");

        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(1, score.getHandledCount());
        assertEquals("+doubleProp", score.getHandledOrderings().get(0).toString());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp = ? & doubleProp =?");

        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(0, score.getHandledCount());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());
    }

    public void testUniqueIndexNotNeeded() throws Exception {
        // Test a unique index which has been fully specified. Ordering is not
        // needed at all.
        final StorableIndex<StorableTestBasic> ix;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore score;
        Filter<StorableTestBasic> filter;

        ix = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp").unique(true);
        ops = makeOrderings(StorableTestBasic.class, "stringProp", "doubleProp");
        filter = Filter.filterFor(StorableTestBasic.class, "id = ? & intProp = ? & doubleProp =?");

        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(0, score.getHandledCount());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());
    }

    public void testReduce() throws Exception {
        // Tests that redundant ordering properties are removed.
        final StorableIndex<StorableTestBasic> ix;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore score;
        Filter<StorableTestBasic> filter;

        ix = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp");
        ops = makeOrderings(StorableTestBasic.class, 
                            "intProp", "intProp", "id", "doubleProp", "intProp", "doubleProp",
                            "longProp", "longProp", "id", "intProp", "doubleProp");
        filter = Filter.filterFor(StorableTestBasic.class, "id = ?");

        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(2, score.getHandledCount());
        assertEquals(1, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        assertEquals("+intProp", score.getHandledOrderings().get(0).toString());
        assertEquals("+doubleProp", score.getHandledOrderings().get(1).toString());
        assertEquals("+longProp", score.getRemainderOrderings().get(0).toString());
    }

    public void testUnspecifiedDirection() throws Exception {
        // Tests that an originally unspecified ordering direction is determined.
        final StorableIndex<StorableTestBasic> ix;
        List<OrderedProperty<StorableTestBasic>> ops;
        OrderingScore score;
        Filter<StorableTestBasic> filter;

        ix = makeIndex(StorableTestBasic.class, "id", "intProp", "doubleProp");
        ops = makeOrderings(StorableTestBasic.class, "~intProp", "-doubleProp");
        filter = Filter.filterFor(StorableTestBasic.class, "id = ?");

        score = OrderingScore.evaluate(ix, filter, ops);
        assertEquals(2, score.getHandledCount());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        assertEquals("-intProp", score.getHandledOrderings().get(0).toString());
        assertEquals("-doubleProp", score.getHandledOrderings().get(1).toString());

        ops = makeOrderings(StorableTestBasic.class, "~id", "intProp", "~doubleProp");

        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(3, score.getHandledCount());
        assertEquals(0, score.getRemainderCount());
        assertEquals(false, score.shouldReverseOrder());

        assertEquals("+id", score.getHandledOrderings().get(0).toString());
        assertEquals("+intProp", score.getHandledOrderings().get(1).toString());
        assertEquals("+doubleProp", score.getHandledOrderings().get(2).toString());

        ops = makeOrderings(StorableTestBasic.class, "~id", "-intProp", "~doubleProp");

        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(3, score.getHandledCount());
        assertEquals(0, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals("-intProp", score.getHandledOrderings().get(1).toString());
        assertEquals("-doubleProp", score.getHandledOrderings().get(2).toString());

        ops = makeOrderings(StorableTestBasic.class, "~id", "-intProp", "~longProp");

        score = OrderingScore.evaluate(ix, null, ops);
        assertEquals(2, score.getHandledCount());
        assertEquals(1, score.getRemainderCount());
        assertEquals(true, score.shouldReverseOrder());

        assertEquals("-id", score.getHandledOrderings().get(0).toString());
        assertEquals("-intProp", score.getHandledOrderings().get(1).toString());
        assertEquals("+longProp", score.getRemainderOrderings().get(0).toString());
    }
}
