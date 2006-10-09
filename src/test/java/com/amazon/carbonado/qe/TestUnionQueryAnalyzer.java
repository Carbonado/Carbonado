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

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.PropertyFilter;

import com.amazon.carbonado.repo.toy.ToyRepository;

import com.amazon.carbonado.stored.Address;
import com.amazon.carbonado.stored.Order;
import com.amazon.carbonado.stored.Shipment;
import com.amazon.carbonado.stored.Shipper;
import com.amazon.carbonado.stored.StorableTestBasic;

import static com.amazon.carbonado.qe.TestIndexedQueryExecutor.Mock;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestUnionQueryAnalyzer extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestUnionQueryAnalyzer.class);
    }

    static <S extends Storable> StorableIndex<S> makeIndex(Class<S> type, String... props) {
        return TestOrderingScore.makeIndex(type, props);
    }

    static <S extends Storable> OrderingList<S> makeOrdering(Class<S> type, String... props) {
        return TestOrderingScore.makeOrdering(type, props);
    }

    public TestUnionQueryAnalyzer(String name) {
        super(name);
    }

    public void testNullFilter() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        UnionQueryAnalyzer.Result result = uqa.analyze(null, null);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
    }

    public void testSingleSubResult() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "shipmentID = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
    }

    public void testSingleSubResultUnspecifiedDirection() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "shipmentID > ?");
        filter = filter.bind();
        OrderingList<Shipment> orderings =
            makeOrdering(Shipment.class, "~shipmentID", "~orderID");
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, orderings);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
        List<OrderedProperty<Shipment>> handled = 
            subResults.get(0).getCompositeScore().getOrderingScore().getHandledOrdering();
        assertEquals(1, handled.size());
        assertEquals("+shipmentID", handled.get(0).toString());
    }

    public void testSimpleUnion() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID = ? | orderID = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(Shipment.class, "+shipmentID"), result.getTotalOrdering());
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);
        IndexedQueryAnalyzer<Shipment>.Result res_1 = subResults.get(1);

        assertTrue(res_0.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentID = ?").bind(),
                     res_0.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_0.getLocalIndex());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(0, res_0.getRemainderOrdering().size());

        assertTrue(res_1.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "orderID = ?").bind(),
                     res_1.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"),
                     res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        assertEquals(1, res_1.getRemainderOrdering().size());
        assertEquals("+shipmentID", res_1.getRemainderOrdering().get(0).toString());
    }
 
    public void testSimpleUnion2() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID = ? | orderID > ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(Shipment.class, "+shipmentID"), result.getTotalOrdering());
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);
        IndexedQueryAnalyzer<Shipment>.Result res_1 = subResults.get(1);

        assertTrue(res_0.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentID = ?").bind(),
                     res_0.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_0.getLocalIndex());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(0, res_0.getRemainderOrdering().size());

        assertTrue(res_1.handlesAnything());
        assertTrue(res_1.getCompositeScore().getFilteringScore().hasRangeStart());
        assertFalse(res_1.getCompositeScore().getFilteringScore().hasRangeEnd());
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"),
                     res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        assertEquals(1, res_1.getRemainderOrdering().size());
        assertEquals("+shipmentID", res_1.getRemainderOrdering().get(0).toString());
    }

    public void testSimpleUnion3() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID = ? | orderID > ? & orderID <= ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(Shipment.class, "+shipmentID"), result.getTotalOrdering());
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);
        IndexedQueryAnalyzer<Shipment>.Result res_1 = subResults.get(1);

        assertTrue(res_0.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentID = ?").bind(),
                     res_0.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_0.getLocalIndex());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(0, res_0.getRemainderOrdering().size());

        // Note: index that has proper filtering is preferred because
        // "orderId > ? & orderID <= ?" filter specifies a complete range.
        // We'll have to do a sort, but it isn't expected to be over that many records.
        assertTrue(res_1.handlesAnything());
        assertTrue(res_1.getCompositeScore().getFilteringScore().hasRangeStart());
        assertTrue(res_1.getCompositeScore().getFilteringScore().hasRangeEnd());
        List<PropertyFilter<Shipment>> rangeFilters =
            res_1.getCompositeScore().getFilteringScore().getRangeStartFilters();
        assertEquals(1, rangeFilters.size());
        assertEquals(Filter.filterFor(Shipment.class, "orderID > ?").bind(), rangeFilters.get(0));
        rangeFilters = res_1.getCompositeScore().getFilteringScore().getRangeEndFilters();
        assertEquals(1, rangeFilters.size());
        assertEquals(Filter.filterFor(Shipment.class, "orderID <= ?").bind(), rangeFilters.get(0));
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"), res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        // Sort operation required because the "shipmentID" index was not chosen.
        assertEquals("+shipmentID", res_1.getRemainderOrdering().get(0).toString());
    }

    public void testSimpleUnionUnspecifiedDirection() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID > ? | orderID = ?");
        filter = filter.bind();
        OrderingList<Shipment> orderings =
            makeOrdering(Shipment.class, "~shipmentID", "~orderID");
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, orderings);
        assertEquals(OrderingList.get(Shipment.class, "+shipmentID", "+orderID"),
                     result.getTotalOrdering());
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);
        IndexedQueryAnalyzer<Shipment>.Result res_1 = subResults.get(1);

        List<OrderedProperty<Shipment>> handled =
            res_0.getCompositeScore().getOrderingScore().getHandledOrdering();
        assertEquals(1, handled.size());
        assertEquals("+shipmentID", handled.get(0).toString());

        handled = res_1.getCompositeScore().getOrderingScore().getHandledOrdering();
        assertEquals(0, handled.size());

        assertTrue(res_0.handlesAnything());
        assertTrue(res_0.getCompositeScore().getFilteringScore().hasRangeStart());
        assertFalse(res_0.getCompositeScore().getFilteringScore().hasRangeEnd());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_0.getLocalIndex());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(1, res_0.getRemainderOrdering().size());
        assertEquals("+orderID", res_0.getRemainderOrdering().get(0).toString());

        assertTrue(res_1.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "orderID = ?").bind(),
                     res_1.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"), res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        assertEquals(1, res_1.getRemainderOrdering().size());
        assertEquals("+shipmentID", res_1.getRemainderOrdering().get(0).toString());
    }

    public void testSimpleMerge() throws Exception {
        // Because query has an 'or' operation, the analyzer will initially
        // split this into a union. After futher analysis, it should decide
        // that this offers no benefit and will merge them back.
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class,
             "shipmentID = ? & (shipmentID = ? | orderID = ?)");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);

        assertTrue(res_0.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentID = ?").bind(),
                     res_0.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_0.getLocalIndex());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(0, res_0.getRemainderOrdering().size());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentID = ? | orderID = ?"),
                     res_0.getRemainderFilter().unbind());
    }

    public void testFullScan() throws Exception {
        // Because no indexes were selected, there's no union to perform.
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "shipmentNotes = ? | shipperID = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);

        assertFalse(res_0.handlesAnything());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(0, res_0.getRemainderOrdering().size());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentNotes = ? | shipperID = ?").bind(),
                     res_0.getRemainderFilter());
    }

    public void testFullScanFallback() throws Exception {
        // Because not all sub-results of union use an index, just fallback to
        // doing a full scan.
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "shipmentNotes = ? | orderID = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);

        assertFalse(res_0.handlesAnything());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(0, res_0.getRemainderOrdering().size());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentNotes = ? | orderID = ?").bind(),
                     res_0.getRemainderFilter());
    }

    public void testFullScanExempt() throws Exception {
        // Although not all sub-results use an index, one that does has a join
        // so it is exempt from folding into the full scan.
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "shipmentNotes = ? | orderID = ? & order.orderTotal > ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);
        IndexedQueryAnalyzer<Shipment>.Result res_1 = subResults.get(1);

        assertTrue(res_0.handlesAnything());
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"), res_0.getLocalIndex());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(1, res_0.getRemainderOrdering().size());
        assertEquals("+shipmentID", res_0.getRemainderOrdering().get(0).toString());
        assertEquals(Filter.filterFor(Shipment.class, "order.orderTotal > ?").bind(),
                     res_0.getRemainderFilter());

        assertTrue(res_1.handlesAnything());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        assertEquals(0, res_1.getRemainderOrdering().size());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentNotes = ?").bind(),
                     res_1.getRemainderFilter());

    }

    public void testComplexUnionPlan() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(StorableTestBasic.class,
                                   TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<StorableTestBasic> filter = Filter.filterFor
            (StorableTestBasic.class, "doubleProp = ? | (stringProp = ? & intProp = ?) | id > ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(StorableTestBasic.class, "+id"), result.getTotalOrdering());

        QueryExecutor<StorableTestBasic> exec = result.createExecutor();

        assertEquals(filter, exec.getFilter());
        assertEquals(OrderingList.get(StorableTestBasic.class, "+id"), exec.getOrdering());

        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(3, subResults.size());

        StringBuffer buf = new StringBuffer();
        exec.printPlan(buf, 0, null);
        String plan = buf.toString();

        String expected =
            "union\n" +
            "  sort: [+id]\n" +
            "    index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "    ...index: {properties=[-doubleProp, +longProp, ~id], unique=true}\n" +
            "    ...identity filter: doubleProp = ?\n" +
            "  index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "  ...index: {properties=[-stringProp, -intProp, ~id], unique=true}\n" +
            "  ...identity filter: stringProp = ? & intProp = ?\n" +
            "  clustered index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "  ...index: {properties=[+id], unique=true}\n" +
            "  ...range filter: id > ?\n";

        // Test test will fail if the format of the plan changes.
        assertEquals(expected, plan);
    }

    public void testComplexUnionPlan2() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(StorableTestBasic.class,
                                   TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<StorableTestBasic> filter = Filter.filterFor
            (StorableTestBasic.class, "doubleProp = ? | stringProp = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(StorableTestBasic.class, "+doubleProp", "+stringProp"),
                     result.getTotalOrdering());

        QueryExecutor<StorableTestBasic> exec = result.createExecutor();

        assertEquals(filter, exec.getFilter());
        assertEquals(OrderingList.get(StorableTestBasic.class, "+doubleProp", "+stringProp"),
                     exec.getOrdering());

        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());

        StringBuffer buf = new StringBuffer();
        exec.printPlan(buf, 0, null);
        String plan = buf.toString();

        String expected =
            "union\n" +
            "  sort: [+stringProp]\n" +
            "    index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "    ...index: {properties=[-doubleProp, +longProp, ~id], unique=true}\n" +
            "    ...identity filter: doubleProp = ?\n" +
            "  index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "  ...index: {properties=[+stringProp, +doubleProp], unique=true}\n" +
            "  ...identity filter: stringProp = ?\n";

        // Test test will fail if the format of the plan changes.
        assertEquals(expected, plan);
    }

    public void testComplexUnionPlan3() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(StorableTestBasic.class,
                                   TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<StorableTestBasic> filter = Filter.filterFor
            (StorableTestBasic.class, "stringProp = ? | stringProp = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(StorableTestBasic.class, "+stringProp", "+doubleProp"),
                     result.getTotalOrdering());

        QueryExecutor<StorableTestBasic> exec = result.createExecutor();
        assertEquals(OrderingList.get(StorableTestBasic.class, "+stringProp", "+doubleProp"),
                     exec.getOrdering());

        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());

        StringBuffer buf = new StringBuffer();
        exec.printPlan(buf, 0, null);
        String plan = buf.toString();

        String expected =
            "union\n" +
            "  index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "  ...index: {properties=[+stringProp, +doubleProp], unique=true}\n" +
            "  ...identity filter: stringProp = ?\n" +
            "  index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "  ...index: {properties=[+stringProp, +doubleProp], unique=true}\n" +
            "  ...identity filter: stringProp = ?[2]\n";

        // Test test will fail if the format of the plan changes.
        assertEquals(expected, plan);
    }

    public void testComplexUnionPlan4() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(StorableTestBasic.class,
                                   TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<StorableTestBasic> filter = Filter.filterFor
            (StorableTestBasic.class, "doubleProp = ? | stringProp = ? | id > ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(StorableTestBasic.class, "+doubleProp", "+id"),
                     result.getTotalOrdering());

        QueryExecutor<StorableTestBasic> exec = result.createExecutor();

        assertEquals(filter, exec.getFilter());
        assertEquals(OrderingList.get(StorableTestBasic.class, "+doubleProp", "+id"),
                     exec.getOrdering());

        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(3, subResults.size());

        StringBuffer buf = new StringBuffer();
        exec.printPlan(buf, 0, null);
        String plan = buf.toString();

        String expected =
            "union\n" +
            "  sort: [+id]\n" +
            "    index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "    ...index: {properties=[-doubleProp, +longProp, ~id], unique=true}\n" +
            "    ...identity filter: doubleProp = ?\n" +
            "  sort: [+doubleProp], [+id]\n" +
            "    index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "    ...index: {properties=[+stringProp, +doubleProp], unique=true}\n" +
            "    ...identity filter: stringProp = ?\n" +
            "  sort: [+doubleProp, +id]\n" +
            "    clustered index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "    ...index: {properties=[+id], unique=true}\n" +
            "    ...range filter: id > ?\n";

        // Test test will fail if the format of the plan changes.
        assertEquals(expected, plan);
    }

    public void testComplexUnionPlan5() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(StorableTestBasic.class,
                                   TestIndexedQueryAnalyzer.RepoAccess.INSTANCE);
        Filter<StorableTestBasic> filter = Filter.filterFor
            (StorableTestBasic.class, "stringProp = ? | stringProp = ? | id > ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        assertEquals(OrderingList.get(StorableTestBasic.class, "+stringProp", "+doubleProp"),
                     result.getTotalOrdering());

        QueryExecutor<StorableTestBasic> exec = result.createExecutor();

        assertEquals(filter, exec.getFilter());
        assertEquals(OrderingList.get(StorableTestBasic.class, "+stringProp", "+doubleProp"),
                     exec.getOrdering());

        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(3, subResults.size());

        StringBuffer buf = new StringBuffer();
        exec.printPlan(buf, 0, null);
        String plan = buf.toString();

        String expected =
            "union\n" +
            "  index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "  ...index: {properties=[+stringProp, +doubleProp], unique=true}\n" +
            "  ...identity filter: stringProp = ?\n" +
            "  index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "  ...index: {properties=[+stringProp, +doubleProp], unique=true}\n" +
            "  ...identity filter: stringProp = ?[2]\n" +
            "  sort: [+stringProp, +doubleProp]\n" +
            "    clustered index scan: com.amazon.carbonado.stored.StorableTestBasic\n" +
            "    ...index: {properties=[+id], unique=true}\n" +
            "    ...range filter: id > ?\n";

        // Test test will fail if the format of the plan changes.
        assertEquals(expected, plan);
    }
}
