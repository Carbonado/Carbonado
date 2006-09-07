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

    static <S extends Storable> List<OrderedProperty<S>> makeOrderings(Class<S> type,
                                                                       String... props)
    {
        return TestOrderingScore.makeOrderings(type, props);
    }

    public TestUnionQueryAnalyzer(String name) {
        super(name);
    }

    public void testSingleSubResult() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "shipmentID = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
    }

    public void testSingleSubResultUnspecifiedDirection() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "shipmentID > ?");
        filter = filter.bind();
        List<OrderedProperty<Shipment>> orderings =
            makeOrderings(Shipment.class, "~shipmentID", "~orderID");
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, orderings);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(1, subResults.size());
        List<OrderedProperty<Shipment>> handled = 
            subResults.get(0).getCompositeScore().getOrderingScore().getHandledOrderings();
        assertEquals(1, handled.size());
        assertEquals("+shipmentID", handled.get(0).toString());
    }

    public void testSimpleUnion() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID = ? | orderID = ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
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
        assertEquals(0, res_0.getRemainderOrderings().size());

        assertTrue(res_1.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "orderID = ?").bind(),
                     res_1.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "orderID"), res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        assertEquals(1, res_1.getRemainderOrderings().size());
        assertEquals("+shipmentID", res_1.getRemainderOrderings().get(0).toString());
    }
 
    public void testSimpleUnion2() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID = ? | orderID > ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
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
        assertEquals(0, res_0.getRemainderOrderings().size());

        // Note: index that has proper ordering is preferred because "orderId > ?"
        // filter does not specify a complete range. It is not expected to actually
        // filter much, so we choose to avoid a large sort instead.
        assertTrue(res_1.handlesAnything());
        assertFalse(res_1.getCompositeScore().getFilteringScore().hasRangeStart());
        assertFalse(res_1.getCompositeScore().getFilteringScore().hasRangeEnd());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        assertEquals(0, res_0.getRemainderOrderings().size());
        // Remainder filter exists because the "orderID" index was not chosen.
        assertEquals(Filter.filterFor(Shipment.class, "orderID > ?").bind(),
                     res_1.getRemainderFilter());
    }

    public void testSimpleUnion3() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID = ? | orderID > ? & orderID <= ?");
        filter = filter.bind();
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, null);
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
        assertEquals(0, res_0.getRemainderOrderings().size());

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
        assertEquals(makeIndex(Shipment.class, "orderID"), res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        // Sort operation required because the "shipmentID" index was not chosen.
        assertEquals("+shipmentID", res_1.getRemainderOrderings().get(0).toString());
    }

    public void testSimpleUnionUnspecifiedDirection() throws Exception {
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class,
                                                   "shipmentID > ? | orderID = ?");
        filter = filter.bind();
        List<OrderedProperty<Shipment>> orderings =
            makeOrderings(Shipment.class, "~shipmentID", "~orderID");
        UnionQueryAnalyzer.Result result = uqa.analyze(filter, orderings);
        List<IndexedQueryAnalyzer<Shipment>.Result> subResults = result.getSubResults();

        assertEquals(2, subResults.size());
        IndexedQueryAnalyzer<Shipment>.Result res_0 = subResults.get(0);
        IndexedQueryAnalyzer<Shipment>.Result res_1 = subResults.get(1);

        List<OrderedProperty<Shipment>> handled =
            res_0.getCompositeScore().getOrderingScore().getHandledOrderings();
        assertEquals(1, handled.size());
        assertEquals("+shipmentID", handled.get(0).toString());

        handled = res_1.getCompositeScore().getOrderingScore().getHandledOrderings();
        assertEquals(0, handled.size());

        assertTrue(res_0.handlesAnything());
        assertTrue(res_0.getCompositeScore().getFilteringScore().hasRangeStart());
        assertFalse(res_0.getCompositeScore().getFilteringScore().hasRangeEnd());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), res_0.getLocalIndex());
        assertEquals(null, res_0.getForeignIndex());
        assertEquals(null, res_0.getForeignProperty());
        assertEquals(1, res_0.getRemainderOrderings().size());
        assertEquals("+orderID", res_0.getRemainderOrderings().get(0).toString());

        assertTrue(res_1.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "orderID = ?").bind(),
                     res_1.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "orderID"), res_1.getLocalIndex());
        assertEquals(null, res_1.getForeignIndex());
        assertEquals(null, res_1.getForeignProperty());
        assertEquals(1, res_1.getRemainderOrderings().size());
        assertEquals("+shipmentID", res_1.getRemainderOrderings().get(0).toString());
    }

    public void testSimpleMerge() throws Exception {
        // Because query has an 'or' operation, the analyzer will initially
        // split this into a union. After futher analysis, it should decide
        // that this offers no benefit and will merge them back.
        UnionQueryAnalyzer uqa =
            new UnionQueryAnalyzer(Shipment.class, TestIndexedQueryAnalyzer.IxProvider.INSTANCE);
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
        assertEquals(0, res_0.getRemainderOrderings().size());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentID = ? | orderID = ?"),
                     res_0.getRemainderFilter().unbind());
    }
}
