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

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

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
public class TestIndexedQueryAnalyzer extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestIndexedQueryAnalyzer.class);
    }

    static <S extends Storable> StorableIndex<S> makeIndex(Class<S> type, String... props) {
        return TestOrderingScore.makeIndex(type, props);
    }

    public TestIndexedQueryAnalyzer(String name) {
        super(name);
    }

    // Note: these tests don't perform exhaustive tests to find the best index, as those tests
    // are performed by TestFilteringScore and TestOrderingScore.

    public void testFullScan() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Address.class, IxProvider.INSTANCE);
        Filter<Address> filter = Filter.filterFor(Address.class, "addressZip = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertFalse(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getRemainderFilter());
        assertEquals(makeIndex(Address.class, "addressID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testIndexScan() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Address.class, IxProvider.INSTANCE);
        Filter<Address> filter = Filter.filterFor(Address.class, "addressID = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Address.class, "addressID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testBasic() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "shipmentID = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());

        filter = Filter.filterFor(Shipment.class, "orderID = ?");
        filter = filter.bind();
        result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "orderID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testSimpleJoin() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "order.orderTotal >= ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertTrue(result.getCompositeScore().getFilteringScore().hasRangeStart());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Order.class, "orderTotal"), result.getForeignIndex());
        assertEquals("order", result.getForeignProperty().toString());
    }

    public void testJoinPriority() throws Exception {
        // Selects foreign index because filter score is better.

        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "shipmentNotes = ? & order.orderTotal >= ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertTrue(result.getCompositeScore().getFilteringScore().hasRangeStart());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentNotes = ?").bind(),
                     result.getCompositeScore().getFilteringScore().getRemainderFilter());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Order.class, "orderTotal"), result.getForeignIndex());
        assertEquals("order", result.getForeignProperty().toString());
    }

    public void testJoinNonPriority() throws Exception {
        // Selects local index because filter score is just as good and local
        // indexes are preferred.

        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "orderID >= ? & order.orderTotal >= ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertTrue(result.getCompositeScore().getFilteringScore().hasRangeStart());
        assertEquals(Filter.filterFor(Shipment.class, "order.orderTotal >= ?").bind(),
                     result.getCompositeScore().getFilteringScore().getRemainderFilter());
        assertEquals(makeIndex(Shipment.class, "orderID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testChainedJoin() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "order.address.addressState = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Address.class, "addressState"), result.getForeignIndex());
        assertEquals("order.address", result.getForeignProperty().toString());
    }

    public void testChainedJoinExecutor() throws Exception {
        Repository repo = new ToyRepository();

        IndexedQueryAnalyzer<Shipment> iqa =
            new IndexedQueryAnalyzer<Shipment>(Shipment.class, IxProvider.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "order.address.addressState = ? & order.address.addressZip = ?");
        FilterValues<Shipment> values = filter.initialFilterValues();
        filter = values.getFilter();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter);

        assertTrue(result.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "order.address.addressState = ?").bind(),
                     result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(Filter.filterFor(Shipment.class, "order.address.addressZip = ?").bind(),
                     result.getCompositeScore().getFilteringScore().getRemainderFilter());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Address.class, "addressState"), result.getForeignIndex());
        assertEquals("order.address", result.getForeignProperty().toString());

        Mock ixExec = new Mock(result.getForeignIndex(), result.getCompositeScore());

        QueryExecutor joinExec = new JoinedQueryExecutor
            (repo, result.getForeignProperty(), ixExec);

        QueryExecutor filteredExec = new FilteredQueryExecutor
            (joinExec, result.getCompositeScore().getFilteringScore().getRemainderFilter());

        //System.out.println();
        //filteredExec.printPlan(System.out, 0, null);

        joinExec.fetch(values.with("WA"));

        assertEquals(1, ixExec.mIdentityValues.length);
        assertEquals("WA", ixExec.mIdentityValues[0]);
        assertEquals(BoundaryType.OPEN, ixExec.mRangeStartBoundary);
        assertEquals(null, ixExec.mRangeStartValue);
        assertEquals(BoundaryType.OPEN, ixExec.mRangeEndBoundary);
        assertEquals(null, ixExec.mRangeEndValue);
        assertFalse(ixExec.mReverseRange);
        assertFalse(ixExec.mReverseOrder);
    }

    static class IxProvider implements IndexProvider {
        static final IxProvider INSTANCE = new IxProvider();

        public <S extends Storable> Collection<StorableIndex<S>> indexesFor(Class<S> type) {
            StorableIndex<S>[] indexes;

            if (Address.class.isAssignableFrom(type)) {
                indexes = new StorableIndex[] {
                    makeIndex(type, "addressID"),
                    makeIndex(type, "addressState")
                };
            } else if (Order.class.isAssignableFrom(type)) {
                indexes = new StorableIndex[] {
                    makeIndex(type, "orderID"),
                    makeIndex(type, "orderTotal"),
                    makeIndex(type, "addressID")
                };
            } else if (Shipment.class.isAssignableFrom(type)) {
                indexes = new StorableIndex[] {
                    makeIndex(type, "shipmentID"),
                    makeIndex(type, "orderID"),
                };
            } else if (Shipper.class.isAssignableFrom(type)) {
                indexes = new StorableIndex[] {
                    makeIndex(type, "shipperID")
                };
            } else {
                indexes = new StorableIndex[0];
            }

            return Arrays.asList(indexes);
        }
    }
}
