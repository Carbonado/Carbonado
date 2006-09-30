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
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.cursor.ArraySortBuffer;
import com.amazon.carbonado.cursor.SortBuffer;

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
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Address.class, RepoAccess.INSTANCE);
        Filter<Address> filter = Filter.filterFor(Address.class, "addressZip = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, null);

        assertFalse(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getRemainderFilter());
        assertEquals(makeIndex(Address.class, "addressID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testIndexScan() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Address.class, RepoAccess.INSTANCE);
        Filter<Address> filter = Filter.filterFor(Address.class, "addressID = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Address.class, "addressID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testBasic() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "shipmentID = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "shipmentID"), result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());

        filter = Filter.filterFor(Shipment.class, "orderID = ?");
        filter = filter.bind();
        result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"),
                     result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());

        filter = Filter.filterFor(Shipment.class, "orderID > ?");
        filter = filter.bind();
        result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertTrue(result.getCompositeScore().getFilteringScore().hasRangeStart());
        assertFalse(result.getCompositeScore().getFilteringScore().hasRangeEnd());
        List<PropertyFilter<Shipment>> rangeFilters =
            result.getCompositeScore().getFilteringScore().getRangeStartFilters();
        assertEquals(1, rangeFilters.size());
        assertEquals(filter, rangeFilters.get(0));
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"),
                     result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testSimpleJoin() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "order.orderTotal >= ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertTrue(result.getCompositeScore().getFilteringScore().hasRangeStart());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Order.class, "orderTotal"), result.getForeignIndex());
        assertEquals("order", result.getForeignProperty().toString());
    }

    public void testJoinPriority() throws Exception {
        // Selects foreign index because filter score is better.

        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "shipmentNotes = ? & order.orderTotal >= ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, null);

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

        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "orderID >= ? & order.orderTotal >= ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertTrue(result.getCompositeScore().getFilteringScore().hasRangeStart());
        assertEquals(Filter.filterFor(Shipment.class, "order.orderTotal >= ?").bind(),
                     result.getCompositeScore().getFilteringScore().getRemainderFilter());
        assertEquals(makeIndex(Shipment.class, "orderID", "shipmentNotes"),
                     result.getLocalIndex());
        assertEquals(null, result.getForeignIndex());
        assertEquals(null, result.getForeignProperty());
    }

    public void testChainedJoin() throws Exception {
        IndexedQueryAnalyzer iqa = new IndexedQueryAnalyzer(Shipment.class, RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "order.address.addressState = ?");
        filter = filter.bind();
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertEquals(filter, result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Address.class, "addressState", "-addressCity"),
                     result.getForeignIndex());
        assertEquals("order.address", result.getForeignProperty().toString());
    }

    public void testChainedJoinExecutor() throws Exception {
        IndexedQueryAnalyzer<Shipment> iqa =
            new IndexedQueryAnalyzer<Shipment>(Shipment.class, RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "order.address.addressState = ? & order.address.addressZip = ?");
        FilterValues<Shipment> values = filter.initialFilterValues();
        filter = values.getFilter();
        IndexedQueryAnalyzer<Shipment>.Result result = iqa.analyze(filter, null);

        assertTrue(result.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "order.address.addressState = ?").bind(),
                     result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(Filter.filterFor(Shipment.class, "order.address.addressZip = ?").bind(),
                     result.getCompositeScore().getFilteringScore().getRemainderFilter());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Address.class, "addressState", "-addressCity"),
                     result.getForeignIndex());
        assertEquals("order.address", result.getForeignProperty().toString());

        QueryExecutor<Shipment> joinExec = JoinedQueryExecutor.build
            (RepoAccess.INSTANCE,
             result.getForeignProperty(), result.getFilter(), result.getOrdering());

        FilterValues<Shipment> fv = values.with("WA").with("12345");

        StringBuffer buf = new StringBuffer();
        joinExec.printPlan(buf, 0, fv);
        String plan = buf.toString();

        // This is actually a pretty terrible plan due to the iterators. This
        // is expected however, since we lied and said we had indexes.
        String expected =
            "join: com.amazon.carbonado.stored.Shipment\n" +
            "...inner loop: order\n" +
            "  filter: orderID = ?\n" +
            "    collection iterator: com.amazon.carbonado.stored.Shipment\n" +
            "...outer loop\n" +
            "  join: com.amazon.carbonado.stored.Order\n" +
            "  ...inner loop: address\n" +
            "    filter: addressID = ?\n" +
            "      collection iterator: com.amazon.carbonado.stored.Order\n" +
            "  ...outer loop\n" +
            "    filter: addressState = WA & addressZip = 12345\n" +
            "      collection iterator: com.amazon.carbonado.stored.Address\n";

        assertEquals(expected, plan);

        joinExec.fetch(fv);

    }
    
    public void testComplexChainedJoinExecutor() throws Exception {
        IndexedQueryAnalyzer<Shipment> iqa =
            new IndexedQueryAnalyzer<Shipment>(Shipment.class, RepoAccess.INSTANCE);
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class,
             "order.address.addressState = ? & order.address.addressID != ? " +
             "& order.address.addressZip = ? & order.orderTotal > ? & shipmentNotes <= ? " +
             "& order.addressID > ?");
        FilterValues<Shipment> values = filter.initialFilterValues();
        filter = values.getFilter();
        OrderingList<Shipment> ordering = OrderingList
            .get(Shipment.class, "order.address.addressCity", "shipmentNotes", "order.orderTotal");
        IndexedQueryAnalyzer.Result result = iqa.analyze(filter, ordering);

        assertTrue(result.handlesAnything());
        assertEquals(Filter.filterFor(Shipment.class, "order.address.addressState = ?").bind(),
                     result.getCompositeScore().getFilteringScore().getIdentityFilter());
        assertEquals(null, result.getLocalIndex());
        assertEquals(makeIndex(Address.class, "addressState", "-addressCity"),
                     result.getForeignIndex());
        assertEquals("order.address", result.getForeignProperty().toString());

        QueryExecutor<Shipment> joinExec = JoinedQueryExecutor.build
            (RepoAccess.INSTANCE,
             result.getForeignProperty(), result.getFilter(), result.getOrdering());

        FilterValues<Shipment> fv =
            values.with("WA").with(45).with("12345").with(100).with("Z").with(2);

        StringBuffer buf = new StringBuffer();
        joinExec.printPlan(buf, 0, fv);
        String plan = buf.toString();

        // This is actually a pretty terrible plan due to the iterators. This
        // is expected however, since we lied and said we had indexes.
        String expected =
            "sort: [+order.address.addressCity, +shipmentNotes], [+order.orderTotal]\n" +
            "  join: com.amazon.carbonado.stored.Shipment\n" +
            "  ...inner loop: order\n" +
            "    sort: [+shipmentNotes]\n" +
            "      filter: shipmentNotes <= Z & orderID = ?\n" +
            "        collection iterator: com.amazon.carbonado.stored.Shipment\n" +
            "  ...outer loop\n" +
            "    join: com.amazon.carbonado.stored.Order\n" +
            "    ...inner loop: address\n" +
            "      filter: orderTotal > 100 & addressID > 2 & addressID = ?\n" +
            "        collection iterator: com.amazon.carbonado.stored.Order\n" +
            "    ...outer loop\n" +
            "      sort: [+addressCity]\n" +
            "        filter: addressState = WA & addressID != 45 & addressZip = 12345\n" +
            "          collection iterator: com.amazon.carbonado.stored.Address\n";

        //System.out.println(plan);
        assertEquals(expected, plan);

        joinExec.fetch(fv);

        // Now do it the easier way and compare plans.
        QueryExecutor<Shipment> joinExec2 = result.createExecutor();

        StringBuffer buf2 = new StringBuffer();
        joinExec2.printPlan(buf2, 0, fv);
        String plan2 = buf2.toString();

        assertEquals(expected, plan2);

        Filter<Shipment> expectedFilter = Filter.filterFor
            (Shipment.class,
             "order.address.addressState = ? & order.address.addressID != ? " +
             "& order.address.addressZip = ? & order.orderTotal > ? " +
             "& order.addressID > ?" +
             "& shipmentNotes <= ? ");

        assertEquals(expectedFilter.disjunctiveNormalForm(),
                     joinExec2.getFilter().unbind().disjunctiveNormalForm());
    }

    static class RepoAccess implements RepositoryAccess {
        static final RepoAccess INSTANCE = new RepoAccess();

        public Repository getRootRepository() {
            throw new UnsupportedOperationException();
        }

        public <S extends Storable> StorageAccess<S> storageAccessFor(Class<S> type) {
            return new StoreAccess<S>(type);
        }
    }

    /**
     * Partially implemented StorageAccess which only supplies information
     * about indexes.
     */
    static class StoreAccess<S extends Storable>
        implements StorageAccess<S>, QueryExecutorFactory<S>
    {
        private final Class<S> mType;

        StoreAccess(Class<S> type) {
            mType = type;
        }

        public Class<S> getStorableType() {
            return mType;
        }

        public QueryExecutorFactory<S> getQueryExecutorFactory() {
            return this;
        }

        public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering) {
            Iterable<S> iterable = Collections.emptyList();

            QueryExecutor<S> exec = new IterableQueryExecutor<S>
                (filter.getStorableType(), iterable);

            if (filter != null) {
                exec = new FilteredQueryExecutor<S>(exec, filter);
            }

            if (ordering != null && ordering.size() > 0) {
                exec = new SortedQueryExecutor<S>(null, exec, null, ordering);
            }

            return exec;
        }

        public Collection<StorableIndex<S>> getAllIndexes() {
            StorableIndex<S>[] indexes;

            if (Address.class.isAssignableFrom(mType)) {
                indexes = new StorableIndex[] {
                    makeIndex(mType, "addressID"),
                    makeIndex(mType, "addressState", "-addressCity")
                };
            } else if (Order.class.isAssignableFrom(mType)) {
                indexes = new StorableIndex[] {
                    makeIndex(mType, "orderID"),
                    makeIndex(mType, "orderTotal"),
                    makeIndex(mType, "addressID", "orderTotal")
                };
            } else if (Shipment.class.isAssignableFrom(mType)) {
                indexes = new StorableIndex[] {
                    makeIndex(mType, "shipmentID"),
                    makeIndex(mType, "orderID", "shipmentNotes"),
                };
            } else if (Shipper.class.isAssignableFrom(mType)) {
                indexes = new StorableIndex[] {
                    makeIndex(mType, "shipperID")
                };
            } else if (StorableTestBasic.class.isAssignableFrom(mType)) {
                indexes = new StorableIndex[] {
                    makeIndex(mType, "id").unique(true).clustered(true),
                    makeIndex(mType, "stringProp", "doubleProp").unique(true),
                    makeIndex(mType, "-stringProp", "-intProp", "~id").unique(true),
                    makeIndex(mType, "+intProp", "stringProp", "~id").unique(true),
                    makeIndex(mType, "-doubleProp", "+longProp", "~id").unique(true),
                };
            } else {
                indexes = new StorableIndex[0];
            }

            return Arrays.asList(indexes);
        }

        public Storage<S> storageDelegate(StorableIndex<S> index) {
            return null;
        }

        public SortBuffer<S> createSortBuffer() {
            return new ArraySortBuffer<S>();
        }

        public long countAll() {
            throw new UnsupportedOperationException();
        }

        public Cursor<S> fetchAll() {
            throw new UnsupportedOperationException();
        }

        public Cursor<S> fetchOne(StorableIndex<S> index, Object[] identityValues) {
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
        }
    }
}
