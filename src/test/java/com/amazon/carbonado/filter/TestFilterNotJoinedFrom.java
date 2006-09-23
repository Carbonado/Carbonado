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

package com.amazon.carbonado.filter;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.stored.Address;
import com.amazon.carbonado.stored.Order;
import com.amazon.carbonado.stored.Shipment;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestFilterNotJoinedFrom extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilterNotJoinedFrom.class);
    }

    public TestFilterNotJoinedFrom(String name) {
        super(name);
    }

    public void testOpen() {
        Filter<Shipment> filter = Filter.getOpenFilter(Shipment.class);

        Filter<Shipment>.NotJoined nj = filter.notJoinedFrom("order");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());
    }

    public void testClosed() {
        Filter<Shipment> filter = Filter.getClosedFilter(Shipment.class);

        Filter<Shipment>.NotJoined nj = filter.notJoinedFrom("order");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());
    }

    public void testSimple() {
        Filter<Shipment> filter = Filter.filterFor(Shipment.class, "shipmentNotes != ?");
        filter = filter.bind();

        Filter<Shipment>.NotJoined nj = filter.notJoinedFrom("order");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        filter = Filter.filterFor(Shipment.class, "order.orderTotal < ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.filterFor(Order.class, "orderTotal < ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.getOpenFilter(Shipment.class), nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        filter = Filter.filterFor(Shipment.class, "order.address.addressCity = ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.filterFor(Order.class, "address.addressCity = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.getOpenFilter(Shipment.class), nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertEquals(Filter.filterFor(Address.class, "addressCity = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.getOpenFilter(Shipment.class), nj.getRemainderFilter());

        try {
            nj = filter.notJoinedFrom("order.address.addressCity");
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    public void testAnd() {
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "shipmentNotes != ? & shipmentDate > ?");
        filter = filter.bind();

        Filter<Shipment>.NotJoined nj = filter.notJoinedFrom("order");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        filter = Filter.filterFor
            (Shipment.class,
             "order.orderTotal < ? & shipmentNotes != ? & order.orderComments = ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.filterFor(Order.class, "orderTotal < ? & orderComments = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentNotes != ?").bind(),
                     nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        filter = Filter.filterFor
            (Shipment.class,
             "order.orderTotal < ? & order.address.addressCity != ? " +
             "& order.address.addressZip = ? & shipmentNotes != ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.filterFor(Order.class, "orderTotal < ? & address.addressCity != ? " +
                                      "& address.addressZip = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.filterFor(Shipment.class, "shipmentNotes != ?").bind(),
                     nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertEquals(Filter.filterFor(Address.class, "addressCity != ? & addressZip = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.filterFor
                     (Shipment.class, "order.orderTotal < ? & shipmentNotes != ?").bind(),
                     nj.getRemainderFilter());
    }

    public void testOr() {
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class, "shipmentNotes != ? | shipmentDate > ?");
        filter = filter.bind();

        Filter<Shipment>.NotJoined nj = filter.notJoinedFrom("order");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        filter = Filter.filterFor
            (Shipment.class,
             "order.orderTotal < ? | order.orderComments = ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.filterFor(Order.class, "orderTotal < ? | orderComments = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.getOpenFilter(Shipment.class), nj.getRemainderFilter());

        filter = Filter.filterFor
            (Shipment.class,
             "order.orderTotal < ? | shipmentNotes != ? | order.orderComments = ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.getOpenFilter(Order.class), nj.getNotJoinedFilter());
        assertEquals(filter, nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertTrue(nj.getNotJoinedFilter() instanceof OpenFilter);
        assertEquals(filter, nj.getRemainderFilter());

        filter = Filter.filterFor
            (Shipment.class,
             "order.address.addressCity != ? | order.address.addressZip = ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.filterFor(Order.class,
                                      "address.addressCity != ? | address.addressZip = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.getOpenFilter(Shipment.class), nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertEquals(Filter.filterFor(Address.class, "addressCity != ? | addressZip = ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.getOpenFilter(Shipment.class), nj.getRemainderFilter());

        filter = Filter.filterFor
            (Shipment.class,
             "order.orderTotal < ? | order.address.addressCity != ? " +
             "| order.address.addressZip = ? | shipmentNotes != ?");
        filter = filter.bind();

        nj = filter.notJoinedFrom("order");
        assertEquals(Filter.getOpenFilter(Order.class), nj.getNotJoinedFilter());
        assertEquals(filter, nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertEquals(Filter.getOpenFilter(Address.class), nj.getNotJoinedFilter());
        assertEquals(filter, nj.getRemainderFilter());
    }

    public void testMixed() {
        Filter<Shipment> filter = Filter.filterFor
            (Shipment.class,
             "(order.address.addressCity = ? & order.address.addressZip = ?) " +
             "| order.orderTotal != ?");
        filter = filter.bind();

        Filter<Shipment>.NotJoined nj = filter.notJoinedFrom("order");
        assertEquals(Filter.filterFor
                     (Order.class,
                      "address.addressCity = ? & address.addressZip = ? | orderTotal != ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.getOpenFilter(Shipment.class), nj.getRemainderFilter());

        nj = filter.notJoinedFrom("order.address");
        assertEquals(Filter.getOpenFilter(Address.class), nj.getNotJoinedFilter());
        assertEquals(filter, nj.getRemainderFilter());

        filter = filter.and("order.address.customData > ?");
        filter = filter.bind();
                     
        nj = filter.notJoinedFrom("order.address");
        assertEquals(Filter.filterFor(Address.class, "customData > ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.filterFor
                     (Shipment.class,
                      "(order.address.addressCity = ? | order.orderTotal != ?) " +
                      "& (order.address.addressZip = ? | order.orderTotal != ?)"),
                     nj.getRemainderFilter().unbind());

        filter = filter.disjunctiveNormalForm();

        nj = filter.notJoinedFrom("order.address");
        assertEquals(Filter.filterFor(Address.class, "customData > ?").bind(),
                     nj.getNotJoinedFilter());
        assertEquals(Filter.filterFor
                     (Shipment.class,
                      "order.address.addressCity = ? & order.address.addressZip = ? " +
                      "| order.orderTotal != ?").bind(),
                     nj.getRemainderFilter());
    }
}
