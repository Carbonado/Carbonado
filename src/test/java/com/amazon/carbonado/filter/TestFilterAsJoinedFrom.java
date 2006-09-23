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
public class TestFilterAsJoinedFrom extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestFilterAsJoinedFrom.class);
    }

    public TestFilterAsJoinedFrom(String name) {
        super(name);
    }

    public void testOpen() {
	Filter<Address> filter = Filter.getOpenFilter(Address.class);

	Filter<Order> f2 = filter.asJoinedFrom(Order.class, "address");
	assertTrue(f2 instanceof OpenFilter);
	assertEquals(Order.class, f2.getStorableType());

	Filter<Shipment> f3 = filter.asJoinedFrom(Shipment.class, "order.address");
	assertTrue(f3 instanceof OpenFilter);
	assertEquals(Shipment.class, f3.getStorableType());

	Filter<Shipment> f4 = f2.asJoinedFrom(Shipment.class, "order");
	assertEquals(f3, f4);
	assertTrue(f3 == f4);
    }

    public void testClosed() {
	Filter<Address> filter = Filter.getClosedFilter(Address.class);

	Filter<Order> f2 = filter.asJoinedFrom(Order.class, "address");
	assertTrue(f2 instanceof ClosedFilter);
	assertEquals(Order.class, f2.getStorableType());

	Filter<Shipment> f3 = filter.asJoinedFrom(Shipment.class, "order.address");
	assertTrue(f3 instanceof ClosedFilter);
	assertEquals(Shipment.class, f3.getStorableType());

	Filter<Shipment> f4 = f2.asJoinedFrom(Shipment.class, "order");
	assertEquals(f3, f4);
	assertTrue(f3 == f4);
    }

    public void testSimple() {
	Filter<Address> filter = Filter.filterFor(Address.class, "addressCity = ?");

	Filter<Order> f2 = filter.asJoinedFrom(Order.class, "address");
	assertEquals(f2, Filter.filterFor(Order.class, "address.addressCity = ?"));

	Filter<Shipment> f3 = filter.asJoinedFrom(Shipment.class, "order.address");
	assertEquals(f3, Filter.filterFor(Shipment.class, "order.address.addressCity = ?"));

	Filter<Shipment> f4 = f2.asJoinedFrom(Shipment.class, "order");
	assertEquals(f3, f4);
	assertTrue(f3 == f4);
    }

    public void testAnd() {
	Filter<Address> filter = Filter.filterFor
	    (Address.class, "addressCity = ? & addressZip != ?");

	Filter<Order> f2 = filter.asJoinedFrom(Order.class, "address");
	assertEquals(f2, Filter.filterFor
		     (Order.class, "address.addressCity = ? & address.addressZip != ?"));

	Filter<Shipment> f3 = filter.asJoinedFrom(Shipment.class, "order.address");
	assertEquals(f3, Filter.filterFor
		     (Shipment.class,
		      "order.address.addressCity = ? & order.address.addressZip != ?"));

	Filter<Shipment> f4 = f2.asJoinedFrom(Shipment.class, "order");
	assertEquals(f3, f4);
	assertTrue(f3 == f4);
    }

    public void testOr() {
	Filter<Address> filter = Filter.filterFor
	    (Address.class, "addressCity = ? | addressZip != ?");

	Filter<Order> f2 = filter.asJoinedFrom(Order.class, "address");
	assertEquals(f2, Filter.filterFor
		     (Order.class, "address.addressCity = ? | address.addressZip != ?"));

	Filter<Shipment> f3 = filter.asJoinedFrom(Shipment.class, "order.address");
	assertEquals(f3, Filter.filterFor
		     (Shipment.class,
		      "order.address.addressCity = ? | order.address.addressZip != ?"));

	Filter<Shipment> f4 = f2.asJoinedFrom(Shipment.class, "order");
	assertEquals(f3, f4);
	assertTrue(f3 == f4);
    }

    public void testMixed() {
	Filter<Address> filter = Filter.filterFor
	    (Address.class, "addressCity = ? & addressZip != ? | addressState > ?");

	Filter<Order> f2 = filter.asJoinedFrom(Order.class, "address");
	assertEquals
	    (f2, Filter.filterFor
	     (Order.class,
	      "address.addressCity = ? & address.addressZip != ? | address.addressState > ?"));

	Filter<Shipment> f3 = filter.asJoinedFrom(Shipment.class, "order.address");
	assertEquals(f3, Filter.filterFor
		     (Shipment.class,
		      "order.address.addressCity = ? & order.address.addressZip != ? " +
		      " | order.address.addressState > ?"));
	Filter<Shipment> f4 = f2.asJoinedFrom(Shipment.class, "order");
	assertEquals(f3, f4);
	assertTrue(f3 == f4);
    }

    public void testError() {
	try {
	    Filter<Address> filter = Filter.filterFor(Address.class, "addressCity = ?");
	    Filter<Order> f2 = filter.asJoinedFrom(Order.class, "orderTotal");
	    fail();
	} catch (IllegalArgumentException e) {
	}
    }
}
