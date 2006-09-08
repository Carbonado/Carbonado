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
import java.util.List;

import junit.framework.TestSuite;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.repo.toy.ToyRepository;

import com.amazon.carbonado.stored.UserAddress;
import com.amazon.carbonado.stored.UserInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestJoinedQueryExecutor extends TestQueryExecutor {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestJoinedQueryExecutor.class);
    }

    private Repository mRepository;

    protected void setUp() throws Exception {
        super.setUp();
        mRepository = new ToyRepository();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testJoin() throws Exception {
        QueryExecutor<UserAddress> addressExecutor = addressExecutor();

        QueryExecutor<UserInfo> userExecutor = new JoinedQueryExecutor<UserAddress, UserInfo>
            (mRepository, UserInfo.class, "address", addressExecutor);

        assertEquals("address.state = ?", userExecutor.getFilter().toString());
        assertEquals("+address.country", userExecutor.getOrdering().get(0).toString());

        // Create some addresses
        Storage<UserAddress> addressStorage = mRepository.storageFor(UserAddress.class);
        UserAddress addr = addressStorage.prepare();
        addr.setAddressID(1);
        addr.setLine1("4567, 123 Street");
        addr.setCity("Springfield");
        addr.setState("IL");
        addr.setCountry("USA");
        addr.insert();

        addr = addressStorage.prepare();
        addr.setAddressID(2);
        addr.setLine1("1111 Apt 1, 1st Ave");
        addr.setCity("Somewhere");
        addr.setState("AA");
        addr.setCountry("USA");
        addr.setNeighborAddressID(1);
        addr.insert();

        addr = addressStorage.prepare();
        addr.setAddressID(3);
        addr.setLine1("9999");
        addr.setCity("Chicago");
        addr.setState("IL");
        addr.setCountry("USA");
        addr.insert();

        // Create some users
        Storage<UserInfo> userStorage = mRepository.storageFor(UserInfo.class);
        UserInfo user = userStorage.prepare();
        user.setUserID(1);
        user.setStateID(1);
        user.setFirstName("Bob");
        user.setLastName("Loblaw");
        user.setAddressID(1);
        user.insert();

        user = userStorage.prepare();
        user.setUserID(2);
        user.setStateID(1);
        user.setFirstName("Deb");
        user.setLastName("Loblaw");
        user.setAddressID(1);
        user.insert();

        user = userStorage.prepare();
        user.setUserID(3);
        user.setStateID(1);
        user.setFirstName("No");
        user.setLastName("Body");
        user.setAddressID(2);
        user.insert();

        // Now do a basic join, finding everyone in IL.

        FilterValues<UserInfo> values = Filter
            .filterFor(UserInfo.class, "address.state = ?").initialFilterValues().with("IL");

        Cursor<UserInfo> cursor = userExecutor.fetch(values);
        assertTrue(cursor.hasNext());
        assertEquals(1, cursor.next().getUserID());
        assertEquals(2, cursor.next().getUserID());
        assertFalse(cursor.hasNext());
        cursor.close();

        assertEquals(2L, userExecutor.count(values));

        // Now do a multi join, finding everyone with an explicit neighbor in IL.

        userExecutor = new JoinedQueryExecutor<UserAddress, UserInfo>
            (mRepository, UserInfo.class, "address.neighbor", addressExecutor);

        assertEquals("address.neighbor.state = ?", userExecutor.getFilter().toString());
        assertEquals("+address.neighbor.country", userExecutor.getOrdering().get(0).toString());

        values = Filter
            .filterFor(UserInfo.class, "address.neighbor.state = ?")
            .initialFilterValues().with("IL");

        cursor = userExecutor.fetch(values);
        assertTrue(cursor.hasNext());
        assertEquals(3, cursor.next().getUserID());
        assertFalse(cursor.hasNext());
        cursor.close();

        assertEquals(1L, userExecutor.count(values));
        
    }

    protected QueryExecutor<UserAddress> addressExecutor() throws Exception {
        Storage<UserAddress> addressStorage = mRepository.storageFor(UserAddress.class);

        QueryExecutor<UserAddress> addressExecutor =
            new ScanQueryExecutor<UserAddress>(addressStorage.query());

        addressExecutor = new FilteredQueryExecutor<UserAddress>
            (addressExecutor, Filter.filterFor(UserAddress.class, "state = ?"));

        OrderingList<UserAddress> ordering = OrderingList.get(UserAddress.class, "+country");

        addressExecutor =
            new ArraySortedQueryExecutor<UserAddress>(addressExecutor, null, ordering);

        return addressExecutor;
    }

    static class ScanQueryExecutor<S extends Storable> extends FullScanQueryExecutor<S> {
        private final Query<S> mQuery;

        ScanQueryExecutor(Query<S> query) {
            super(query.getStorableType());
            mQuery = query;
        }

        protected Cursor<S> fetch() throws FetchException {
            return mQuery.fetch();
        }
    }
}
