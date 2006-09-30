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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
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
        StorableInfo<UserInfo> info = StorableIntrospector.examine(UserInfo.class);
        Map<String, ? extends StorableProperty<UserInfo>> properties = info.getAllProperties();

        RepositoryAccess repoAccess = new RepoAccess();

        ChainedProperty<UserInfo> targetToSourceProperty =
            ChainedProperty.get(properties.get("address"));

        Filter<UserInfo> targetFilter = Filter.filterFor(UserInfo.class, "address.state = ?");
        OrderingList<UserInfo> targetOrdering =
            OrderingList.get(UserInfo.class, "+address.country");

        QueryExecutor<UserInfo> userExecutor = JoinedQueryExecutor.build
            (repoAccess, targetToSourceProperty, targetFilter, targetOrdering);

        //System.out.println();
        //userExecutor.printPlan(System.out, 0, null);

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

        targetToSourceProperty = ChainedProperty.parse(info, "address.neighbor");

        targetFilter = Filter.filterFor(UserInfo.class, "address.neighbor.state = ?");
        targetOrdering = OrderingList.get(UserInfo.class, "+address.neighbor.country");

        userExecutor = JoinedQueryExecutor.build
            (repoAccess, targetToSourceProperty, targetFilter, targetOrdering);

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

    class RepoAccess implements RepositoryAccess {
        public Repository getRootRepository() {
            return mRepository;
        }

        public <S extends Storable> StorageAccess<S> storageAccessFor(Class<S> type) {
            return new StoreAccess<S>(type);
        }
    }

    class StoreAccess<S extends Storable> implements StorageAccess<S>, QueryExecutorFactory<S> {
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

        public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering)
            throws RepositoryException
        {
            Storage<S> storage = mRepository.storageFor(mType);

            QueryExecutor<S> exec = new FullScanQueryExecutor<S>
                (new ScanQuerySupport<S>(storage.query()));

            if (filter != null) {
                exec = new FilteredQueryExecutor<S>(exec, filter);
            }

            if (ordering != null && ordering.size() > 0) {
                exec = new SortedQueryExecutor<S>(null, exec, null, ordering);
            }

            return exec;
        }

        public Collection<StorableIndex<S>> getAllIndexes() {
            StorableIndex<S>[] indexes = new StorableIndex[0];
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

    static class ScanQuerySupport<S extends Storable> implements FullScanQueryExecutor.Support<S> {
        private final Query<S> mQuery;

        ScanQuerySupport(Query<S> query) {
            mQuery = query;
        }

        public Class<S> getStorableType() {
            return mQuery.getStorableType();
        }

        public long countAll() throws FetchException {
            return mQuery.count();
        }

        public Cursor<S> fetchAll() throws FetchException {
            return mQuery.fetch();
        }
    }
}
