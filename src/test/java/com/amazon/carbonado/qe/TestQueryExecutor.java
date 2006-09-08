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

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.stored.Dummy;
import com.amazon.carbonado.stored.Address;

/**
 *
 *
 * @author Brian S O'Neill
 */
public abstract class TestQueryExecutor extends TestCase {

    protected QueryExecutor<Address> createExecutor(int... ids) {
        return new IterableQueryExecutor<Address>(Address.class, createCollection(ids));
    }

    protected Collection<Address> createCollection(int... ids) {
        Collection<Address> elements = new ArrayList<Address>(ids.length);
        for (int i=0; i<ids.length; i++) {
            elements.add(new DummyAddress(ids[i]));
        }
        return elements;
    }

    protected void compareElements(Cursor<Address> elements, int... expectedIDs)
        throws FetchException
    {
        for (int id : expectedIDs) {
            if (elements.hasNext()) {
                Address e = elements.next();
                if (e.getAddressID() != id) {
                    fail("Element mismatch: expected=" + id + ", actual=" + e.getAddressID());
                    elements.close();
                    return;
                }
            } else {
                fail("Too few elements in cursor");
                return;
            }
        }

        if (elements.hasNext()) {
            Address e = elements.next();
            fail("Too many elements in cursor: " + e.getAddressID());
            elements.close();
        }
    }

    protected OrderingList<Address> createOrdering(String... properties) {
        return OrderingList.get(Address.class, properties);
    }

    static void printPlan(QueryExecutor executor) {
        try {
            executor.printPlan(System.out, 0, null);
        } catch (IOException e) {
        }
    }

    private static class DummyAddress extends Dummy implements Address {
        private long mID;
        private String mLine1;
        private String mLine2;
        private String mCity;
        private String mState;
        private String mZip;
        private String mCountry;
        private String mData;

        DummyAddress(long id) {
            mID = id;
            mLine1 = "line1_" + id;
            mLine2 = "line2_" + id;
            mCity = "city_" + id;
            mState = "state_" + id;
            mZip = "zip_" + id;
            mCountry = "country_" + id;
            mData = "data_" + id;
        }

        public long getAddressID() {
            return mID;
        }

        public void setAddressID(long id) {
            mID = id;
        }

        public String getAddressLine1() {
            return mLine1;
        }

        public void setAddressLine1(String value) {
            mLine1 = value;
        }

        public String getAddressLine2() {
            return mLine2;
        }

        public void setAddressLine2(String value) {
            mLine2 = value;
        }

        public String getAddressCity() {
            return mCity;
        }

        public void setAddressCity(String value) {
            mCity = value;
        }

        public String getAddressState() {
            return mState;
        }

        public void setAddressState(String value) {
            mState = value;
        }

        public String getAddressZip() {
            return mZip;
        }

        public void setAddressZip(String value) {
            mZip = value;
        }

        public String getAddressCountry() {
            return mCountry;
        }

        public void setAddressCountry(String value) {
            mCountry = value;
        }

        public String getCustomData() {
            return mData;
        }

        public void setCustomData(String str) {
            mData = str;
        }

        public String toString() {
            return "address " + mID;
        }
    }
}
