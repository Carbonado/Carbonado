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

package com.amazon.carbonado.stored;

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Join;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Sequence;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@Alias("TEST_ORDER")
@PrimaryKey("orderID")
public interface Order extends Storable<Order> {
    @Sequence("TEST_ORDER_ID_SEQ")
    long getOrderID();
    void setOrderID(long id);

    String getOrderNumber();
    void setOrderNumber(String value);

    int getOrderTotal();
    void setOrderTotal(int value);

    @Nullable
    String getOrderComments();
    void setOrderComments(String value);

    long getAddressID();
    void setAddressID(long value);

    @Join
    @Nullable
    Address getAddress() throws FetchException;
    void setAddress(Address value);

    @Join
    Query<OrderItem> getOrderItems() throws FetchException;

    @Join
    Query<Shipment> getShipments() throws FetchException;

    @Join
    Query<Promotion> getPromotions() throws FetchException;
}
