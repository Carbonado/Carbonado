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
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Sequence;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@Alias("TEST_SHIPPER")
@PrimaryKey("shipperID")
public interface Shipper extends Storable<Shipper> {
    @Sequence("TEST_SHIPPER_ID_SEQ")
    long getShipperID();
    void setShipperID(long id);

    String getShipperName();
    void setShipperName(String value);

    String getShipperDetails();
    void setShipperDetails(String value);

    long getAddressID();
    void setAddressID(long value);

    @Join
    Address getAddress() throws FetchException;
    void setAddress(Address value);
}
