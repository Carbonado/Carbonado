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

import com.amazon.carbonado.*;

/**
 * 
 *
 * @author Brian S O'Neill (boneill)
 */
@Indexes({
    @Index({"country", "state", "city"}),
    @Index({"state", "city"}),
    @Index("city")
})
@PrimaryKey("addressID")
public abstract class UserAddress implements Storable<UserAddress> {
    public abstract int getAddressID();
    public abstract void setAddressID(int id);

    public abstract String getLine1();
    public abstract void setLine1(String value);

    @Nullable
    public abstract String getLine2();
    public abstract void setLine2(String value);

    public abstract String getCity();
    public abstract void setCity(String value);

    public abstract String getState();
    public abstract void setState(String value);

    public abstract String getCountry();
    public abstract void setCountry(String value);

    @Nullable
    public abstract String getPostalCode();
    public abstract void setPostalCode(String value);

    @Nullable
    public abstract Integer getNeighborAddressID();
    public abstract void setNeighborAddressID(Integer id);

    @Nullable
    @Join(internal="neighborAddressID", external="addressID")
    public abstract UserAddress getNeighbor() throws FetchException;
    public abstract void setNeighbor(UserAddress address) throws FetchException;
}
