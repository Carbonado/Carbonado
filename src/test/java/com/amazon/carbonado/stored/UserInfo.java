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
import com.amazon.carbonado.constraint.*;

/**
 * 
 *
 * @author Brian S O'Neill (boneill)
 */
@Indexes({
    @Index("firstName"),
    @Index("lastName"),
    @Index("addressID")
})
@PrimaryKey("userID")
public abstract class UserInfo implements Storable<UserInfo> {
    public abstract int getUserID();
    public abstract void setUserID(int id);

    public abstract int getStateID();
    @IntegerConstraint(allowed={1, 2, 3})
    public abstract void setStateID(int state);

    public abstract String getFirstName();
    @LengthConstraint(min=1, max=50)
    public abstract void setFirstName(String value);

    public abstract String getLastName();
    @LengthConstraint(min=1, max=50)
    public abstract void setLastName(String value);

    public abstract int getAddressID();
    public abstract void setAddressID(int id);

    @Join
    public abstract UserAddress getAddress() throws FetchException;
    public abstract void setAddress(UserAddress address);
}
