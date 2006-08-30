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
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Sequence;

/**
 *
 *
 * @author Brian S O'Neill
 */
@Alias("TEST_ADDRESS")
@PrimaryKey("addressID")
public interface Address extends Storable {
    @Sequence("TEST_ADDRESS_ID_SEQ")
    long getAddressID();
    void setAddressID(long id);

    String getAddressLine1();
    void setAddressLine1(String value);

    @Nullable
    String getAddressLine2();
    void setAddressLine2(String value);

    String getAddressCity();
    void setAddressCity(String value);

    @Nullable
    String getAddressState();
    void setAddressState(String value);

    String getAddressZip();
    void setAddressZip(String value);

    String getAddressCountry();
    void setAddressCountry(String value);

    @Independent
    String getCustomData();
    void setCustomData(String str);
}

