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

import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Version;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("ID")
public interface StorableVersioned extends Storable {
    int getID();

    void setID(int id);

    String getValue();

    void setValue(String value);

    @Nullable
    String getName();

    void setName(String name);

    @Version
    int getVersion();

    void setVersion(int version);
}
