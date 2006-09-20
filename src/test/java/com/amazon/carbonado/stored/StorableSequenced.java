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
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Sequence;
import com.amazon.carbonado.Storable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@PrimaryKey("ID")
public interface StorableSequenced extends Storable<StorableSequenced> {
    @Sequence("pk")
    long getID();

    void setID(long id);

    @Sequence("some_int")
    int getSomeInt();

    void setSomeInt(int i);

    @Sequence("some_IntegerObj")
    Integer getSomeIntegerObj();

    void setSomeIntegerObj(Integer i);

    @Sequence("some_long")
    long getSomeLong();

    void setSomeLong(long i);

    @Sequence("some_LongObj")
    @Nullable
    Long getSomeLongObj();

    void setSomeLongObj(Long i);

    @Sequence("some_String")
    String getSomeString();

    void setSomeString(String str);

    String getData();

    void setData(String data);
}
