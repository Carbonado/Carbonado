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

package com.amazon.carbonado.sequence;

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.Authoritative;
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Version;

/**
 * Stores data for {@link SequenceValueGenerator}. To use with JDBC repository,
 * create a table like so:
 *
 * <pre>
 * CREATE TABLE CARBONADO_SEQUENCE (
 *     NAME           VARCHAR(100) PRIMARY KEY,
 *     INITIAL_VALUE  BIGINT       NOT NULL,
 *     NEXT_VALUE     BIGINT       NOT NULL,
 *     VERSION        INT          NOT NULL
 * )
 * </pre>
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
@PrimaryKey("name")
@Authoritative
@Independent
@Alias({
    "CARBONADO_SEQUENCE", "Carbonado_Sequence", "carbonado_sequence",
    "CarbonadoSequence", "carbonadoSequence"
})
public interface StoredSequence extends Storable<StoredSequence> {
    @Alias({"NAME", "Name", "name"})
    String getName();
    void setName(String name);

    /**
     * Returns the initial value for the sequence.
     */
    @Alias({"INITIAL_VALUE", "Initial_Value", "initial_value", "InitialValue", "initialValue"})
    long getInitialValue();
    void setInitialValue(long value);

    /**
     * Returns the pre-adjusted next value of the sequence. This value is
     * initially Long.MIN_VALUE, and it increments up to Long.MAX_VALUE. The actual
     * next value for the sequence is: (getNextValue() + Long.MIN_VALUE + getInitialValue()).
     */
    @Alias({"NEXT_VALUE", "Next_Value", "next_value", "NextValue", "nextValue"})
    long getNextValue();
    void setNextValue(long value);

    @Alias({"VERSION", "Version", "version"})
    @Version
    int getVersion();
    void setVersion(int version);
}
