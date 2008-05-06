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

package com.amazon.carbonado.spi;

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.Authoritative;
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;

/**
 * Stores data for SequenceValueGenerator.
 *
 * @author Brian S O'Neill
 * @deprecated Replaced by {@link com.amazon.carbonado.sequence.StoredSequence}
 */
@Deprecated
@PrimaryKey("name")
@Authoritative
@Independent
@Alias("CARBONADO_SEQUENCE")
public interface StoredSequence extends Storable<StoredSequence> {
    String getName();
    void setName(String name);

    /**
     * Returns the initial value for the sequence.
     */
    long getInitialValue();
    void setInitialValue(long value);

    /**
     * Returns the pre-adjusted next value of the sequence. This value is
     * initially Long.MIN_VALUE, and it increments up to Long.MAX_VALUE. The actual
     * next value for the sequence is: (getNextValue() + Long.MIN_VALUE + getInitialValue()).
     */
    long getNextValue();
    void setNextValue(long value);
}
