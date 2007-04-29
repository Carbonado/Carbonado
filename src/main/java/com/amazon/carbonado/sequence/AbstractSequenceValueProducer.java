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

import java.math.BigInteger;

import com.amazon.carbonado.PersistException;

/**
 *
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public abstract class AbstractSequenceValueProducer implements SequenceValueProducer {
    protected AbstractSequenceValueProducer() {
    }

    public int nextIntValue() throws PersistException {
        return (int) nextLongValue();
    }

    public String nextDecimalValue() throws PersistException {
        return nextNumericalValue(10, 0);
    }

    public String nextNumericalValue(int radix, int minLength) throws PersistException {
        long next = nextLongValue();
        String str;

        if (next >= 0) {
            str = Long.toString(next, radix);
        } else {
            // Use BigInteger to print negative values as positive by expanding
            // precision to 72 bits

            byte[] bytes = new byte[9];
            bytes[8] = (byte) (next & 0xff);
            bytes[7] = (byte) ((next >>= 8) & 0xff);
            bytes[6] = (byte) ((next >>= 8) & 0xff);
            bytes[5] = (byte) ((next >>= 8) & 0xff);
            bytes[4] = (byte) ((next >>= 8) & 0xff);
            bytes[3] = (byte) ((next >>= 8) & 0xff);
            bytes[2] = (byte) ((next >>= 8) & 0xff);
            bytes[1] = (byte) ((next >>= 8) & 0xff);
            //bytes[0] = 0;

            str = new BigInteger(bytes).toString(radix);
        }

        int pad = minLength - str.length();

        if (pad > 0) {
            StringBuilder b = new StringBuilder(minLength);
            while (--pad >= 0) {
                b.append('0');
            }
            b.append(str);
            str = b.toString();
        }

        return str;
    }
}
