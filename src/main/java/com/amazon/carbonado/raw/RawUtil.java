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

package com.amazon.carbonado.raw;

/**
 * Utilities for manipulating binary data.
 *
 * @author Brian S O'Neill
 */
public class RawUtil {
    /**
     * Adds one to an unsigned integer, represented as a byte array. If
     * overflowed, value in byte array is 0x00, 0x00, 0x00...
     *
     * @param value unsigned integer to increment
     * @return false if overflowed
     */
    public static boolean increment(byte[] value) {
        for (int i=value.length; --i>=0; ) {
            byte newValue = (byte) ((value[i] & 0xff) + 1);
            value[i] = newValue;
            if (newValue != 0) {
                // No carry bit, so done adding.
                return true;
            }
        }
        // This point is reached upon overflow.
        return false;
    }

    /**
     * Subtracts one from an unsigned integer, represented as a byte array. If
     * overflowed, value in byte array is 0xff, 0xff, 0xff...
     *
     * @param value unsigned integer to decrement
     * @return false if overflowed
     */
    public static boolean decrement(byte[] value) {
        for (int i=value.length; --i>=0; ) {
            byte newValue = (byte) ((value[i] & 0xff) + -1);
            value[i] = newValue;
            if (newValue != -1) {
                // No borrow bit, so done subtracting.
                return true;
            }
        }
        // This point is reached upon overflow.
        return false;
    }
}
