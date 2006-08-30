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

package com.amazon.carbonado.info;

/**
 * Describes a property sorting direction.
 *
 * @author Brian S O'Neill
 */
public enum Direction {
    ASCENDING('+'), DESCENDING('-'), UNSPECIFIED('~');

    private char mCharValue;

    private Direction(char charValue) {
        mCharValue = charValue;
    }

    /**
     * Returns the reverse direction of this.
     */
    public Direction reverse() {
        if (this == ASCENDING) {
            return DESCENDING;
        } else if (this == DESCENDING) {
            return ASCENDING;
        }
        return this;
    }

    /**
     * Returns '+' for ASCENDING, '-' for DESCENDING, and '~' for UNSPECIFIED.
     */
    public char toCharacter() {
        return mCharValue;
    }

    /**
     * Returns ASCENDING for '+', DESCENDING for '-', UNSPECIFIED for anything
     * else.
     */
    public static Direction fromCharacter(char c) {
        if (c == '+') {
            return ASCENDING;
        }
        if (c == '-') {
            return DESCENDING;
        }
        return UNSPECIFIED;
    }
}
