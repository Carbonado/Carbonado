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

package com.amazon.carbonado.filter;

/**
 * Relational operator enumeration.
 *
 * @author Brian S O'Neill
 */
public enum RelOp {
    /** Equals */
    EQ,
    /** Not Equals */
    NE,
    /** Less Than */
    LT,
    /** Greator than or Equal */
    GE,
    /** Greator Than */
    GT,
    /** Less than or Equal */
    LE;

    /**
     * Returns one of "=", "!=", "<", ">=", ">", or "<=".
     */
    @Override
    public String toString() {
        switch (this) {
        case EQ:
            return "=";
        case NE:
            return "!=";
        case LT:
            return "<";
        case GE:
            return ">=";
        case GT:
            return ">";
        case LE:
            return "<=";
        default:
            return super.toString();
        }
    }

    public RelOp reverse() {
        switch (this) {
        case EQ: default:
            return NE;
        case NE:
            return EQ;
        case LT:
            return GE;
        case GE:
            return LT;
        case GT:
            return LE;
        case LE:
            return GT;
        }
    }
}
