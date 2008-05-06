/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.jdbc;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.PropertyFilter;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class NullablePropertyStatement<S extends Storable> extends SQLStatement<S> {
    private final PropertyFilter<S> mFilter;
    private final boolean mIsNullOp;

    NullablePropertyStatement(PropertyFilter<S> filter, boolean isNullOp) {
        mFilter = filter;
        mIsNullOp = isNullOp;
    }

    @Override
    public int maxLength() {
        return mIsNullOp ? 8 : 12; // for " IS NULL" or " IS NOT NULL"
    }

    @Override
    public void appendTo(StringBuilder b, FilterValues<S> filterValues) {
        if (filterValues != null
            && filterValues.getValue(mFilter) == null
            && filterValues.isAssigned(mFilter))
        {
            if (mIsNullOp) {
                b.append(" IS NULL");
            } else {
                b.append(" IS NOT NULL");
            }
        } else {
            if (mIsNullOp) {
                b.append("=?");
            } else {
                b.append("<>?");
            }
        }
    }
}
