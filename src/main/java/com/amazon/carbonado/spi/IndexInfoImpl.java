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

import java.util.Arrays;

import com.amazon.carbonado.capability.IndexInfo;

import com.amazon.carbonado.info.Direction;

/**
 * Basic implementation of an {@link IndexInfo}.
 *
 * @author Brian S O'Neill
 */
public class IndexInfoImpl implements IndexInfo {
    private final String mName;
    private final boolean mUnique;
    private final boolean mClustered;
    private final String[] mPropertyNames;
    private final Direction[] mPropertyDirections;

    /**
     * @param name optional name for index
     * @param unique true if index requires unique values
     * @param propertyNames required list of property names, must have at least
     * one name
     * @param propertyDirections optional property directions, may be null or
     * same length as property names array
     * @throws IllegalArgumentException
     */
    public IndexInfoImpl(String name, boolean unique, boolean clustered,
                         String[] propertyNames, Direction[] propertyDirections) {
        mName = name;
        mUnique = unique;
        mClustered = clustered;

        if (propertyNames == null || propertyNames.length == 0) {
            throw new IllegalArgumentException();
        }

        for (int i=propertyNames.length; --i>=0; ) {
            if (propertyNames[i] == null) {
                throw new IllegalArgumentException();
            }
        }

        propertyNames = propertyNames.clone();

        if (propertyDirections == null) {
            propertyDirections = new Direction[propertyNames.length];
        } else {
            if (propertyNames.length != propertyDirections.length) {
                throw new IllegalArgumentException();
            }
            propertyDirections = propertyDirections.clone();
        }
        for (int i=propertyDirections.length; --i>=0; ) {
            if (propertyDirections[i] == null) {
                propertyDirections[i] = Direction.UNSPECIFIED;
            }
        }

        mPropertyNames = propertyNames;
        mPropertyDirections = propertyDirections;
    }

    public String getName() {
        return mName;
    }

    public boolean isUnique() {
        return mUnique;
    }

    public boolean isClustered() {
        return mClustered;
    }

    public String[] getPropertyNames() {
        return mPropertyNames.clone();
    }

    public Direction[] getPropertyDirections() {
        return mPropertyDirections.clone();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("IndexInfo {name=");
        b.append(mName);
        b.append(", unique=");
        b.append(mUnique);
        b.append(", propertyNames=");
        b.append(Arrays.toString(mPropertyNames));
        b.append(", propertyDirections=");
        b.append(Arrays.toString(mPropertyDirections));
        b.append('}');
        return b.toString();
    }
}
