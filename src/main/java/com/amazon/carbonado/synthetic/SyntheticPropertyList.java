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

package com.amazon.carbonado.synthetic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazon.carbonado.info.Direction;

/**
 * Specification of a collection of properties which will participate in a key
 * or index.  Each property has its own direction specification.
 *
 * @author Brian S O'Neill
 * @author Don Schneider
 */
public abstract class SyntheticPropertyList {
    private List<String> mPropertyList;

    SyntheticPropertyList() {
        mPropertyList = new ArrayList<String>();
    }

    /**
     * Adds a property to this index, with an unspecified direction.
     *
     * @param propertyName name of property to add to index
     */
    public void addProperty(String propertyName) {
        addProperty(propertyName, null);
    }

    /**
     * Adds a property to this index, with the specified direction.
     *
     * @param propertyName name of property to add to index
     * @param direction optional direction of property
     */
    public void addProperty(String propertyName, Direction direction) {
        if (propertyName == null) {
            throw new IllegalArgumentException();
        }
        if (direction == null) {
            direction = Direction.UNSPECIFIED;
        }

        if (direction != Direction.UNSPECIFIED) {
            if (propertyName.length() > 0) {
                if (propertyName.charAt(0) == '-' || propertyName.charAt(0) == '+') {
                    // Overrule the direction.
                    propertyName = propertyName.substring(1);
                }
            }
            propertyName = direction.toCharacter() + propertyName;
        }

        mPropertyList.add(propertyName);
    }

    /**
     * Returns the count of properties in this index.
     */
    public int getPropertyCount() {
        return mPropertyList.size();
    }

    /**
     * Returns all the properties in this index, optionally prefixed with a '+'
     * or '-' to indicate direction.
     */
    public Iterator<String> getProperties() {
        return mPropertyList.iterator();
    }
}
