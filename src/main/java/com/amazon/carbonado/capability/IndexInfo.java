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

package com.amazon.carbonado.capability;

import com.amazon.carbonado.info.Direction;

/**
 * General information about an index defined in a {@link com.amazon.carbonado.Storage}.
 *
 * <p>IndexInfo instances are thread-safe and immutable.
 *
 * @author Brian S O'Neill
 * @see IndexInfoCapability
 */
public interface IndexInfo {
    /**
     * Returns the name of this index, or null if not applicable.
     */
    String getName();

    /**
     * Returns true if index entries are unique.
     */
    boolean isUnique();

    /**
     * Returns true if index is clustered, which means it defines the physical
     * ordering of storables.
     */
    boolean isClustered();

    /**
     * Returns the properties in this index. The array might be empty, but it
     * is never null. The array is a copy, and so it may be safely modified.
     */
    String[] getPropertyNames();

    /**
     * Returns the directions of all the properties in this index. The length
     * of the array matches the length returned by {@link
     * #getPropertyNames}. The array is a copy, and so it may be safely
     * modified.
     */
    Direction[] getPropertyDirections();
}
