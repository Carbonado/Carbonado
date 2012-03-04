/*
 * Copyright 2010-2012 Amazon Technologies, Inc. or its affiliates.
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

import java.util.SortedMap;

import com.amazon.carbonado.Repository;

/**
 * @author Vishal Parakh
 * @author Brian S O'Neill
 */ 
public interface GroupMembers {
    /**
     * Returns members intended for read and write access. Keys must uniquely
     * identify group members, must be serializable, and must have appropriate
     * implementations of the hashCode and equals methods.
     */
    SortedMap<Object, Repository> getReadWriteMembers();
}
