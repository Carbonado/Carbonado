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

package com.amazon.carbonado.repo.sleepycat;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.capability.Capability;

/**
 * Capability to compact a BDB database. This capability is not supported by
 * all versions of BDB.
 *
 * @author Brian S O'Neill
 */
public interface CompactionCapability extends Capability {
    /**
     * Compact an entire BDB backed storage. This call may be made within a
     * transaction scope.
     *
     * @param storableType required storable type
     */
    <S extends Storable> Result<S> compact(Class<S> storableType)
        throws RepositoryException;

    public interface Result<S extends Storable> {
        int getPagesExamine();

        int getPagesFree();

        int getPagesTruncated();

        int getLevels();

        int getDeadlockCount();
    }
}
