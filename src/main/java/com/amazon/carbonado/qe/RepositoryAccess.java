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

package com.amazon.carbonado.qe;

import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

/**
 * Provides internal access to a {@link Repository}, necessary for query
 * execution.
 *
 * @author Brian S O'Neill
 */
public interface RepositoryAccess {
    Repository getRootRepository();

    /**
     * Returns a StorageAccess instance for the given user defined Storable
     * class or interface.
     *
     * @return specific type of StorageAccess instance
     * @throws IllegalArgumentException if specified type is null
     * @throws MalformedTypeException if specified type is not suitable
     */
    <S extends Storable> StorageAccess<S> storageAccessFor(Class<S> type)
        throws SupportException, RepositoryException;
}
