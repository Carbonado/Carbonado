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

import java.util.Collection;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.info.StorableIndex;

/**
 * Provides internal access to a {@link Storage}, necessary for query
 * execution.
 *
 * @author Brian S O'Neill
 */
public interface StorageAccess<S extends Storable>
    extends FullScanQueryExecutor.Support<S>,
            KeyQueryExecutor.Support<S>,
            IndexedQueryExecutor.Support<S>,
            SortedQueryExecutor.Support<S>
{
    /**
     * Returns the specific type of Storable managed by this object.
     */
    Class<S> getStorableType();

    /**
     * Returns a QueryExecutorFactory instance for storage.
     */
    QueryExecutorFactory<S> getQueryExecutorFactory();

    /**
     * Returns all the available indexes.
     */
    Collection<StorableIndex<S>> getAllIndexes();

    /**
     * If the given index is not directly supported by storage, queries should
     * be delegated. Return the storage to delegate to or null if index should
     * not be delegated.
     *
     * @throws IllegalArgumentException if index is unknown
     */
    Storage<S> storageDelegate(StorableIndex<S> index);
}
