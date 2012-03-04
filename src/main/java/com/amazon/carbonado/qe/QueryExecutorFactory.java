/*
 * Copyright 2006-2012 Amazon Technologies, Inc. or its affiliates.
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

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;

/**
 * Produces {@link QueryExecutor} instances from a query specification.
 *
 * @author Brian S O'Neill
 */
public interface QueryExecutorFactory<S extends Storable> {
    Class<S> getStorableType();

    /**
     * Returns an executor that handles the given query specification.
     *
     * @param filter optional filter
     * @param ordering optional order-by properties
     * @param hints optional query hints
     */
    QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering, QueryHints hints)
        throws RepositoryException;
}
