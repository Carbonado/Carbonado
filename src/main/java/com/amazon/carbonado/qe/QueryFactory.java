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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Produces {@link Query} instances from a query specification.
 *
 * @author Brian S O'Neill
 */
public interface QueryFactory<S extends Storable> {
    Class<S> getStorableType();

    /**
     * Returns a query that handles the given query specification.
     *
     * @param filter optional filter object, defaults to open filter if null
     * @param values optional values object, defaults to filter initial values
     * @param ordering optional order-by properties
     * @param hints optional hints
     */
    Query<S> query(Filter<S> filter, FilterValues<S> values, OrderingList<S> ordering,
                   QueryHints hints)
        throws FetchException;
}
