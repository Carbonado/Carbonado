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

import java.io.IOException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Performs all the actual work of executing a query. QueryExecutors are linked
 * together forming a <i>query plan</i>.
 *
 * @author Brian S O'Neill
 * @see QueryExecutorFactory
 */
public interface QueryExecutor<S extends Storable> {
    /**
     * Returns the storable type that this executor operates on.
     */
    Class<S> getStorableType();

    /**
     * Returns a new cursor using the given filter values.
     */
    Cursor<S> fetch(FilterValues<S> values) throws FetchException;

    /**
     * Returns a new cursor using the given filter values and slice.
     *
     * @since 1.2
     */
    Cursor<S> fetchSlice(FilterValues<S> values, long from, Long to) throws FetchException;

    /**
     * Counts the query results using the given filter values.
     */
    long count(FilterValues<S> values) throws FetchException;

    /**
     * Returns the filter used by this QueryExecutor.
     *
     * @return query filter, never null
     */
    Filter<S> getFilter();

    /**
     * Returns the result ordering of this QueryExecutor.
     *
     * @return query ordering in an unmodifiable list
     */
    OrderingList<S> getOrdering();

    /**
     * Prints the native query to any appendable, if applicable.
     *
     * @param values optional
     * @return false if not implemented
     */
    boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException;

    /**
     * Prints the query plan to any appendable, if applicable.
     *
     * @param values optional
     * @return false if not implemented
     */
    boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException;
}
