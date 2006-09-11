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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;

/**
 * QueryExecutor which fully scans all Storables of a given type, referencing
 * an index.
 *
 * @author Brian S O'Neill
 */
public class FullScanIndexedQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    private final Support<S> mSupport;
    private final StorableIndex<S> mIndex;

    /**
     * @param support support for full scan
     * @param index index to use, which may be a primary key index
     * @throws IllegalArgumentException if support or index is null
     */
    public FullScanIndexedQueryExecutor(Support<S> support, StorableIndex<S> index) {
        if (support == null || index == null) {
            throw new IllegalArgumentException();
        }
        mSupport = support;
        mIndex = index;
    }

    /**
     * Returns an open filter.
     */
    public Filter<S> getFilter() {
        return Filter.getOpenFilter(mIndex.getStorableType());
    }

    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        return mSupport.fetch(mIndex);
    }

    /**
     * Returns the natural order of the index.
     */
    public OrderingList<S> getOrdering() {
        return OrderingList.get(mIndex.getOrderedProperties());
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        indent(app, indentLevel);
        app.append("full index scan: ");
        app.append(mIndex.getStorableType().getName());
        newline(app);
        return true;
    }

    public static interface Support<S extends Storable> {
        /**
         * Perform a full scan of all Storables referenced by an index.
         *
         * @param index index to scan, which may be a primary key index
         */
        Cursor<S> fetch(StorableIndex<S> index) throws FetchException;
    }
}
