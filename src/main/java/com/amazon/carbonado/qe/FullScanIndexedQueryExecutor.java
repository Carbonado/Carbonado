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
 * Abstract QueryExecutor which fully scans all Storables of a given type,
 * referencing an index.
 *
 * @author Brian S O'Neill
 */
public abstract class FullScanIndexedQueryExecutor<S extends Storable>
    extends FullScanQueryExecutor<S>
{
    private static <S extends Storable> Class<S> getType(StorableIndex<S> index) {
        if (index == null) {
            throw new IllegalArgumentException();
        }
        return index.getStorableType();
    }

    private final StorableIndex<S> mIndex;

    /**
     * @param index index to use, which may be a primary key index
     * @throws IllegalArgumentException if index is null
     */
    public FullScanIndexedQueryExecutor(StorableIndex<S> index) {
        super(getType(index));
        mIndex = index;
    }

    @Override
    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        return fetch(mIndex);
    }

    /**
     * Returns the natural order of the index.
     */
    @Override
    public OrderingList<S> getOrdering() {
        return OrderingList.get(mIndex.getOrderedProperties());
    }

    protected Cursor<S> fetch() throws FetchException {
        return fetch(mIndex);
    }

    /**
     * Return a new Cursor instance referenced by the given index.
     *
     * @param index index to open, which may be a primary key index
     */
    protected abstract Cursor<S> fetch(StorableIndex<S> index) throws FetchException;
}
