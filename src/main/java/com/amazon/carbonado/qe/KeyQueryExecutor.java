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
 * Abstract QueryExecutor which has a fully specified key, and so cursors
 * produce at most one result.
 *
 * @author Brian S O'Neill
 */
public abstract class KeyQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    private final StorableIndex<S> mIndex;
    private final Filter<S> mKeyFilter;

    /**
     * @param index index to use, which may be a primary key index
     * @param score score determines how best to utilize the index
     * @throws IllegalArgumentException if any parameter is null or if index is
     * not unique or if score is not a key match
     */
    public KeyQueryExecutor(StorableIndex<S> index, FilteringScore<S> score) {
        if (index == null || score == null) {
            throw new IllegalArgumentException();
        }
        if (!index.isUnique() || !score.isKeyMatch()) {
            throw new IllegalArgumentException();
        }
        mIndex = index;
        mKeyFilter = score.getIdentityFilter();
    }

    @Override
    public Class<S> getStorableType() {
        // Storable type of filter may differ if index is used along with a
        // join. The type of the index is the correct storable type.
        return mIndex.getStorableType();
    }

    public Cursor<S> openCursor(FilterValues<S> values) throws FetchException {
        return openCursor(mIndex, values.getValuesFor(mKeyFilter));
    }

    public Filter<S> getFilter() {
        return mKeyFilter;
    }

    /**
     * Returns an empty list.
     */
    public List<OrderedProperty<S>> getOrdering() {
        return Collections.emptyList();
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        indent(app, indentLevel);
        app.append("index key: ");
        app.append(mIndex.getStorableType().getName());
        newline(app);
        indent(app, indentLevel);
        app.append("...index: ");
        mIndex.appendTo(app);
        newline(app);
        indent(app, indentLevel);
        app.append("...key filter: ");
        mKeyFilter.appendTo(app, values);
        newline(app);
        return true;
    }

    /**
     * Return a new Cursor instance referenced by the given index.
     *
     * @param index index to open, which may be a primary key index
     * @param keyValues list of exactly matching values to apply to index
     */
    protected abstract Cursor<S> openCursor(StorableIndex<S> index, Object[] keyValues)
        throws FetchException;
}
