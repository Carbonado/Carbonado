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

/**
 * Abstract QueryExecutor which fully scans all Storables of a given type.
 *
 * @author Brian S O'Neill
 */
public abstract class FullScanQueryExecutor<S extends Storable> extends AbstractQueryExecutor<S> {
    private final Class<S> mType;

    /**
     * @param type type of Storable
     * @throws IllegalArgumentException if type is null
     */
    public FullScanQueryExecutor(Class<S> type) {
        if (type == null) {
            throw new IllegalArgumentException();
        }
        mType = type;
    }

    /**
     * Returns an open filter.
     */
    public Filter<S> getFilter() {
        return Filter.getOpenFilter(mType);
    }

    public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
        return fetch();
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
        app.append("full scan: ");
        app.append(mType.getName());
        newline(app);
        return true;
    }

    /**
     * Return a new Cursor instance.
     */
    protected abstract Cursor<S> fetch() throws FetchException;
}
