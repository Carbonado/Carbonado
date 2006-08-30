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
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Query;

import com.amazon.carbonado.cursor.EmptyCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

import com.amazon.carbonado.info.OrderedProperty;

/**
 * Special query implementation that fetches nothing.
 *
 * @author Brian S O'Neill
 */
public final class EmptyQuery<S extends Storable> extends AbstractQuery<S> {
    private final Storage<S> mStorage;

    // Properties that this query is ordered by.
    private final String[] mOrderings;

    /**
     * @param storage required storage object
     * @param orderings optional order-by properties
     */
    public EmptyQuery(Storage<S> storage, OrderedProperty[] orderings) {
        if (storage == null) {
            throw new IllegalArgumentException();
        }
        mStorage = storage;
        mOrderings = extractOrderingNames(orderings);
    }

    /**
     * @param storage required storage object
     * @param orderings optional order-by properties
     */
    public EmptyQuery(Storage<S> storage, String[] orderings) {
        if (storage == null) {
            throw new IllegalArgumentException();
        }
        mStorage = storage;
        mOrderings = orderings == null ? EMPTY_ORDERINGS : orderings;
    }

    public Class<S> getStorableType() {
        return mStorage.getStorableType();
    }

    /**
     * Always returns a {@link com.amazon.carbonado.filter.ClosedFilter ClosedFilter}.
     */
    public Filter<S> getFilter() {
        return Filter.getClosedFilter(mStorage.getStorableType());
    }

    /**
     * Always returns null.
     */
    public FilterValues<S> getFilterValues() {
        return null;
    }

    /**
     * Always returns zero.
     */
    public int getBlankParameterCount() {
        return 0;
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(int value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(long value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(float value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(double value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(boolean value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(char value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(byte value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(short value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> with(Object value) {
        throw error();
    }

    /**
     * Throws an IllegalStateException unless no values passed in.
     */
    public Query<S> withValues(Object... values) {
        if (values == null || values.length == 0) {
            return this;
        }
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    public Query<S> and(Filter<S> filter) {
        throw new IllegalStateException("Query is already guaranteed to fetch nothing");
    }

    public Query<S> or(Filter<S> filter) throws FetchException {
        return mStorage.query(filter);
    }

    public Query<S> not() throws FetchException {
        Query<S> query = mStorage.query();
        String[] orderings = mOrderings;
        if (orderings.length > 0) {
            query = query.orderBy(orderings);
        }
        return query;
    }

    public Query<S> orderBy(String property) throws FetchException {
        // This allows property to be checked for validity.
        return mStorage.query().orderBy(property).not();
    }

    public Query<S> orderBy(String... properties) throws FetchException {
        // This allows properties to be checked for validity.
        return mStorage.query().orderBy(properties).not();
    }

    /**
     * Always returns an {@link EmptyCursor}.
     */
    public Cursor<S> fetch() {
        return EmptyCursor.getEmptyCursor();
    }

    /**
     * Always returns an {@link EmptyCursor}.
     */
    public Cursor<S> fetchAfter(S start) {
        return EmptyCursor.getEmptyCursor();
    }

    /**
     * Always throws {@link PersistNoneException}.
     */
    public void deleteOne() throws PersistNoneException {
        throw new PersistNoneException();
    }

    /**
     * Always returns false.
     */
    public boolean tryDeleteOne() {
        return false;
    }

    /**
     * Does nothing.
     */
    public void deleteAll() {
    }

    /**
     * Always returns zero.
     */
    public long count() {
        return 0;
    }

    public void appendTo(Appendable app) throws IOException {
        app.append("Query {type=");
        app.append(mStorage.getStorableType().getName());
        app.append(", filter=");
        getFilter().appendTo(app);

        if (mOrderings != null && mOrderings.length > 0) {
            app.append(", orderBy=[");
            for (int i=0; i<mOrderings.length; i++) {
                if (i > 0) {
                    app.append(", ");
                }
                app.append(mOrderings[i]);
            }
            app.append(']');
        }

        app.append('}');
    }

    /**
     * Always returns false.
     */
    public boolean printNative(Appendable app, int indentLevel) {
        return false;
    }

    /**
     * Always returns false.
     */
    public boolean printPlan(Appendable app, int indentLevel) {
        return false;
    }

    private IllegalStateException error() {
        return new IllegalStateException("Query doesn't have any parameters");
    }
}
