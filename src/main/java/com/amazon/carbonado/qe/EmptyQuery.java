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
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Query;

import com.amazon.carbonado.cursor.EmptyCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Special query implementation that fetches nothing.
 *
 * @author Brian S O'Neill
 */
public final class EmptyQuery<S extends Storable> extends AbstractQuery<S> {
    private final QueryFactory<S> mFactory;

    // Properties that this query is ordered by.
    private final OrderingList<S> mOrdering;

    /**
     * @param factory required query factory, used by 'or' and 'not' methods
     * @param ordering optional order-by properties
     */
    public EmptyQuery(QueryFactory<S> factory, OrderingList<S> ordering) {
        if (factory == null) {
            throw new IllegalArgumentException();
        }
        mFactory = factory;
        if (ordering == null) {
            ordering = OrderingList.emptyList();
        }
        mOrdering = ordering;
    }

    /**
     * @param factory required query factory, used by 'or' and 'not' methods
     * @param ordering optional order-by property
     */
    public EmptyQuery(QueryFactory<S> factory, String ordering) {
        this(factory, OrderingList.get(factory.getStorableType(), ordering));
    }

    /**
     * @param factory required query factory, used by 'or' and 'not' methods
     * @param orderings optional order-by properties
     */
    public EmptyQuery(QueryFactory<S> factory, String... orderings) {
        this(factory, OrderingList.get(factory.getStorableType(), orderings));
    }

    /**
     * Used only by test suite. Query throws NullPointerException when invoked.
     */
    EmptyQuery() {
        mFactory = null;
        mOrdering = OrderingList.emptyList();
    }

    @Override
    public Class<S> getStorableType() {
        return mFactory.getStorableType();
    }

    /**
     * Always returns a {@link com.amazon.carbonado.filter.ClosedFilter ClosedFilter}.
     */
    @Override
    public Filter<S> getFilter() {
        return Filter.getClosedFilter(getStorableType());
    }

    /**
     * Always returns null.
     */
    @Override
    public FilterValues<S> getFilterValues() {
        return null;
    }

    /**
     * Always returns zero.
     */
    @Override
    public int getBlankParameterCount() {
        return 0;
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(int value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(long value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(float value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(double value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(boolean value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(char value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(byte value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(short value) {
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> with(Object value) {
        throw error();
    }

    /**
     * Throws an IllegalStateException unless no values passed in.
     */
    @Override
    public Query<S> withValues(Object... values) {
        if (values == null || values.length == 0) {
            return this;
        }
        throw error();
    }

    /**
     * Always throws an IllegalStateException.
     */
    @Override
    public Query<S> and(Filter<S> filter) {
        throw new IllegalStateException("Query is already guaranteed to fetch nothing");
    }

    @Override
    public Query<S> or(Filter<S> filter) throws FetchException {
        return mFactory.query(filter, null, mOrdering, null);
    }

    /**
     * Returns a query that fetches everything, possibly in a specified order.
     */
    @Override
    public Query<S> not() throws FetchException {
        return mFactory.query(null, null, mOrdering, null);
    }

    @Override
    public Query<S> orderBy(String property) throws FetchException {
        return new EmptyQuery<S>(mFactory, property);
    }

    @Override
    public Query<S> orderBy(String... properties) throws FetchException {
        return new EmptyQuery<S>(mFactory, properties);
    }

    @Override
    public <T extends S> Query<S> after(T start) {
        return this;
    }

    /**
     * Always returns an {@link EmptyCursor}.
     */
    @Override
    public Cursor<S> fetch() {
        return EmptyCursor.the();
    }

    /**
     * Always returns an {@link EmptyCursor}.
     */
    @Override
    public Cursor<S> fetchSlice(long from, Long to) {
        checkSliceArguments(from, to);
        return EmptyCursor.the();
    }

    /**
     * Always throws {@link PersistNoneException}.
     */
    @Override
    public void deleteOne() throws PersistNoneException {
        throw new PersistNoneException();
    }

    /**
     * Always returns false.
     */
    @Override
    public boolean tryDeleteOne() {
        return false;
    }

    /**
     * Does nothing.
     */
    @Override
    public void deleteAll() {
    }

    /**
     * Always returns zero.
     */
    @Override
    public long count() {
        return 0;
    }

    /**
     * Always returns false.
     */
    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public void appendTo(Appendable app) throws IOException {
        app.append("Query {type=");
        app.append(getStorableType().getName());
        app.append(", filter=");
        getFilter().appendTo(app);

        if (mOrdering != null && mOrdering.size() > 0) {
            app.append(", orderBy=[");
            for (int i=0; i<mOrdering.size(); i++) {
                if (i > 0) {
                    app.append(", ");
                }
                app.append(mOrdering.get(i).toString());
            }
            app.append(']');
        }

        app.append('}');
    }

    /**
     * Always returns false.
     */
    @Override
    public boolean printNative(Appendable app, int indentLevel) {
        return false;
    }

    /**
     * Always returns false.
     */
    @Override
    public boolean printPlan(Appendable app, int indentLevel) {
        return false;
    }

    @Override
    public int hashCode() {
        return mFactory.hashCode() * 31 + mOrdering.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof EmptyQuery) {
            EmptyQuery<?> other = (EmptyQuery<?>) obj;
            return mFactory.equals(other.mFactory)
                && mOrdering.equals(other.mOrdering);
        }
        return false;
    }

    private IllegalStateException error() {
        return new IllegalStateException("Query doesn't have any parameters");
    }
}
