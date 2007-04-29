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
package com.amazon.carbonado.spi;

import java.io.IOException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.AbstractCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Abstract query that wraps all returned Storables into another Storable.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public abstract class WrappedQuery<S extends Storable> implements Query<S> {

    // The query to which this query will delegate
    private final Query<S> mQuery;

    /**
     * @param query query to wrap
     */
    public WrappedQuery(Query<S> query) {
        mQuery = query;
    }

    public Class<S> getStorableType() {
        return mQuery.getStorableType();
    }

    public Filter<S> getFilter() {
        return mQuery.getFilter();
    }

    public FilterValues<S> getFilterValues() {
        return mQuery.getFilterValues();
    }

    public int getBlankParameterCount() {
        return mQuery.getBlankParameterCount();
    }

    public Query<S> with(int value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(long value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(float value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(double value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(boolean value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(char value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(byte value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(short value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> with(Object value) {
        return newInstance(mQuery.with(value));
    }

    public Query<S> withValues(Object... objects) {
        return newInstance(mQuery.withValues(objects));
    }

    public Query<S> and(String filter) throws FetchException {
        return newInstance(mQuery.and(filter));
    }

    public Query<S> and(Filter<S> filter) throws FetchException {
        return newInstance(mQuery.and(filter));
    }

    public Query<S> or(String filter) throws FetchException {
        return newInstance(mQuery.or(filter));
    }

    public Query<S> or(Filter<S> filter) throws FetchException {
        return newInstance(mQuery.or(filter));
    }

    public Query<S> not() throws FetchException {
        return newInstance(mQuery.not());
    }

    public Query<S> orderBy(String property) throws FetchException, UnsupportedOperationException {
        return newInstance(mQuery.orderBy(property));
    }

    public Query<S> orderBy(String... strings)
        throws FetchException, UnsupportedOperationException
    {
        return newInstance(mQuery.orderBy(strings));
    }

    public Cursor<S> fetch() throws FetchException {
        return new WrappedCursor(mQuery.fetch());
    }

    public Cursor<S> fetchAfter(S start) throws FetchException {
        return new WrappedCursor(mQuery.fetchAfter(start));
    }

    public S loadOne() throws FetchException {
        return wrap(mQuery.loadOne());
    }

    public S tryLoadOne() throws FetchException {
        S one = mQuery.tryLoadOne();
        return one == null ? null : wrap(one);
    }

    public void deleteOne() throws PersistException {
        mQuery.tryDeleteOne();
    }

    public boolean tryDeleteOne() throws PersistException {
        return mQuery.tryDeleteOne();
    }

    public void deleteAll() throws PersistException {
        mQuery.deleteAll();
    }

    public long count() throws FetchException {
        return mQuery.count();
    }

    public boolean printNative() {
        return mQuery.printNative();
    }

    public boolean printNative(Appendable app) throws IOException {
        return mQuery.printNative(app);
    }

    public boolean printNative(Appendable app, int indentLevel) throws IOException {
        return mQuery.printNative(app, indentLevel);
    }

    public boolean printPlan() {
        return mQuery.printPlan();
    }

    public boolean printPlan(Appendable app) throws IOException {
        return mQuery.printPlan(app);
    }

    public boolean printPlan(Appendable app, int indentLevel) throws IOException {
        return mQuery.printPlan(app, indentLevel);
    }

    public void appendTo(Appendable appendable) throws IOException {
        appendable.append(mQuery.toString());
    }

    public String toString() {
        return mQuery.toString();
    }

    @Override
    public int hashCode() {
        return mQuery.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof WrappedQuery) {
            WrappedQuery<?> other = (WrappedQuery<?>) obj;
            return mQuery.equals(other.mQuery);
        }
        return false;
    }

    protected Query<S> getWrappedQuery() {
        return mQuery;
    }

    /**
     * Called to wrap the given Storable.
     */
    protected abstract S wrap(S storable);

    protected abstract WrappedQuery<S> newInstance(Query<S> query);

    private class WrappedCursor extends AbstractCursor<S> {
        private Cursor<S> mCursor;

        public WrappedCursor(Cursor<S> cursor) {
            mCursor = cursor;
        }

        public void close() throws FetchException {
            mCursor.close();
        }

        public boolean hasNext() throws FetchException {
            return mCursor.hasNext();
        }

        public S next() throws FetchException {
            return wrap(mCursor.next());
        }

        public int skipNext(int amount) throws FetchException {
            return mCursor.skipNext(amount);
        }
    }
}
