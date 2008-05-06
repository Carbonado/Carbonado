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

package com.amazon.carbonado.repo.logging;

import java.io.IOException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 *
 *
 * @author Brian S O'Neill
 */
class LoggingQuery<S extends Storable> implements Query<S> {
    private final LoggingStorage<S> mStorage;
    private final Query<S> mQuery;

    LoggingQuery(LoggingStorage<S> storage, Query<S> query) {
        mStorage = storage;
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

    public Query<S> after(S start) throws FetchException {
        return newInstance(mQuery.after(start));
    }

    public Cursor<S> fetch() throws FetchException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.fetch() on " + this);
        }
        return mQuery.fetch();
    }

    public Cursor<S> fetchSlice(long from, Long to) throws FetchException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.fetchSlice(start, to) on " + this +
                      ", from: " + from + ", to: " + to);
        }
        return mQuery.fetchSlice(from, to);
    }

    public Cursor<S> fetchAfter(S start) throws FetchException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.fetchAfter(start) on " + this + ", start: " + start);
        }
        return mQuery.fetchAfter(start);
    }

    public S loadOne() throws FetchException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.loadOne() on " + this);
        }
        return mQuery.loadOne();
    }

    public S tryLoadOne() throws FetchException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.tryLoadOne() on " + this);
        }
        return mQuery.tryLoadOne();
    }

    public void deleteOne() throws PersistException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.deleteOne() on " + this);
        }
        mQuery.deleteOne();
    }

    public boolean tryDeleteOne() throws PersistException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.tryDeleteOne() on " + this);
        }
        return mQuery.tryDeleteOne();
    }

    public void deleteAll() throws PersistException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.deleteAll() on " + this);
        }
        mQuery.deleteAll();
    }

    public long count() throws FetchException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.count() on " + this);
        }
        return mQuery.count();
    }

    public boolean exists() throws FetchException {
        Log log = mStorage.mLog;
        if (log.isEnabled()) {
            log.write("Query.exists() on " + this);
        }
        return mQuery.exists();
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

    @Override
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
        if (obj instanceof LoggingQuery) {
            LoggingQuery<?> other = (LoggingQuery<?>) obj;
            return mQuery.equals(other.mQuery);
        }
        return false;
    }

    private LoggingQuery<S> newInstance(Query<S> query) {
        return new LoggingQuery<S>(mStorage, query);
    }
}
