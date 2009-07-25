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
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistMultipleException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Query;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;

import com.amazon.carbonado.util.Appender;

/**
 * Abstract query implementation which uses a {@link QueryExecutor}.
 *
 * @author Brian S O'Neill
 */
public abstract class StandardQuery<S extends Storable> extends AbstractQuery<S>
    implements Appender
{
    // Filter for this query, which may be null.
    private final Filter<S> mFilter;
    // Values for this query, which may be null.
    private final FilterValues<S> mValues;
    // Properties that this query is ordered by.
    private final OrderingList<S> mOrdering;
    // Optional query hints.
    private final QueryHints mHints;

    private volatile QueryExecutor<S> mExecutor;

    /**
     * @param filter optional filter object, defaults to open filter if null
     * @param values optional values object, defaults to filter initial values
     * @param ordering optional order-by properties
     * @param hints optional query hints
     */
    protected StandardQuery(Filter<S> filter,
                            FilterValues<S> values,
                            OrderingList<S> ordering,
                            QueryHints hints)
    {
        if (filter != null && filter.isOpen()) {
            filter = null;
        }

        if (values == null) {
            if (filter != null) {
                values = filter.initialFilterValues();
            }
        }

        mFilter = filter;
        mValues = values;

        if (ordering == null) {
            ordering = OrderingList.emptyList();
        }
        mOrdering = ordering;

        mHints = hints;
    }

    @Override
    public Class<S> getStorableType() {
        return queryFactory().getStorableType();
    }

    @Override
    public Filter<S> getFilter() {
        Filter<S> filter = mFilter;
        if (filter == null) {
            return Filter.getOpenFilter(getStorableType());
        }
        return filter;
    }

    @Override
    public FilterValues<S> getFilterValues() {
        return mValues;
    }

    @Override
    public int getBlankParameterCount() {
        return mValues == null ? 0 : mValues.getBlankParameterCount();
    }

    @Override
    public Query<S> with(int value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(long value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(float value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(double value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(boolean value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(char value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(byte value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(short value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> with(Object value) {
        return newInstance(requireValues().with(value));
    }

    @Override
    public Query<S> withValues(Object... values) {
        if (values == null || values.length == 0) {
            return this;
        }
        return newInstance(requireValues().withValues(values));
    }

    @Override
    public Query<S> and(Filter<S> filter) throws FetchException {
        Filter<S> newFilter;
        FilterValues<S> newValues;
        if (mFilter == null) {
            newFilter = filter;
            newValues = filter.initialFilterValues();
        } else {
            if (getBlankParameterCount() > 0) {
                throw new IllegalStateException("Blank parameters exist in query: " + this);
            }
            newFilter = mFilter.and(filter);
            newValues = newFilter.initialFilterValues();
            if (mValues != null) {
                newValues = newValues.withValues(mValues.getSuppliedValues());
            }
        }
        return createQuery(newFilter, newValues, mOrdering, mHints);
    }

    @Override
    public Query<S> or(Filter<S> filter) throws FetchException {
        if (mFilter == null) {
            throw new IllegalStateException("Query is already guaranteed to fetch everything");
        }
        if (getBlankParameterCount() > 0) {
            throw new IllegalStateException("Blank parameters exist in query: " + this);
        }
        Filter<S> newFilter = mFilter.or(filter);
        FilterValues<S> newValues = newFilter.initialFilterValues();
        if (mValues != null) {
            newValues = newValues.withValues(mValues.getSuppliedValues());
        }
        return createQuery(newFilter, newValues, mOrdering, mHints);
    }

    @Override
    public Query<S> not() throws FetchException {
        if (mFilter == null) {
            return new EmptyQuery<S>(queryFactory(), mOrdering);
        }
        Filter<S> newFilter = mFilter.not();
        FilterValues<S> newValues = newFilter.initialFilterValues();
        if (mValues != null) {
            newValues = newValues.withValues(mValues.getSuppliedValues());
        }
        return createQuery(newFilter, newValues, mOrdering, mHints);
    }

    @Override
    public Query<S> orderBy(String property) throws FetchException {
        return createQuery(mFilter, mValues,
                           OrderingList.get(getStorableType(), property), mHints);
    }

    @Override
    public Query<S> orderBy(String... properties) throws FetchException {
        return createQuery(mFilter, mValues,
                           OrderingList.get(getStorableType(), properties), mHints);
    }

    @Override
    public <T extends S> Query<S> after(T start) throws FetchException {
        OrderingList<S> orderings;
        if (start == null || (orderings = mOrdering).size() == 0) {
            return this;
        }
        return buildAfter(start, orderings);
    }

    private <T extends S> Query<S> buildAfter(T start, OrderingList<S> orderings)
        throws FetchException
    {
        Class<S> storableType = getStorableType();
        Filter<S> orderFilter = Filter.getClosedFilter(storableType);
        Filter<S> openFilter = Filter.getOpenFilter(storableType);
        Filter<S> lastSubFilter = openFilter;

        Object[] values = new Object[orderings.size()];

        for (int i=0;;) {
            OrderedProperty<S> property = orderings.get(i);
            RelOp operator = RelOp.GT;
            if (property.getDirection() == Direction.DESCENDING) {
                operator = RelOp.LT;
            }
            String propertyName = property.getChainedProperty().toString();

            values[i] = start.getPropertyValue(propertyName);

            orderFilter = orderFilter.or(lastSubFilter.and(propertyName, operator));

            if (++i >= orderings.size()) {
                break;
            }

            Filter<S> propFilter = openFilter.and(propertyName, RelOp.EQ).bind();
            lastSubFilter = lastSubFilter.and(propFilter);
        }

        Query<S> query = this.and(orderFilter);

        for (int i=0; i<values.length; i++) {
            for (int j=0; j<=i; j++) {
                query = query.with(values[j]);
            }
        }

        return query;
    }

    @Override
    public Cursor<S> fetch() throws FetchException {
        try {
            return executor().fetch(mValues);
        } catch (RepositoryException e) {
            throw e.toFetchException();
        }
    }

    @Override
    public Cursor<S> fetchSlice(long from, Long to) throws FetchException {
        if (!checkSliceArguments(from, to)) {
            return fetch();
        }
        try {
            QueryHints hints = QueryHints.emptyHints().with(QueryHint.CONSUME_SLICE);
            return executorFactory().executor(mFilter, mOrdering, hints)
                .fetchSlice(mValues, from, to);
        } catch (RepositoryException e) {
            throw e.toFetchException();
        }
    }

    @Override
    public boolean tryDeleteOne() throws PersistException {
        Transaction txn = enterTransaction(IsolationLevel.READ_COMMITTED);
        try {
            Cursor<S> cursor = fetch();
            boolean result;
            try {
                if (cursor.hasNext()) {
                    S obj = cursor.next();
                    if (cursor.hasNext()) {
                        throw new PersistMultipleException(toString());
                    }
                    result = obj.tryDelete();
                } else {
                    return false;
                }
            } finally {
                cursor.close();
            }
            if (txn != null) {
                txn.commit();
            }
            return result;
        } catch (FetchException e) {
            throw e.toPersistException();
        } finally {
            if (txn != null) {
                txn.exit();
            }
        }
    }

    @Override
    public void deleteAll() throws PersistException {
        Transaction txn = enterTransaction(IsolationLevel.READ_COMMITTED);
        try {
            Cursor<S> cursor = fetch();
            try {
                while (cursor.hasNext()) {
                    cursor.next().tryDelete();
                }
            } finally {
                cursor.close();
            }
            if (txn != null) {
                txn.commit();
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        } finally {
            if (txn != null) {
                txn.exit();
            }
        }
    }

    @Override
    public long count() throws FetchException {
        try {
            return executor().count(mValues);
        } catch (RepositoryException e) {
            throw e.toFetchException();
        }
    }

    @Override
    public boolean exists() throws FetchException {
        Cursor<S> cursor = fetchSlice(0L, 1L);
        try {
            return cursor.skipNext(1) > 0;
        } finally {
            cursor.close();
        }
    }

    @Override
    public boolean printNative(Appendable app, int indentLevel) throws IOException {
        try {
            return executor().printNative(app, indentLevel, mValues);
        } catch (RepositoryException e) {
            return false;
        }
    }

    @Override
    public boolean printPlan(Appendable app, int indentLevel) throws IOException {
        try {
            return executor().printPlan(app, indentLevel, mValues);
        } catch (RepositoryException e) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = queryFactory().hashCode();
        hash = hash * 31 + executorFactory().hashCode();
        if (mFilter != null) {
            hash = hash * 31 + mFilter.hashCode();
        }
        if (mValues != null) {
            hash = hash * 31 + mValues.hashCode();
        }
        hash = hash * 31 + mOrdering.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof StandardQuery) {
            StandardQuery<?> other = (StandardQuery<?>) obj;
            return queryFactory().equals(other.queryFactory())
                && executorFactory().equals(other.executorFactory())
                && (mFilter == null ? (other.mFilter == null) : (mFilter.equals(other.mFilter)))
                && (mValues == null ? (other.mValues == null) : (mValues.equals(other.mValues)))
                && mOrdering.equals(other.mOrdering);
        }
        return false;
    }

    @Override
    public void appendTo(Appendable app) throws IOException {
        app.append("Query {type=");
        app.append(getStorableType().getName());
        app.append(", filter=");
        Filter<S> filter = getFilter();
        if (filter.isOpen() || filter.isClosed()) {
            filter.appendTo(app);
        } else {
            app.append('"');
            filter.appendTo(app, mValues);
            app.append('"');
        }

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

    private FilterValues<S> requireValues() {
        FilterValues<S> values = mValues;
        if (values == null) {
            throw new IllegalStateException("Query doesn't have any parameters");
        }
        return values;
    }

    protected OrderingList<S> getOrdering() {
        return mOrdering;
    }

    /**
     * Returns the executor in use by this query.
     */
    protected QueryExecutor<S> executor() throws RepositoryException {
        QueryExecutor<S> executor = mExecutor;
        if (executor == null) {
            mExecutor = executor = executorFactory().executor(mFilter, mOrdering, null);
        }
        return executor;
    }

    /**
     * Ensures that a cached query executor reference is available. If not, the
     * query executor factory is called and the executor is cached.
     */
    protected void setExecutor() throws RepositoryException {
        executor();
    }

    /**
     * Resets any cached reference to a query executor. If a reference is
     * available, it is replaced, but a clear reference is not set.
     */
    protected void resetExecutor() throws RepositoryException {
        if (mExecutor != null) {
            mExecutor = executorFactory().executor(mFilter, mOrdering, null);
        }
    }

    /**
     * Clears any cached reference to a query executor. The next time this
     * Query is used, it will get an executor from the query executor factory
     * and cache a reference to it.
     */
    protected void clearExecutor() {
        mExecutor = null;
    }

    /**
     * Enter a transaction as needed by the standard delete operation, or null
     * if transactions are not supported.
     *
     * @param level minimum desired isolation level
     */
    protected abstract Transaction enterTransaction(IsolationLevel level);

    /**
     * Return a QueryFactory which is used to form new queries from this one.
     */
    protected abstract QueryFactory<S> queryFactory();

    /**
     * Return a QueryExecutorFactory which is used to get an executor.
     */
    protected abstract QueryExecutorFactory<S> executorFactory();

    /**
     * Return a new or cached instance of StandardQuery implementation, using
     * new filter values. The Filter in the FilterValues is the same as was
     * passed in the constructor.
     *
     * @param values non-null values object
     * @param ordering order-by properties, never null
     */
    protected abstract StandardQuery<S> newInstance(FilterValues<S> values,
                                                    OrderingList<S> ordering,
                                                    QueryHints hints);

    private StandardQuery<S> newInstance(FilterValues<S> values) {
        StandardQuery<S> query = newInstance(values, mOrdering, mHints);
        query.mExecutor = this.mExecutor;
        return query;
    }

    private Query<S> createQuery(Filter<S> filter,
                                 FilterValues<S> values,
                                 OrderingList<S> ordering,
                                 QueryHints hints)
        throws FetchException
    {
        return queryFactory().query(filter, values, ordering, hints);
    }
}
