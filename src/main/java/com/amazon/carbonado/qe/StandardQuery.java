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

import org.cojen.util.BeanPropertyAccessor;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistMultipleException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Query;

import com.amazon.carbonado.filter.ClosedFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.OpenFilter;
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
    // Values for this query, which may be null.
    private final FilterValues<S> mValues;
    // Properties that this query is ordered by.
    private final OrderingList<S> mOrdering;

    private volatile QueryExecutor<S> mExecutor;

    /**
     * @param values optional values object, defaults to open filter if null
     */
    protected StandardQuery(FilterValues<S> values) {
        this(values, null);
    }

    /**
     * @param values optional values object, defaults to open filter if null
     * @param ordering optional order-by properties
     */
    protected StandardQuery(FilterValues<S> values, OrderingList<S> ordering) {
        mValues = values;
        if (ordering == null) {
            ordering = OrderingList.emptyList();
        }
        mOrdering = ordering;
    }

    public Class<S> getStorableType() {
        return getStorage().getStorableType();
    }

    public Filter<S> getFilter() {
        FilterValues<S> values = mValues;
        if (values != null) {
            return values.getFilter();
        }
        return Filter.getOpenFilter(getStorage().getStorableType());
    }

    public FilterValues<S> getFilterValues() {
        return mValues;
    }

    public int getBlankParameterCount() {
        return mValues == null ? 0 : mValues.getBlankParameterCount();
    }

    public Query<S> with(int value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(long value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(float value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(double value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(boolean value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(char value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(byte value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(short value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> with(Object value) {
        return newInstance(requireValues().with(value));
    }

    public Query<S> withValues(Object... values) {
        if (values == null || values.length == 0) {
            return this;
        }
        return newInstance(requireValues().withValues(values));
    }

    public Query<S> and(Filter<S> filter) throws FetchException {
        FilterValues<S> values = mValues;
        Query<S> newQuery;
        if (values == null) {
            newQuery = getStorage().query(filter);
        } else {
            newQuery = getStorage().query(values.getFilter().and(filter));
            newQuery = newQuery.withValues(values.getValues());
        }
        return mOrdering.size() == 0 ? newQuery : newQuery.orderBy(mOrdering.asStringArray());
    }

    public Query<S> or(Filter<S> filter) throws FetchException {
        FilterValues<S> values = mValues;
        if (values == null) {
            throw new IllegalStateException("Query is already guaranteed to fetch everything");
        }
        Query<S> newQuery = getStorage().query(values.getFilter().or(filter));
        newQuery = newQuery.withValues(values.getValues());
        return mOrdering.size() == 0 ? newQuery : newQuery.orderBy(mOrdering.asStringArray());
    }

    public Query<S> not() throws FetchException {
        FilterValues<S> values = mValues;
        if (values == null) {
            return new EmptyQuery<S>(getStorage(), mOrdering);
        }
        Query<S> newQuery = getStorage().query(values.getFilter().not());
        newQuery = newQuery.withValues(values.getSuppliedValues());
        return mOrdering.size() == 0 ? newQuery : newQuery.orderBy(mOrdering.asStringArray());
    }

    public Query<S> orderBy(String property) throws FetchException {
        return newInstance(mValues, OrderingList.get(getStorableType(), property));
    }

    public Query<S> orderBy(String... properties) throws FetchException {
        return newInstance(mValues, OrderingList.get(getStorableType(), properties));
    }

    public Cursor<S> fetch() throws FetchException {
        return executor().fetch(mValues);
    }

    public Cursor<S> fetchAfter(S start) throws FetchException {
        OrderingList<S> orderings;
        if (start == null || (orderings = mOrdering).size() == 0) {
            return fetch();
        }

        Class<S> storableType = getStorage().getStorableType();
        Filter<S> orderFilter = Filter.getClosedFilter(storableType);
        Filter<S> lastSubFilter = Filter.getOpenFilter(storableType);
        BeanPropertyAccessor accessor = BeanPropertyAccessor.forClass(storableType);

        Object[] values = new Object[orderings.size()];

        for (int i=0;;) {
            OrderedProperty<S> property = orderings.get(i);
            RelOp operator = RelOp.GT;
            if (property.getDirection() == Direction.DESCENDING) {
                operator = RelOp.LT;
            }
            String propertyName = property.getChainedProperty().toString();

            values[i] = accessor.getPropertyValue(start, propertyName);

            orderFilter = orderFilter.or(lastSubFilter.and(propertyName, operator));

            if (++i >= orderings.size()) {
                break;
            }

            lastSubFilter = lastSubFilter.and(propertyName, RelOp.EQ);
        }

        Query<S> newQuery = this.and(orderFilter);

        for (int i=0; i<values.length; i++) {
            for (int j=0; j<=i; j++) {
                newQuery = newQuery.with(values[j]);
            }
        }

        return newQuery.fetch();
    }

    public boolean tryDeleteOne() throws PersistException {
        Transaction txn = enterTransactionForDelete(IsolationLevel.READ_COMMITTED);
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

    public void deleteAll() throws PersistException {
        Transaction txn = enterTransactionForDelete(IsolationLevel.READ_COMMITTED);
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

    public long count() throws FetchException {
        return executor().count(mValues);
    }

    public boolean printNative(Appendable app, int indentLevel) throws IOException {
        return executor().printNative(app, indentLevel, mValues);
    }

    public boolean printPlan(Appendable app, int indentLevel) throws IOException {
        return executor().printPlan(app, indentLevel, mValues);
    }

    @Override
    public int hashCode() {
        int hash = getStorage().hashCode() * 31;
        if (mValues != null) {
            hash += mValues.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof StandardQuery) {
            StandardQuery<?> other = (StandardQuery<?>) obj;
            return getStorage().equals(other.getStorage()) &&
                (mValues == null ? (other.mValues == null) : (mValues.equals(other.mValues)));
        }
        return false;
    }

    public void appendTo(Appendable app) throws IOException {
        app.append("Query {type=");
        app.append(getStorableType().getName());
        app.append(", filter=");
        Filter<S> filter = getFilter();
        if (filter instanceof OpenFilter || filter instanceof ClosedFilter) {
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

    /**
     * Return the Storage object that the query is operating on.
     */
    protected abstract Storage<S> getStorage();

    /**
     * Enter a transaction as needed by the standard delete operation, or null
     * if transactions are not supported.
     *
     * @param level minimum desired isolation level
     */
    protected abstract Transaction enterTransactionForDelete(IsolationLevel level);

    /**
     * Return a new or cached executor.
     *
     * @param values optional values object, defaults to open filter if null
     * @param orderings order-by properties, never null
     */
    protected abstract QueryExecutor<S> getExecutor(FilterValues<S> values,
                                                    OrderingList<S> orderings);

    /**
     * Return a new or cached instance of StandardQuery implementation, using
     * new filter values. The Filter in the FilterValues is the same as was
     * passed in the constructor.
     *
     * @param values optional values object, defaults to open filter if null
     * @param orderings order-by properties, never null
     */
    protected abstract StandardQuery<S> newInstance(FilterValues<S> values,
                                                    OrderingList<S> orderings);

    private StandardQuery<S> newInstance(FilterValues<S> values) {
        return newInstance(values, mOrdering);
    }

    private QueryExecutor<S> executor() {
        QueryExecutor<S> executor = mExecutor;
        if (executor == null) {
            mExecutor = executor = getExecutor(mValues, mOrdering);
        }
        return executor;
    }
}
