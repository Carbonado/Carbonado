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

import org.cojen.util.BeanPropertyAccessor;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistMultipleException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.util.Appender;

import com.amazon.carbonado.filter.ClosedFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.OpenFilter;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.OrderedProperty;

import com.amazon.carbonado.qe.AbstractQuery;
import com.amazon.carbonado.qe.EmptyQuery;

/**
 * BaseQuery supports binding filters to values.
 *
 * @author Brian S O'Neill
 * @deprecated Use {@link com.amazon.carbonado.qe.StandardQuery}
 */
public abstract class BaseQuery<S extends Storable> extends AbstractQuery<S> implements Appender {
    /**
     * Appends spaces to the given appendable. Useful for implementing
     * printNative and printPlan.
     */
    public static void indent(Appendable app, int indentLevel) throws IOException {
        for (int i=0; i<indentLevel; i++) {
            app.append(' ');
        }
    }

    protected static final String[] EMPTY_ORDERINGS = {};

    protected static String[] extractOrderingNames(OrderedProperty<?>[] orderings) {
        String[] orderingStrings;
        if (orderings == null || orderings.length == 0) {
            return EMPTY_ORDERINGS;
        }
        orderingStrings = new String[orderings.length];
        for (int i=0; i<orderingStrings.length; i++) {
            orderingStrings[i] = orderings[i].toString().intern();
        }
        return orderingStrings;
    }

    private final Repository mRepository;
    private final Storage<S> mStorage;
    // Values for this query.
    private final FilterValues<S> mValues;
    // Properties that this query is ordered by.
    private final String[] mOrderings;

    // Note: Since constructor has parameters, this class is called Base
    // instead of Abstract.
    /**
     * @param storage required storage object
     * @param values optional values object, defaults to open filter if null
     * @param orderings optional order-by properties
     */
    protected BaseQuery(Repository repo,
                        Storage<S> storage,
                        FilterValues<S> values,
                        OrderedProperty<S>[] orderings)
    {
        if (repo == null || storage == null) {
            throw new IllegalArgumentException();
        }
        mRepository = repo;
        mStorage = storage;
        mValues = values;
        mOrderings = extractOrderingNames(orderings);
    }

    /**
     * @param storage required storage object
     * @param values optional values object, defaults to open filter if null
     * @param orderings optional order-by properties, not cloned
     */
    protected BaseQuery(Repository repo,
                        Storage<S> storage,
                        FilterValues<S> values,
                        String[] orderings)
    {
        if (repo == null || storage == null) {
            throw new IllegalArgumentException();
        }
        mRepository = repo;
        mStorage = storage;
        mValues = values;
        mOrderings = orderings == null ? EMPTY_ORDERINGS : orderings;
    }

    public Class<S> getStorableType() {
        return mStorage.getStorableType();
    }

    public Filter<S> getFilter() {
        FilterValues<S> values = mValues;
        if (values != null) {
            return values.getFilter();
        }
        return Filter.getOpenFilter(mStorage.getStorableType());
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
        FilterValues<S> values = getFilterValues();
        Query<S> newQuery;
        if (values == null) {
            newQuery = mStorage.query(filter);
        } else {
            newQuery = mStorage.query(values.getFilter().and(filter));
            newQuery = newQuery.withValues(values.getValues());
        }
        return mOrderings.length == 0 ? newQuery : newQuery.orderBy(mOrderings);
    }

    public Query<S> or(Filter<S> filter) throws FetchException {
        FilterValues<S> values = getFilterValues();
        if (values == null) {
            throw new IllegalStateException("Query is already guaranteed to fetch everything");
        }
        Query<S> newQuery = mStorage.query(values.getFilter().or(filter));
        newQuery = newQuery.withValues(values.getValues());
        return mOrderings.length == 0 ? newQuery : newQuery.orderBy(mOrderings);
    }

    public Query<S> not() throws FetchException {
        FilterValues<S> values = getFilterValues();
        if (values == null) {
            // FIXME: fix this or remove BaseQuery class.
            throw new UnsupportedOperationException();
            //return new EmptyQuery<S>(mStorage, mOrderings);
        }
        Query<S> newQuery = mStorage.query(values.getFilter().not());
        newQuery = newQuery.withValues(values.getSuppliedValues());
        return mOrderings.length == 0 ? newQuery : newQuery.orderBy(mOrderings);
    }

    public Cursor<S> fetchAfter(S start) throws FetchException {
        String[] orderings;
        if (start == null || (orderings = mOrderings).length == 0) {
            return fetch();
        }

        Class<S> storableType = mStorage.getStorableType();
        Filter<S> orderFilter = Filter.getClosedFilter(storableType);
        Filter<S> lastSubFilter = Filter.getOpenFilter(storableType);
        BeanPropertyAccessor accessor = BeanPropertyAccessor.forClass(storableType);

        Object[] values = new Object[orderings.length];

        for (int i=0;;) {
            String propertyName = orderings[i];
            RelOp operator = RelOp.GT;
            char c = propertyName.charAt(0);
            if (c == '-') {
                propertyName = propertyName.substring(1);
                operator = RelOp.LT;
            } else if (c == '+') {
                propertyName = propertyName.substring(1);
            }

            values[i] = accessor.getPropertyValue(start, propertyName);

            orderFilter = orderFilter.or(lastSubFilter.and(propertyName, operator));

            if (++i >= orderings.length) {
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
        Transaction txn = mRepository.enterTransaction(IsolationLevel.READ_COMMITTED);
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
            txn.commit();
            return result;
        } catch (FetchException e) {
            throw e.toPersistException();
        } finally {
            txn.exit();
        }
    }

    public void deleteAll() throws PersistException {
        Transaction txn = mRepository.enterTransaction(IsolationLevel.READ_COMMITTED);
        try {
            Cursor<S> cursor = fetch();
            try {
                while (cursor.hasNext()) {
                    cursor.next().tryDelete();
                }
            } finally {
                cursor.close();
            }
            txn.commit();
        } catch (FetchException e) {
            throw e.toPersistException();
        } finally {
            txn.exit();
        }
    }

    /**
     * Returns the query ordering properties, never null. The returned array is
     * not cloned, only for performance reasons. Subclasses should not alter it.
     */
    protected String[] getOrderings() {
        return mOrderings;
    }

    protected final Repository getRepository() {
        return mRepository;
    }

    protected final Storage<S> getStorage() {
        return mStorage;
    }

    @Override
    public int hashCode() {
        return mStorage.hashCode() * 31 + getFilterValues().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof BaseQuery) {
            BaseQuery<?> other = (BaseQuery<?>) obj;
            return mStorage.equals(other.mStorage) &&
                getFilterValues().equals(other.getFilterValues());
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
            filter.appendTo(app, getFilterValues());
            app.append('"');
        }

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

    private FilterValues<S> requireValues() {
        FilterValues<S> values = getFilterValues();
        if (values == null) {
            throw new IllegalStateException("Query doesn't have any parameters");
        }
        return values;
    }

    /**
     * Return a new instance of BaseQuery implementation, using new filter
     * values. The Filter in the FilterValues is the same as was passed in the
     * constructor.
     *
     * <p>Any orderings in this query must also be applied in the new
     * query. Call getOrderings to get them.
     *
     * @param values never null
     */
    protected abstract BaseQuery<S> newInstance(FilterValues<S> values);
}
