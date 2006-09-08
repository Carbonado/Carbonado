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
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.cursor.JoinedCursorFactory;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.ClosedFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.OpenFilter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;

/**
 * QueryExecutor which wraps an executor for type <i>A</i>, follows a join, and
 * produces type <i>B</i>.
 *
 * @author Brian S O'Neill
 * @see JoinedCursorFactory
 */
public class JoinedQueryExecutor<A extends Storable, B extends Storable>
    extends AbstractQueryExecutor<B>
{
    private static <A extends Storable, B extends Storable> OrderingList<B>
        transformOrdering(Class<B> bType,
                          String bToAProperty,
                          QueryExecutor<A> aExecutor)
    {
        StorableInfo<B> bInfo = StorableIntrospector.examine(bType);

        OrderingList<A> aOrdering = aExecutor.getOrdering();
        int size = aOrdering.size();
        OrderedProperty<B>[] bOrdering = new OrderedProperty[size];

        for (int i=0; i<size; i++) {
            OrderedProperty<A> aProp = aOrdering.get(i);
            String bName = bToAProperty + '.' + aProp.getChainedProperty();
            OrderedProperty<B> bProp = OrderedProperty
                .get(ChainedProperty.parse(bInfo, bName), aProp.getDirection());
            bOrdering[i] = bProp;
        }

        return OrderingList.get(bOrdering);
    }

    private final JoinedCursorFactory<A, B> mFactory;
    private final QueryExecutor<A> mAExecutor;

    private final FilterValues<A> mAFilterValues;
    private final Filter<B> mBFilter;
    private final OrderingList<B> mBOrdering;

    /**
     * @param repo access to storage instances for properties
     * @param bType type of <i>B</i> instances
     * @param bToAProperty property of <i>B</i> type which maps to instances of
     * <i>A</i> type.
     * @param aExecutor executor for <i>A</i> instances
     * @throws IllegalArgumentException if property type is not <i>A</i>
     */
    public JoinedQueryExecutor(Repository repo,
                               Class<B> bType,
                               String bToAProperty,
                               QueryExecutor<A> aExecutor)
        throws SupportException, FetchException, RepositoryException
    {
        mFactory = new JoinedCursorFactory<A, B>
            (repo, bType, bToAProperty, aExecutor.getStorableType());
        mAExecutor = aExecutor;

        Filter<A> aFilter = aExecutor.getFilter();

        mAFilterValues = aFilter.initialFilterValues();
        mBFilter = aFilter.accept(new FilterTransformer(bType, bToAProperty), null);

        mBOrdering = transformOrdering(bType, bToAProperty, aExecutor);
    }

    /**
     * @param repo access to storage instances for properties
     * @param bToAProperty property of <i>B</i> type which maps to instances of
     * <i>A</i> type.
     * @param aExecutor executor for <i>A</i> instances
     * @throws IllegalArgumentException if property type is not <i>A</i>
     */
    public JoinedQueryExecutor(Repository repo,
                               ChainedProperty<B> bToAProperty,
                               QueryExecutor<A> aExecutor)
        throws SupportException, FetchException, RepositoryException
    {
        mFactory = new JoinedCursorFactory<A, B>
            (repo, bToAProperty, aExecutor.getStorableType());
        mAExecutor = aExecutor;

        Filter<A> aFilter = aExecutor.getFilter();

        mAFilterValues = aFilter.initialFilterValues();
        mBFilter = aFilter.accept(new FilterTransformer(bToAProperty), null);

        mBOrdering = transformOrdering(bToAProperty.getPrimeProperty().getEnclosingType(),
                                       bToAProperty.toString(), aExecutor);
    }

    public Filter<B> getFilter() {
        return mBFilter;
    }

    public Cursor<B> fetch(FilterValues<B> values) throws FetchException {
        return mFactory.join(mAExecutor.fetch(transferValues(values)));
    }

    public OrderingList<B> getOrdering() {
        return mBOrdering;
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<B> values)
        throws IOException
    {
        indent(app, indentLevel);
        app.append("join: ");
        // FIXME: split multi-way join into more nested levels
        app.append(getStorableType().getName());
        newline(app);
        mAExecutor.printPlan(app, increaseIndent(indentLevel), transferValues(values));
        return true;
    }

    private FilterValues<A> transferValues(FilterValues<B> values) {
        if (values == null || mAFilterValues == null) {
            return null;
        }
        return mAFilterValues.withValues(values.getSuppliedValues());
    }

    private class FilterTransformer extends Visitor<A, Filter<B>, Object> {
        private final Class<B> mBType;
        private final String mBToAProperty;

        FilterTransformer(Class<B> bType, String bToAProperty) {
            mBType = bType;
            mBToAProperty = bToAProperty;
        }

        FilterTransformer(ChainedProperty<B> bToAProperty) {
            mBType = bToAProperty.getPrimeProperty().getEnclosingType();
            mBToAProperty = bToAProperty.toString();
        }

        public Filter<B> visit(OrFilter<A> aFilter, Object param) {
            return aFilter.getLeftFilter().accept(this, param)
                .and(aFilter.getRightFilter().accept(this, param));
        }

        public Filter<B> visit(AndFilter<A> aFilter, Object param) {
            return aFilter.getLeftFilter().accept(this, param)
                .or(aFilter.getRightFilter().accept(this, param));
        }

        public Filter<B> visit(PropertyFilter<A> aFilter, Object param) {
            String name;

            ChainedProperty<A> aChainedProp = aFilter.getChainedProperty();
            if (mBType == aChainedProp.getPrimeProperty().getEnclosingType()) {
                // If type if A is already B, (which violates generic type
                // signature) then it came from join index analysis.
                name = aChainedProp.toString();
            } else {
                StringBuilder nameBuilder = new StringBuilder(mBToAProperty).append('.');
                try {
                    aChainedProp.appendTo(nameBuilder);
                } catch (IOException e) {
                    // Not gonna happen
                }
                name = nameBuilder.toString();
            }

            Filter<B> bFilter = Filter.getOpenFilter(mBType);
            if (aFilter.isConstant()) {
                bFilter = bFilter.and(name, aFilter.getOperator(), aFilter.constant());
            } else {
                bFilter = bFilter.and(name, aFilter.getOperator());
            }

            return bFilter;
        }

        public Filter<B> visit(OpenFilter<A> aFilter, Object param) {
            return Filter.getOpenFilter(mBType);
        }

        public Filter<B> visit(ClosedFilter<A> aFilter, Object param) {
            return Filter.getClosedFilter(mBType);
        }
    }
}
