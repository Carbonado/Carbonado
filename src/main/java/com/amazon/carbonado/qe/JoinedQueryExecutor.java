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
import com.amazon.carbonado.info.StorableProperty;

/**
 * QueryExecutor which wraps an executor for type <i>source</i>, follows a
 * join, and produces type <i>target</i>.
 *
 * @author Brian S O'Neill
 * @see JoinedCursorFactory
 * @param <S> source type
 * @param <T> target type
 */
public class JoinedQueryExecutor<S extends Storable, T extends Storable>
    extends AbstractQueryExecutor<T>
{
    private static <S extends Storable, T extends Storable> OrderingList<T>
        transformOrdering(Class<T> targetType,
                          String targetToSourceProperty,
                          QueryExecutor<S> sourceExecutor)
    {
        StorableInfo<T> targetInfo = StorableIntrospector.examine(targetType);

        OrderingList<S> sourceOrdering = sourceExecutor.getOrdering();
        int size = sourceOrdering.size();
        OrderedProperty<T>[] targetOrdering = new OrderedProperty[size];

        for (int i=0; i<size; i++) {
            OrderedProperty<S> sourceProp = sourceOrdering.get(i);
            String targetName = targetToSourceProperty + '.' + sourceProp.getChainedProperty();
            OrderedProperty<T> targetProp = OrderedProperty
                .get(ChainedProperty.parse(targetInfo, targetName), sourceProp.getDirection());
            targetOrdering[i] = targetProp;
        }

        return OrderingList.get(targetOrdering);
    }

    private final ChainedProperty<T> mTargetToSourceProperty;
    private final JoinedCursorFactory<S, T> mFactory;
    private final QueryExecutor<S> mSourceExecutor;

    private final FilterValues<S> mSourceFilterValues;
    private final Filter<T> mTargetFilter;
    private final OrderingList<T> mTargetOrdering;

    /**
     * @param repo access to storage instances for properties
     * @param targetType type of <i>target</i> instances
     * @param targetToSourceProperty property of <i>target</i> type which maps
     * to instances of <i>source</i> type.
     * @param sourceExecutor executor for <i>source</i> instances
     * @throws IllegalArgumentException if property type is not <i>source</i>
     */
    public JoinedQueryExecutor(Repository repo,
                               Class<T> targetType,
                               String targetToSourceProperty,
                               QueryExecutor<S> sourceExecutor)
        throws SupportException, FetchException, RepositoryException
    {
        this(repo,
             ChainedProperty.parse(StorableIntrospector.examine(targetType),
                                   targetToSourceProperty),
             sourceExecutor);
    }

    /**
     * @param repo access to storage instances for properties
     * @param targetToSourceProperty property of <i>target</i> type which maps
     * to instances of <i>source</i> type.
     * @param aExecutor executor for <i>A</i> instances
     * @throws IllegalArgumentException if property type is not <i>A</i>
     */
    public JoinedQueryExecutor(Repository repo,
                               ChainedProperty<T> targetToSourceProperty,
                               QueryExecutor<S> sourceExecutor)
        throws SupportException, FetchException, RepositoryException
    {
        mTargetToSourceProperty = targetToSourceProperty;
        mFactory = new JoinedCursorFactory<S, T>
            (repo, targetToSourceProperty, sourceExecutor.getStorableType());
        mSourceExecutor = sourceExecutor;

        Filter<S> sourceFilter = sourceExecutor.getFilter();

        mSourceFilterValues = sourceFilter.initialFilterValues();
        mTargetFilter = sourceFilter.accept(new FilterTransformer(), null);

        mTargetOrdering = transformOrdering
            (targetToSourceProperty.getPrimeProperty().getEnclosingType(),
             targetToSourceProperty.toString(), sourceExecutor);
    }

    public Filter<T> getFilter() {
        return mTargetFilter;
    }

    public Cursor<T> fetch(FilterValues<T> values) throws FetchException {
        return mFactory.join(mSourceExecutor.fetch(transferValues(values)));
    }

    public OrderingList<T> getOrdering() {
        return mTargetOrdering;
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<T> values)
        throws IOException
    {
        int chainCount = mTargetToSourceProperty.getChainCount();

        for (int i = -1; i < chainCount; i++) {
            indent(app, indentLevel);
            app.append("join: ");

            StorableProperty<?> prop;
            if (i == -1) {
                prop = mTargetToSourceProperty.getPrimeProperty();
            } else {
                prop = mTargetToSourceProperty.getChainedProperty(i);
            }

            app.append(prop.getEnclosingType().getName());
            newline(app);
            indent(app, indentLevel);
            app.append("...via property: ");
            app.append(prop.getName());
            newline(app);
            indentLevel = increaseIndent(indentLevel);
        }

        mSourceExecutor.printPlan(app, indentLevel, transferValues(values));
        return true;
    }

    private FilterValues<S> transferValues(FilterValues<T> values) {
        if (values == null || mSourceFilterValues == null) {
            return null;
        }
        return mSourceFilterValues.withValues(values.getSuppliedValues());
    }

    private class FilterTransformer extends Visitor<S, Filter<T>, Object> {
        private final Class<T> mTargetType;

        FilterTransformer() {
            mTargetType = mTargetToSourceProperty.getPrimeProperty().getEnclosingType();
        }

        public Filter<T> visit(OrFilter<S> sourceFilter, Object param) {
            return sourceFilter.getLeftFilter().accept(this, param)
                .and(sourceFilter.getRightFilter().accept(this, param));
        }

        public Filter<T> visit(AndFilter<S> sourceFilter, Object param) {
            return sourceFilter.getLeftFilter().accept(this, param)
                .or(sourceFilter.getRightFilter().accept(this, param));
        }

        public Filter<T> visit(PropertyFilter<S> sourceFilter, Object param) {
            String name;

            ChainedProperty<S> sourceChainedProp = sourceFilter.getChainedProperty();
            if (mTargetType == sourceChainedProp.getPrimeProperty().getEnclosingType()) {
                // If type of S is already T, (which violates generic type
                // signature) then it came from join index analysis.
                name = sourceChainedProp.toString();
            } else {
                StringBuilder nameBuilder = new StringBuilder();
                try {
                    mTargetToSourceProperty.appendTo(nameBuilder);
                    nameBuilder.append('.');
                    sourceChainedProp.appendTo(nameBuilder);
                } catch (IOException e) {
                    // Not gonna happen
                }
                name = nameBuilder.toString();
            }

            Filter<T> targetFilter = Filter.getOpenFilter(mTargetType);
            if (sourceFilter.isConstant()) {
                targetFilter = targetFilter
                    .and(name, sourceFilter.getOperator(), sourceFilter.constant());
            } else {
                targetFilter = targetFilter.and(name, sourceFilter.getOperator());
            }

            return targetFilter;
        }

        public Filter<T> visit(OpenFilter<S> sourceFilter, Object param) {
            return Filter.getOpenFilter(mTargetType);
        }

        public Filter<T> visit(ClosedFilter<S> sourceFilter, Object param) {
            return Filter.getClosedFilter(mTargetType);
        }
    }
}
