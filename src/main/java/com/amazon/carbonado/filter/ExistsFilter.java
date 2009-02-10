/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.filter;

import java.io.IOException;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Filter tree node that performs an existence or non-existence test against a
 * join property.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public class ExistsFilter<S extends Storable> extends Filter<S> {
    private static final long serialVersionUID = 1L;

    /**
     * Returns a canonical instance, creating a new one if there isn't one
     * already in the cache.
     */
    static <S extends Storable> Filter<S> build(ChainedProperty<S> property,
                                                Filter<?> subFilter,
                                                boolean not)
    {
        if (property == null) {
            throw new IllegalArgumentException();
        }

        StorableProperty<?> joinProperty = property.getLastProperty();

        if (subFilter == null) {
            subFilter = Filter.getOpenFilter(joinProperty.getJoinedType());
        } else if (joinProperty.getJoinedType() != subFilter.getStorableType()) {
            throw new IllegalArgumentException
                ("Filter not compatible with join property type: " +
                 property + " joins to a " + joinProperty.getJoinedType().getName() +
                 ", but filter is for a " + subFilter.getStorableType().getName());
        }

        if (subFilter.isClosed()) {
            // Exists filter reduces to a closed (or open) filter.
            Filter<S> f = Filter.getClosedFilter(property.getPrimeProperty().getEnclosingType());
            return not ? f.not() : f;
        } else if (joinProperty.isQuery() || subFilter.isOpen()) {
            return getCanonical(property, subFilter, not);
        } else {
            // Convert to normal join filter.
            return subFilter.asJoinedFrom(property);
        }
    }

    /**
     * Returns a canonical instance, creating a new one if there isn't one
     * already in the cache.
     */
    @SuppressWarnings("unchecked")
    private static <S extends Storable> ExistsFilter<S> getCanonical(ChainedProperty<S> property,
                                                                     Filter<?> subFilter,
                                                                     boolean not)
    {
        return (ExistsFilter<S>) cCanonical.put(new ExistsFilter<S>(property, subFilter, not));
    }

    private final ChainedProperty<S> mProperty;
    private final Filter<?> mSubFilter;
    private final boolean mNot;

    private transient volatile Filter<S> mJoinedSubFilter;
    private transient volatile boolean mNoParameters;

    private ExistsFilter(ChainedProperty<S> property, Filter<?> subFilter, boolean not) {
        super(property.getPrimeProperty().getEnclosingType());
        mProperty = property;
        mSubFilter = subFilter;
        mNot = not;
    }

    /**
     * Returns the join property that is being checked for existence or
     * non-existence. The last property in the chain is a one-to-many or
     * many-to-one join, but it is a many-to-one join only if the sub-filter is
     * also open.
     *
     * @return chained property whose last property is a join
     */
    public ChainedProperty<S> getChainedProperty() {
        return mProperty;
    }

    /**
     * Returns the filter applied to the join, which might be open. For a
     * many-to-one join, the sub-filter is always open.
     *
     * @return filter which is applied to last property of chain
     */
    public Filter<?> getSubFilter() {
        return mSubFilter;
    }

    Filter<S> getJoinedSubFilter() {
        Filter<S> joined = mJoinedSubFilter;
        if (joined == null) {
            mJoinedSubFilter = joined = mSubFilter.asJoinedFromAny(mProperty);
        }
        return joined;
    }

    /**
     * @return true if this filter is testing for "not exists"
     */
    public boolean isNotExists() {
        return mNot;
    }

    @Override
    public Filter<S> not() {
        return getCanonical(mProperty, mSubFilter, !mNot);
    }

    @Override
    public FilterValues<S> initialFilterValues() {
        if (mNoParameters) {
            return null;
        }
        FilterValues<S> filterValues = super.initialFilterValues();
        if (filterValues == null) {
            // Avoid cost of discovering this the next time.
            mNoParameters = true;
        }
        return filterValues;
    }

    @Override
    PropertyFilterList<S> getTailPropertyFilterList() {
        if (mNoParameters) {
            return null;
        }
        PropertyFilterList<S> tail = super.getTailPropertyFilterList();
        if (tail == null) {
            // Avoid cost of discovering this the next time.
            mNoParameters = true;
        }
        return tail;
    }

    @Override
    public <R, P> R accept(Visitor<S, R, P> visitor, P param) {
        return visitor.visit(this, param);
    }

    @Override
    public ExistsFilter<S> bind() {
        Filter<?> boundSubFilter = mSubFilter.bind();
        if (boundSubFilter == mSubFilter) {
            return this;
        }
        return getCanonical(mProperty, boundSubFilter, mNot);
    }

    @Override
    public ExistsFilter<S> unbind() {
        Filter<?> unboundSubFilter = mSubFilter.unbind();
        if (unboundSubFilter == mSubFilter) {
            return this;
        }
        return getCanonical(mProperty, unboundSubFilter, mNot);
    }

    @Override
    public boolean isBound() {
        return mSubFilter.isBound();
    }

    @Override
    void markBound() {
    }

    @Override
    public <T extends Storable> ExistsFilter<T> asJoinedFromAny(ChainedProperty<T> joinProperty) {
        ChainedProperty<T> newProperty = joinProperty.append(getChainedProperty());
        return getCanonical(newProperty, mSubFilter, mNot);
    }

    @Override
    NotJoined notJoinedFromCNF(ChainedProperty<S> joinProperty) {
        ChainedProperty<?> notJoinedProp = getChainedProperty();
        ChainedProperty<?> jp = joinProperty;

        while (notJoinedProp.getPrimeProperty().equals(jp.getPrimeProperty())) {
            notJoinedProp = notJoinedProp.tail();
            if (jp.getChainCount() == 0) {
                jp = null;
                break;
            }
            jp = jp.tail();
        }

        if (jp != null || notJoinedProp.equals(getChainedProperty())) {
            return super.notJoinedFromCNF(joinProperty);
        }

        ExistsFilter<?> notJoinedFilter = getCanonical(notJoinedProp, mSubFilter, mNot);

        return new NotJoined(notJoinedFilter, getOpenFilter(getStorableType()));
    }

    @Override
    Filter<S> buildDisjunctiveNormalForm() {
        return this;
    }

    @Override
    Filter<S> buildConjunctiveNormalForm() {
        return this;
    }

    @Override
    boolean isDisjunctiveNormalForm() {
        return true;
    }

    @Override
    boolean isConjunctiveNormalForm() {
        return true;
    }

    @Override
    boolean isReduced() {
        return true;
    }

    @Override
    void markReduced() {
    }

    @Override
    int generateHashCode() {
        int hash = mProperty.hashCode() * 31 + mSubFilter.hashCode();
        return mNot ? ~hash : hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ExistsFilter) {
            ExistsFilter<?> other = (ExistsFilter<?>) obj;
            return getStorableType() == other.getStorableType()
                && mSubFilter == other.mSubFilter
                && mNot == other.mNot
                && mProperty.equals(other.mProperty);
        }
        return false;
    }

    @Override
    public void appendTo(Appendable app, FilterValues<S> values) throws IOException {
        if (mNot) {
            app.append('!');
        }
        mProperty.appendTo(app);
        app.append('(');

        Filter<?> subFilter = mSubFilter;
        if (subFilter != null && !(subFilter.isOpen())) {
            FilterValues subValues;
            if (values == null) {
                subValues = null;
            } else {
                FilterValues subInitialValues = mSubFilter.initialFilterValues();
                if (subInitialValues == null) {
                    subValues = null;
                } else {
                    subValues = subInitialValues
                        .withValues(values.getSuppliedValuesFor(getJoinedSubFilter()));
                    subFilter = subValues.getFilter();
                }
            }
            subFilter.appendTo(app, subValues);
        }

        app.append(')');
    }
}
