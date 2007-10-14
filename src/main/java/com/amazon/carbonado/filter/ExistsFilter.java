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

import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Filter tree node that performs an existence or non-existence test against a
 * one-to-many join.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public class ExistsFilter<S extends Storable> extends Filter<S> {
    /**
     * Returns a canonical instance, creating a new one if there isn't one
     * already in the cache.
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> ExistsFilter<S> getCanonical(ChainedProperty<S> property,
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

    ExistsFilter(ChainedProperty<S> property, Filter<?> subFilter, boolean not) {
        super(property == null ? null : property.getPrimeProperty().getEnclosingType());

        StorableProperty<?> joinProperty = property.getLastProperty();
        if (!joinProperty.isQuery()) {
            throw new IllegalArgumentException("Not a one-to-many join property: " + property);
        }
        if (subFilter == null) {
            subFilter = Filter.getOpenFilter(joinProperty.getJoinedType());
        } else if (subFilter.isClosed()) {
            throw new IllegalArgumentException("Exists sub-filter cannot be closed: " + subFilter);
        } else if (joinProperty.getJoinedType() != subFilter.getStorableType()) {
            throw new IllegalArgumentException
                ("Filter not compatible with join property type: " +
                 property + " joins to a " + joinProperty.getJoinedType().getName() +
                 ", but filter is for a " + subFilter.getStorableType().getName());
        }

        mProperty = property;
        mSubFilter = subFilter;
        mNot = not;
    }

    /**
     * @return chained property whose last property is a one-to-many join
     */
    public ChainedProperty<S> getChainedProperty() {
        return mProperty;
    }

    /**
     * @return filter which is applied to last property of chain, which might be open
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

    public <R, P> R accept(Visitor<S, R, P> visitor, P param) {
        return visitor.visit(this, param);
    }

    public ExistsFilter<S> bind() {
        Filter<?> boundSubFilter = mSubFilter.bind();
        if (boundSubFilter == mSubFilter) {
            return this;
        }
        return getCanonical(mProperty, boundSubFilter, mNot);
    }

    public ExistsFilter<S> unbind() {
        Filter<?> unboundSubFilter = mSubFilter.unbind();
        if (unboundSubFilter == mSubFilter) {
            return this;
        }
        return getCanonical(mProperty, unboundSubFilter, mNot);
    }

    public boolean isBound() {
        return mSubFilter.isBound();
    }

    void markBound() {
    }

    <T extends Storable> ExistsFilter<T> asJoinedFromAny(ChainedProperty<T> joinProperty) {
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

    Filter<S> buildDisjunctiveNormalForm() {
        return this;
    }

    Filter<S> buildConjunctiveNormalForm() {
        return this;
    }

    boolean isDisjunctiveNormalForm() {
        return true;
    }

    boolean isConjunctiveNormalForm() {
        return true;
    }

    boolean isReduced() {
        return true;
    }

    void markReduced() {
    }

    @Override
    public int hashCode() {
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
