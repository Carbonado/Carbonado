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

package com.amazon.carbonado.filter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;

/**
 * Filter which blocks any results from passing through.
 *
 * @author Brian S O'Neill
 */
public class ClosedFilter<S extends Storable> extends Filter<S> {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    static <S extends Storable> ClosedFilter<S> getCanonical(Class<S> type) {
        return (ClosedFilter<S>) cCanonical.put(new ClosedFilter<S>(type));
    }

    private ClosedFilter(Class<S> type) {
        super(type);
    }

    /**
     * Always returns true.
     *
     * @since 1.2
     */
    @Override
    public final boolean isClosed() {
        return true;
    }

    @Override
    public ClosedFilter<S> and(Filter<S> filter) {
        return this;
    }

    @Override
    public Filter<S> or(Filter<S> filter) {
        return filter;
    }

    @Override
    public OpenFilter<S> not() {
        return getOpenFilter(getStorableType());
    }

    /**
     * @since 1.1.1
     */
    @Override
    public List<Filter<S>> disjunctiveNormalFormSplit() {
        // Yes, the Java compiler really wants me to do a useless cast.
        return Collections.singletonList((Filter<S>) this);
    }

    /**
     * @since 1.1.1
     */
    @Override
    public List<Filter<S>> conjunctiveNormalFormSplit() {
        // Yes, the Java compiler really wants me to do a useless cast.
        return Collections.singletonList((Filter<S>) this);
    }

    @Override
    public FilterValues<S> initialFilterValues() {
        return null;
    }

    @Override
    PropertyFilterList<S> getTailPropertyFilterList() {
        return null;
    }

    @Override
    public <R, P> R accept(Visitor<S, R, P> visitor, P param) {
        return visitor.visit(this, param);
    }

    @Override
    public ClosedFilter<S> bind() {
        return this;
    }

    @Override
    public ClosedFilter<S> unbind() {
        return this;
    }

    @Override
    public boolean isBound() {
        return true;
    }

    @Override
    public <T extends Storable> ClosedFilter<T> asJoinedFromAny(ChainedProperty<T> joinProperty) {
        return getClosedFilter(joinProperty.getPrimeProperty().getEnclosingType());
    }

    @Override
    void markBound() {
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
        return getStorableType().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ClosedFilter) {
            ClosedFilter<?> other = (ClosedFilter<?>) obj;
            return getStorableType() == other.getStorableType();
        }
        return false;
    }

    @Override
    public String toString() {
        return "closed";
    }

    @Override
    public void appendTo(Appendable app, FilterValues<S> values) throws IOException {
        app.append("closed");
    }
}
