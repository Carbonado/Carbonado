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
 * Filter which lets all results pass through.
 *
 * @author Brian S O'Neill
 */
public class OpenFilter<S extends Storable> extends Filter<S> {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    static <S extends Storable> OpenFilter<S> getCanonical(Class<S> type) {
        return (OpenFilter<S>) cCanonical.put(new OpenFilter<S>(type));
    }

    private OpenFilter(Class<S> type) {
        super(type);
    }

    /**
     * Always returns true.
     *
     * @since 1.2
     */
    @Override
    public final boolean isOpen() {
        return true;
    }

    @Override
    public Filter<S> and(Filter<S> filter) {
        return filter;
    }

    @Override
    public OpenFilter<S> or(Filter<S> filter) {
        return this;
    }

    @Override
    public ClosedFilter<S> not() {
        return getClosedFilter(getStorableType());
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
    public OpenFilter<S> bind() {
        return this;
    }

    @Override
    public OpenFilter<S> unbind() {
        return this;
    }

    @Override
    public boolean isBound() {
        return true;
    }

    @Override
    public <T extends Storable> OpenFilter<T> asJoinedFromAny(ChainedProperty<T> joinProperty) {
        return getOpenFilter(joinProperty.getPrimeProperty().getEnclosingType());
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
        if (obj instanceof OpenFilter) {
            OpenFilter<?> other = (OpenFilter<?>) obj;
            return getStorableType() == other.getStorableType();
        }
        return false;
    }

    @Override
    public String toString() {
        return "open";
    }

    @Override
    public void appendTo(Appendable app, FilterValues<S> values) throws IOException {
        app.append("open");
    }
}
