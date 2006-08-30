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

import com.amazon.carbonado.Storable;

/**
 * Filter which lets all results pass through.
 *
 * @author Brian S O'Neill
 */
public class OpenFilter<S extends Storable> extends Filter<S> {
    OpenFilter(Class<S> type) {
        super(type);
    }

    public Filter<S> and(Filter<S> filter) {
        return filter;
    }

    public Filter<S> or(Filter<S> filter) {
        return this;
    }

    public Filter<S> not() {
        return getClosedFilter(getStorableType());
    }

    @Override
    public FilterValues<S> initialFilterValues() {
        return null;
    }

    @Override
    PropertyFilterList<S> getTailPropertyFilterList() {
        return null;
    }

    public <R, P> R accept(Visitor<S, R, P> visitor, P param) {
        return visitor.visit(this, param);
    }

    public Filter<S> bind() {
        return this;
    }

    public boolean isBound() {
        return true;
    }

    void markBound() {
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

    public void appendTo(Appendable app, FilterValues<S> values) throws IOException {
        app.append("open");
    }

    void dumpTree(Appendable app, int indentLevel) throws IOException {
        for (int i=0; i<indentLevel; i++) {
            app.append("  ");
        }
        app.append("open");
    }
}
