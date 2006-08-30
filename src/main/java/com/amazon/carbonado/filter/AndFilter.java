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
 * Filter tree node that performs a logical 'and' test.
 *
 * @author Brian S O'Neill
 */
public class AndFilter<S extends Storable> extends BinaryOpFilter<S> {
    /**
     * Returns a canonical instance.
     *
     * @throws IllegalArgumentException if either filter is null
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> AndFilter<S> getCanonical(Filter<S> left, Filter<S> right) {
        return (AndFilter<S>) cCanonical.put(new AndFilter<S>(left, right));
    }

    /**
     * @throws IllegalArgumentException if either filter is null
     */
    private AndFilter(Filter<S> left, Filter<S> right) {
        super(left, right);
    }

    public Filter<S> not() {
        return mLeft.not().or(mRight.not());
    }

    public <R, P> R accept(Visitor<S, R, P> visitor, P param) {
        return visitor.visit(this, param);
    }

    Filter<S> buildDisjunctiveNormalForm() {
        Filter<S> left = mLeft.reduce().dnf();
        Filter<S> right = mRight.reduce().dnf();
        if (left instanceof OrFilter) {
            return left.accept(new Distributer<S>(true, true), right).reduce().dnf();
        }
        if (right instanceof OrFilter) {
            return right.accept(new Distributer<S>(false, true), left).reduce().dnf();
        }
        return left.and(right).reduce();
    }

    Filter<S> buildConjunctiveNormalForm() {
        return mLeft.cnf().and(mRight.cnf()).reduce();
    }

    boolean checkIsDisjunctiveNormalForm() {
        return (!(mLeft instanceof OrFilter))
            && (!(mRight instanceof OrFilter))
            && mLeft.isDisjunctiveNormalForm()
            && mRight.isDisjunctiveNormalForm();
    }

    boolean checkIsConjunctiveNormalForm() {
        return mLeft.isConjunctiveNormalForm() && mRight.isConjunctiveNormalForm();
    }

    @Override
    public int hashCode() {
        return mLeft.hashCode() * 31 + mRight.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof AndFilter) {
            AndFilter<?> other = (AndFilter<?>) obj;
            return getStorableType() == other.getStorableType()
                && mLeft.equals(other.mLeft) && mRight.equals(other.mRight);
        }
        return false;
    }

    public void appendTo(Appendable app, FilterValues<S> values) throws IOException {
        if (mLeft instanceof OrFilter) {
            app.append('(');
            mLeft.appendTo(app, values);
            app.append(')');
        } else {
            mLeft.appendTo(app, values);
        }
        app.append(" & ");
        if (mRight instanceof OrFilter) {
            app.append('(');
            mRight.appendTo(app, values);
            app.append(')');
        } else {
            mRight.appendTo(app, values);
        }
    }

    void dumpTree(Appendable app, int indentLevel) throws IOException {
        mRight.dumpTree(app, indentLevel + 1);
        for (int i=0; i<indentLevel; i++) {
            app.append("  ");
        }
        app.append("and");
        app.append('\n');
        mLeft.dumpTree(app, indentLevel + 1);
    }
}
