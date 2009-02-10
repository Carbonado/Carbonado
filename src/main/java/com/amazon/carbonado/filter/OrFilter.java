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

import com.amazon.carbonado.info.ChainedProperty;

/**
 * Filter tree node that performs a logical 'or' test.
 *
 * @author Brian S O'Neill
 */
public class OrFilter<S extends Storable> extends BinaryOpFilter<S> {
    private static final long serialVersionUID = 1L;

    /**
     * Returns a canonical instance.
     *
     * @throws IllegalArgumentException if either filter is null
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> OrFilter<S> getCanonical(Filter<S> left, Filter<S> right) {
        return (OrFilter<S>) cCanonical.put(new OrFilter<S>(left, right));
    }

    /**
     * @throws IllegalArgumentException if either filter is null
     */
    private OrFilter(Filter<S> left, Filter<S> right) {
        super(left, right);
    }

    @Override
    public Filter<S> not() {
        return mLeft.not().and(mRight.not());
    }

    @Override
    public <R, P> R accept(Visitor<S, R, P> visitor, P param) {
        return visitor.visit(this, param);
    }

    @Override
    public Filter<S> unbind() {
        if (!isBound()) {
            return this;
        }
        return mLeft.unbind().or(mRight.unbind());
    }

    @Override
    public <T extends Storable> Filter<T> asJoinedFromAny(ChainedProperty<T> joinProperty) {
        return mLeft.asJoinedFromAny(joinProperty).or(mRight.asJoinedFromAny(joinProperty));
    }

    @Override
    NotJoined notJoinedFromCNF(ChainedProperty<S> joinProperty) {
        NotJoined left = mLeft.notJoinedFromCNF(joinProperty);
        NotJoined right = mRight.notJoinedFromCNF(joinProperty);

        // Assert that our child nodes are only OrFilter or PropertyFilter.
        if (!isConjunctiveNormalForm()) {
            throw new IllegalStateException();
        }

        // If child nodes have any remainder, then everything must go to the
        // remainder. As per the contract of notJoinedFrom, the not-joined and
        // remainder filters are logically and'd together to reform the
        // original filter. If the remainder was broken up, then the not-joined
        // and remainder filters would need to logically or'd together to
        // reform the original filter, breaking the notJoinedFrom contract.

        if (!(left.getRemainderFilter().isOpen()) || !(right.getRemainderFilter().isOpen())) {
            return super.notJoinedFromCNF(joinProperty);
        }

        // Remove wildcards to shut the compiler up.
        Filter leftNotJoined = left.getNotJoinedFilter();
        Filter rightNotJoined = right.getNotJoinedFilter();

        return new NotJoined(leftNotJoined.or(rightNotJoined), getOpenFilter(getStorableType()));
    }

    @Override
    Filter<S> buildDisjunctiveNormalForm() {
        return mLeft.dnf().or(mRight.dnf()).reduce();
    }

    @Override
    Filter<S> buildConjunctiveNormalForm() {
        Filter<S> left = mLeft.reduce().cnf();
        Filter<S> right = mRight.reduce().cnf();
        if (left instanceof AndFilter) {
            return left.accept(new Distributer<S>(true, false), right).reduce().cnf();
        }
        if (right instanceof AndFilter) {
            return right.accept(new Distributer<S>(false, false), left).reduce().cnf();
        }
        return left.or(right).reduce();
    }

    @Override
    boolean checkIsDisjunctiveNormalForm() {
        return mLeft.isDisjunctiveNormalForm() && mRight.isDisjunctiveNormalForm();
    }

    @Override
    boolean checkIsConjunctiveNormalForm() {
        return (!(mLeft instanceof AndFilter))
            && (!(mRight instanceof AndFilter))
            && mLeft.isConjunctiveNormalForm()
            && mRight.isConjunctiveNormalForm();
    }

    @Override
    int generateHashCode() {
        return mLeft.hashCode() * 31 + mRight.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof OrFilter) {
            OrFilter<?> other = (OrFilter<?>) obj;
            return getStorableType() == other.getStorableType()
                && mLeft.equals(other.mLeft) && mRight.equals(other.mRight);
        }
        return false;
    }

    @Override
    public void appendTo(Appendable app, FilterValues<S> values) throws IOException {
        if (mLeft instanceof AndFilter) {
            app.append('(');
            mLeft.appendTo(app, values);
            app.append(')');
        } else {
            mLeft.appendTo(app, values);
        }
        app.append(" | ");
        if (mRight instanceof AndFilter) {
            app.append('(');
            mRight.appendTo(app, values);
            app.append(')');
        } else {
            mRight.appendTo(app, values);
        }
    }
}
