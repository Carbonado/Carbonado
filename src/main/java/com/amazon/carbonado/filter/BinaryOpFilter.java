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

import com.amazon.carbonado.Storable;

/**
 * Base class for filter tree nodes that have a left and right child.
 *
 * @author Brian S O'Neill
 */
public abstract class BinaryOpFilter<S extends Storable> extends Filter<S> {
    // Bits used in mState.
    private static final byte REDUCED   = 0x1 << 0; // tree is reduced
    private static final byte DNF_KNOWN = 0x1 << 1; // disjunctive normal form state is known
    private static final byte DNF       = 0x1 << 2; // is disjunctive normal form if set
    private static final byte CNF_KNOWN = 0x1 << 3; // conjunctive normal form state is known
    private static final byte CNF       = 0x1 << 4; // is conjunctive normal form if set
    private static final byte BOUND     = 0x1 << 5; // properties are bound when set

    final Filter<S> mLeft;
    final Filter<S> mRight;

    byte mState;

    BinaryOpFilter(Filter<S> left, Filter<S> right) {
        super(left == null ? null : left.getStorableType());
        if (left == null || right == null) {
            throw new IllegalArgumentException("Left or right filter is null");
        }
        if (left.getStorableType() != right.getStorableType()) {
            throw new IllegalArgumentException("Type mismatch");
        }
        mLeft = left;
        mRight = right;
        if (left.isBound() && right.isBound()) {
            markBound();
        }
    }

    public Filter<S> getLeftFilter() {
        return mLeft;
    }

    public Filter<S> getRightFilter() {
        return mRight;
    }

    @Override
    public Filter<S> bind() {
        if (isBound()) {
            return this;
        }
        return Binder.doBind(this);
    }

    @Override
    public synchronized boolean isBound() {
        return (mState & BOUND) != 0;
    }

    @Override
    synchronized void markBound() {
        mState |= BOUND;
    }

    @Override
    final synchronized boolean isDisjunctiveNormalForm() {
        if ((mState & DNF_KNOWN) != 0) { // if dnf state is known...
            return (mState & DNF) != 0;  // return true if dnf
        }
        if (checkIsDisjunctiveNormalForm()) {
            mState |= (DNF_KNOWN | DNF); // dnf state is now known, and is dnf
            return true;
        } else {
            mState |= DNF_KNOWN; // dnf state is now known, and is not dnf
            mState &= ~DNF;
            return false;
        }
    }

    abstract boolean checkIsDisjunctiveNormalForm();

    @Override
    final synchronized boolean isConjunctiveNormalForm() {
        if ((mState & CNF_KNOWN) != 0) { // if cnf state is known...
            return (mState & CNF) != 0;  // return true if cnf
        }
        if (checkIsConjunctiveNormalForm()) {
            mState |= (CNF_KNOWN | CNF); // cnf state is now known, and is cnf
            return true;
        } else {
            mState |= CNF_KNOWN; // cnf state is now known, and is not cnf
            mState &= ~CNF;
            return false;
        }
    }

    abstract boolean checkIsConjunctiveNormalForm();

    @Override
    synchronized boolean isReduced() {
        return (mState & REDUCED) != 0;
    }

    @Override
    synchronized void markReduced() {
        mState |= REDUCED;
    }
}
