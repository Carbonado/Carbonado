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
 * Algebraically distributes a tree into each node of the visted tree.
 *
 * @author Brian S O'Neill
 */
class Distributer<S extends Storable> extends Visitor<S, Filter<S>, Filter<S>> {
    private final boolean mDoRight;
    private final boolean mDoAnd;

    /**
     * @param doRight when true, distribute to the right, otherwise,
     * distribute to the left.
     * @param doAnd when true, distribute 'and' operations, otherwise
     * distribute 'or' operations.
     */
    Distributer(boolean doRight, boolean doAnd) {
        mDoRight = doRight;
        mDoAnd = doAnd;
    }

    /**
     * @param filter candidate node to potentially replace
     * @param distribute node to distribute into candidate node
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(OrFilter<S> filter, Filter<S> distribute) {
        return OrFilter.getCanonical(filter.getLeftFilter().accept(this, distribute),
                                     filter.getRightFilter().accept(this, distribute));
    }

    /**
     * @param filter candidate node to potentially replace
     * @param distribute node to distribute into candidate node
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(AndFilter<S> filter, Filter<S> distribute) {
        return AndFilter.getCanonical(filter.getLeftFilter().accept(this, distribute),
                                      filter.getRightFilter().accept(this, distribute));
    }

    /**
     * @param filter candidate node to potentially replace
     * @param distribute node to distribute into candidate node
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(PropertyFilter<S> filter, Filter<S> distribute) {
        if (mDoRight) {
            return mDoAnd ? filter.and(distribute) : filter.or(distribute);
        } else {
            return mDoAnd ? distribute.and(filter) : distribute.or(filter);
        }
    }

    /**
     * @param filter candidate node to potentially replace
     * @param distribute node to distribute into candidate node
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(ExistsFilter<S> filter, Filter<S> distribute) {
        if (mDoRight) {
            return mDoAnd ? filter.and(distribute) : filter.or(distribute);
        } else {
            return mDoAnd ? distribute.and(filter) : distribute.or(filter);
        }
    }
}
