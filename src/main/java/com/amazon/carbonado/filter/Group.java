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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.amazon.carbonado.Storable;

/**
 * Group of filter nodes that are all 'or'ed or 'and'ed together.
 *
 * @author Brian S O'Neill
 */
class Group<S extends Storable> {
    final boolean mForAnd;
    Set<Filter<S>> mChildren;

    Group(boolean forAnd) {
        mForAnd = forAnd;
    }

    boolean add(Filter<S> child) {
        if (mChildren == null) {
            mChildren = new LinkedHashSet<Filter<S>>(20);
        }
        return mChildren.add(child);
    }

    /**
     * Reduce the group set by eliminating redundant children. The
     * transformations applied are:
     *
     * ((a & b) | b) => b
     * ((a | b) & b) => b
     */
    void reduce() {
        Iterator<Filter<S>> it = mChildren.iterator();
        aLoop:
        while (it.hasNext()) {
            Filter<S> a = it.next();
            for (Filter<S> b : mChildren) {
                if (a != b) {
                    if (a.accept(new Scanner<S>(!mForAnd), b)) {
                        it.remove();
                        continue aLoop;
                    }
                }
            }
        }
    }

    Filter<S> merge() {
        Filter<S> filter = null;
        if (mChildren != null) {
            for (Filter<S> child : mChildren) {
                if (filter == null) {
                    filter = child;
                } else if (mForAnd) {
                    filter = filter.and(child);
                } else {
                    filter = filter.or(child);
                }
            }
        }
        return filter;
    }

    /**
     * Does the work of reducing a group
     */
    private static class Scanner<S extends Storable> extends Visitor<S, Boolean, Filter<S>>{
        private final boolean mForAnd;

        Scanner(boolean forAnd) {
            mForAnd = forAnd;
        }

        /**
         * @return TRUE if overlap was found
         */
        @Override
        public Boolean visit(OrFilter<S> filter, Filter<S> child) {
            if (mForAnd) {
                return false;
            }
            if (filter == child) {
                return true;
            }
            return filter.getLeftFilter().accept(this, child) ||
                filter.getRightFilter().accept(this, child);
        }

        /**
         * @return TRUE if overlap was found
         */
        @Override
        public Boolean visit(AndFilter<S> filter, Filter<S> child) {
            if (!mForAnd) {
                return false;
            }
            if (filter == child) {
                return true;
            }
            return filter.getLeftFilter().accept(this, child) ||
                filter.getRightFilter().accept(this, child);
        }

        /**
         * @return TRUE if overlap was found
         */
        @Override
        public Boolean visit(PropertyFilter<S> filter, Filter<S> child) {
            return filter == child;
        }

        /**
         * @return TRUE if overlap was found
         */
        @Override
        public Boolean visit(ExistsFilter<S> filter, Filter<S> child) {
            return filter == child;
        }
    }
}
