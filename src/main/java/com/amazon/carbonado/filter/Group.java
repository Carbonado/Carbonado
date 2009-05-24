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
    Set<Filter<S>> mElements;

    Group(boolean forAnd) {
        mForAnd = forAnd;
    }

    boolean add(Filter<S> element) {
        if (mElements == null) {
            // Use LinkedHashSet to best preserve original ordering of
            // elements. It also tends to be faster when performing complex
            // dnf/cnf transformations because the reduction step doesn't have
            // to work as hard rearranging elements.
            mElements = new LinkedHashSet<Filter<S>>(20);
        }
        return mElements.add(element);
    }

    /**
     * Reduce the group set by eliminating redundant elements. These
     * transformations applied are:
     *
     * ((a & b) | b) => b
     * ((a | b) & b) => b
     */
    void reduce() {
        // Note that the scanner choice is opposite of our type. This is
        // because a group of 'and's has children which are 'or's and vice
        // versa.
        Scanner<S> scanner = mForAnd ? OrChildScanner.THE : AndChildScanner.THE;

        Iterator<Filter<S>> it = mElements.iterator();
        aLoop:
        while (it.hasNext()) {
            Filter<S> a = it.next();
            for (Filter<S> b : mElements) {
                if (a != b) {
                    if (a.accept(scanner, b)) {
                        it.remove();
                        continue aLoop;
                    }
                }
            }
        }
    }

    Filter<S> merge() {
        Filter<S> filter = null;
        if (mElements != null) {
            for (Filter<S> element : mElements) {
                if (filter == null) {
                    filter = element;
                } else if (mForAnd) {
                    filter = filter.and(element);
                } else {
                    filter = filter.or(element);
                }
            }
        }
        return filter;
    }

    /**
     * Base class for finding filters inside other filters.
     */
    private static abstract class Scanner<S extends Storable>
        extends Visitor<S, Boolean, Filter<S>>
    {
        @Override
        public Boolean visit(PropertyFilter<S> filter, Filter<S> search) {
            return filter == search;
        }

        @Override
        public Boolean visit(ExistsFilter<S> filter, Filter<S> search) {
            return filter == search;
        }
    }

    /**
     * Searches for the existence of a child filter composed of 'or's as a
     * sub-filter of another filter composed of 'or's, returning true if found.
     */
    private static class OrChildScanner<S extends Storable> extends Scanner<S> {
        static final OrChildScanner THE = new OrChildScanner();

        @Override
        public Boolean visit(OrFilter<S> parent, Filter<S> child) {
            if (parent == child) {
                return true;
            }
            if (parent.getLeftFilter().accept(this, child) ||
                parent.getRightFilter().accept(this, child))
            {
                return true;
            }
            Scanner<S> scanner = OrParentScanner.THE;
            return child.accept(scanner, parent);
        }

        @Override
        public Boolean visit(AndFilter<S> parent, Filter<S> child) {
            return false;
        }
    }

    /**
     * Searches for the existence of a child filter composed of 'and's as a
     * sub-filter of another filter composed of 'and's, returning true if
     * found.
     */
    private static class AndChildScanner<S extends Storable> extends Scanner<S> {
        static final AndChildScanner THE = new AndChildScanner();

        @Override
        public Boolean visit(OrFilter<S> parent, Filter<S> child) {
            return false;
        }

        @Override
        public Boolean visit(AndFilter<S> parent, Filter<S> child) {
            if (parent == child) {
                return true;
            }
            if (parent.getLeftFilter().accept(this, child) ||
                parent.getRightFilter().accept(this, child))
            {
                return true;
            }
            Scanner<S> scanner = AndParentScanner.THE;
            return child.accept(scanner, parent);
        }
    }

    /**
     * Searches for the existence of a parent filter composed of 'or's as a
     * super-filter of another filter composed of 'or's, returning true if
     * found.
     */
    private static class OrParentScanner<S extends Storable> extends Scanner<S> {
        static final OrParentScanner THE = new OrParentScanner();

        @Override
        public Boolean visit(AndFilter<S> child, Filter<S> parent) {
            return false;
        }

        @Override
        public Boolean visit(OrFilter<S> child, Filter<S> parent) {
            if (child == parent) {
                return true;
            }
            Scanner<S> scanner = OrChildScanner.THE;
            return parent.accept(scanner, child.getLeftFilter()) &&
                parent.accept(scanner, child.getRightFilter());
        }
    }

    /**
     * Searches for the existence of a parent filter composed of 'and's as a
     * super-filter of another filter composed of 'and's, returning true if
     * found.
     */
    private static class AndParentScanner<S extends Storable> extends Scanner<S> {
        static final AndParentScanner THE = new AndParentScanner();

        @Override
        public Boolean visit(AndFilter<S> child, Filter<S> parent) {
            if (child == parent) {
                return true;
            }
            Scanner<S> scanner = AndChildScanner.THE;
            return parent.accept(scanner, child.getLeftFilter()) &&
                parent.accept(scanner, child.getRightFilter());
        }

        @Override
        public Boolean visit(OrFilter<S> child, Filter<S> parent) {
            return false;
        }
    }
}
