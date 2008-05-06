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
 * Double linked list of leaf PropertyFilters in a Filter. The list order
 * matches the left-to-right order of the PropertyFilters.
 *
 * @author Brian S O'Neill
 */
class PropertyFilterList<S extends Storable> {
    private final PropertyFilter<S> mPropFilter;
    private final PropertyFilterList<S> mNext;
    private final int mNextRemaining;
    private final int mNextBlankRemaining;

    private PropertyFilterList<S> mPrev;
    private int mPrevRemaining = -1;
    private int mBlankCount = -1;

    /**
     * @param next pass null for tail of list
     */
    PropertyFilterList(PropertyFilter<S> propFilter, PropertyFilterList<S> next) {
        mPropFilter = propFilter;
        mNext = next;
        mNextRemaining = next == null ? 0 : next.mNextRemaining + 1;
        mNextBlankRemaining = next == null ? 0
            : (next.mNextBlankRemaining + (next.mPropFilter.isConstant() ? 0 : 1));
    }

    public PropertyFilter<S> getPropertyFilter() {
        return mPropFilter;
    }

    public Class<?> getNaturalPropertyType() {
        return mPropFilter.getType();
    }

    /**
     * Returns the property filter type, always represented as an object
     * type. Primitive types are represented by a wrapper class.
     */
    public Class<?> getObjectPropertyType() {
        return mPropFilter.getBoxedType();
    }

    /**
     * Returns null if the next remaining is zero.
     */
    public PropertyFilterList<S> getNext() {
        return mNext;
    }

    /**
     * Returns the amount of next remaining list nodes.
     */
    public int getNextRemaining() {
        return mNextRemaining;
    }

    /**
     * Returns the amount of next remaining non-constant list nodes.
     */
    public int getNextBlankRemaining() {
        return mNextBlankRemaining;
    }

    /**
     * Returns null if no previous node.
     */
    public PropertyFilterList<S> getPrevious() {
        return mPrev;
    }

    /**
     * Returns the amount of previous remaining list nodes.
     */
    public int getPreviousRemaining() {
        int remaining = mPrevRemaining;
        if (remaining < 0) {
            mPrevRemaining = remaining =
                ((mPrev == null) ? 0 : (mPrev.getPreviousRemaining() + 1));
        }
        return remaining;
    }

    /**
     * Returns the amount of non-constant list nodes up to this node.
     */
    public int getBlankCount() {
        int count = mBlankCount;
        if (count < 0) {
            mBlankCount = count = (mPropFilter.isConstant() ? 0 : 1)
                + ((mPrev == null) ? 0 : mPrev.getBlankCount());
        }
        return count;
    }

    /**
     * @param index if zero, returns this, if one, returns next, etc. If
     * negative, gets from last. -1 returns last, -2 returns next to last, etc.
     * @return null if index too high or too low
     */
    public PropertyFilterList<S> get(int index) {
        if (index <= 0) {
            if (index == 0) {
                return this;
            }
            if ((index = mNextRemaining + index + 1) <= 0) {
                if (index == 0) {
                    return this;
                }
                return null;
            }
        }
        if (mNext == null) {
            return null;
        }
        // Tail recursion.
        return mNext.get(index - 1);
    }

    /**
     * Searches list for given PropertyFilter.
     */
    public boolean contains(PropertyFilter<S> propFilter) {
        if (mPropFilter == propFilter) {
            return true;
        }
        if (mNext == null) {
            return false;
        }
        // Tail recursion.
        return mNext.contains(propFilter);
    }

    /**
     * Prepend a node to the head of the list.
     */
    PropertyFilterList<S> prepend(PropertyFilter<S> propFilter) {
        PropertyFilterList<S> head = new PropertyFilterList<S>(propFilter, this);
        mPrev = head;
        return head;
    }

    static class Builder<S extends Storable>
        extends Visitor<S, PropertyFilterList<S>, PropertyFilterList<S>>
    {
        @Override
        public PropertyFilterList<S> visit(OrFilter<S> filter, PropertyFilterList<S> list) {
            // Traverse right-to-left since list must be built in this order.
            list = filter.getRightFilter().accept(this, list);
            list = filter.getLeftFilter().accept(this, list);
            return list;
        }

        @Override
        public PropertyFilterList<S> visit(AndFilter<S> filter, PropertyFilterList<S> list) {
            // Traverse right-to-left since list must be built in this order.
            list = filter.getRightFilter().accept(this, list);
            list = filter.getLeftFilter().accept(this, list);
            return list;
        }

        @Override
        public PropertyFilterList<S> visit(PropertyFilter<S> filter, PropertyFilterList<S> list) {
            return list == null ? new PropertyFilterList<S>(filter, null) : list.prepend(filter);
        }

        @Override
        public PropertyFilterList<S> visit(ExistsFilter<S> filter, PropertyFilterList<S> list) {
            PropertyFilterList<S> subList =
                filter.getJoinedSubFilter().getTailPropertyFilterList();

            while (subList != null) {
                PropertyFilter<S> joinedFilter = subList.getPropertyFilter();
                if (list == null) {
                    list = new PropertyFilterList<S>(joinedFilter, null);
                } else {
                    list = list.prepend(joinedFilter);
                }
                subList = subList.getPrevious();
            }

            return list;
        }
    }
}
