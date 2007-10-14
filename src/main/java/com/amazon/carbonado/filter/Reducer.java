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
 * Reduces a tree by eliminating redundant 'and' or 'or' terms. As a desired
 * side-effect, the tree is also unbalanced to the left.
 *
 * @author Brian S O'Neill
 */
class Reducer<S extends Storable> extends Visitor<S, Filter<S>, Group<S>> {
    Reducer() {
    }

    /**
     * @param filter candidate node to potentially replace
     * @param group gathered children
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(OrFilter<S> filter, Group<S> group) {
        final Group<S> parentGroup = group;

        boolean groupRoot;
        if (groupRoot = (group == null || group.mForAnd)) {
            group = new Group<S>(false);
        }

        filter.getLeftFilter().accept(this, group);
        filter.getRightFilter().accept(this, group);

        if (groupRoot) {
            group.reduce();
            Filter<S> result = group.merge();
            result.markReduced();
            if (parentGroup != null) {
                parentGroup.add(result);
            }
            return result;
        }

        return null;
    }

    /**
     * @param filter candidate node to potentially replace
     * @param group gathered children
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(AndFilter<S> filter, Group<S> group) {
        final Group<S> parentGroup = group;

        boolean groupRoot;
        if (groupRoot = (group == null || !group.mForAnd)) {
            group = new Group<S>(true);
        }

        filter.getLeftFilter().accept(this, group);
        filter.getRightFilter().accept(this, group);

        if (groupRoot) {
            group.reduce();
            Filter<S> result = group.merge();
            result.markReduced();
            if (parentGroup != null) {
                parentGroup.add(result);
            }
            return result;
        }

        return null;
    }

    /**
     * @param filter candidate node to potentially replace
     * @param group gathered children
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(PropertyFilter<S> filter, Group<S> group) {
        group.add(filter);
        return null;
    }

    /**
     * @param filter candidate node to potentially replace
     * @param group gathered children
     * @return original candidate or replacement
     */
    @Override
    public Filter<S> visit(ExistsFilter<S> filter, Group<S> group) {
        group.add(filter);
        return null;
    }
}
