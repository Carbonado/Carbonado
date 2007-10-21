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

import java.util.IdentityHashMap;
import java.util.Map;

import com.amazon.carbonado.Storable;

/**
 * Used by bind operation - assigns bind IDs to property filters. This allows
 * operations like dnf to not lose track of variable assignments even after
 * more nodes are added to the filter tree.
 *
 * @author Brian S O'Neill
 */
class Binder<S extends Storable> extends Visitor<S, Filter<S>, Object> {
    // Maps PropertyFilter with bind ID zero to PropertyFilter with highest
    // bind ID.
    private final Map<PropertyFilter<S>, PropertyFilter<S>> mBindMap;

    Binder() {
        mBindMap = new IdentityHashMap<PropertyFilter<S>, PropertyFilter<S>>();
    }

    private Binder(Map<PropertyFilter<S>, PropertyFilter<S>> bindMap) {
        mBindMap = bindMap;
    }

    @Override
    public Filter<S> visit(OrFilter<S> filter, Object param) {
        Filter<S> left = filter.getLeftFilter();
        Filter<S> newLeft = left.accept(this, null);
        Filter<S> right = filter.getRightFilter();
        Filter<S> newRight = right.accept(this, null);

        Filter<S> newFilter;
        if (left != newLeft || right != newRight) {
            newFilter = newLeft.or(newRight);
        } else {
            newFilter = filter;
        }

        newFilter.markBound();
        return newFilter;
    }

    @Override
    public Filter<S> visit(AndFilter<S> filter, Object param) {
        Filter<S> left = filter.getLeftFilter();
        Filter<S> newLeft = left.accept(this, null);
        Filter<S> right = filter.getRightFilter();
        Filter<S> newRight = right.accept(this, null);

        Filter<S> newFilter;
        if (left != newLeft || right != newRight) {
            newFilter = newLeft.and(newRight);
        } else {
            newFilter = filter;
        }

        newFilter.markBound();
        return newFilter;
    }

    @Override
    public Filter<S> visit(PropertyFilter<S> filter, Object param) {
        if (filter.isBound()) {
            return filter;
        }
        filter = PropertyFilter.getCanonical(filter, 1);
        PropertyFilter<S> highest = mBindMap.get(filter);
        if (highest == null) {
            highest = filter;
        } else {
            highest = PropertyFilter.getCanonical(filter, highest.getBindID() + 1);
        }
        mBindMap.put(filter, highest);
        return highest;
    }

    @Override
    public Filter<S> visit(ExistsFilter<S> filter, Object param) {
        if (filter.isBound()) {
            return filter;
        }
        Filter<S> boundJoinedSubFilter =
            filter.getJoinedSubFilter().accept(new Binder<S>(mBindMap), null);
        Filter<S>.NotJoined nj =
            boundJoinedSubFilter.notJoinedFromAny(filter.getChainedProperty());
        if (nj.getRemainderFilter() != null && !(nj.getRemainderFilter().isOpen())) {
            // This should not happen.
            throw new IllegalStateException(nj.toString());
        }
        return ExistsFilter.build
            (filter.getChainedProperty(), nj.getNotJoinedFilter(), filter.isNotExists());
    }
}
