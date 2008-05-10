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
    static <S extends Storable> Filter<S> doBind(Filter<S> filter) {
        return doBind(filter, null);
    }

    private static <S extends Storable> Filter<S> doBind(Filter<S> filter, Binder<S> binder) {
        binder = new Binder<S>(binder);
        Filter<S> boundFilter = filter.accept(binder, null);
        if (binder.isRebindNeeded()) {
            binder = new Binder<S>(binder);
            boundFilter = boundFilter.accept(binder, null);
        }
        return boundFilter;
    }

    // Maps PropertyFilter with bind ID zero to PropertyFilter with highest bind
    // ID. All other mappings track which bindings have been created.
    private final Map<PropertyFilter<S>, PropertyFilter<S>> mBindMap;

    private boolean mNeedsRebind;

    private Binder(Binder<S> binder) {
        if (binder == null) {
            mBindMap = new IdentityHashMap<PropertyFilter<S>, PropertyFilter<S>>();
        } else {
            mBindMap = binder.mBindMap;
        }
    }

    public boolean isRebindNeeded() {
        return mNeedsRebind;
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
    public Filter<S> visit(final PropertyFilter<S> filter, Object param) {
        if (filter.isBound()) {
            if (mBindMap.containsKey(filter)) {
                // Binding was created by this Binder.
                return filter;
            }
            final PropertyFilter<S> zero = PropertyFilter.getCanonical(filter, 0);
            PropertyFilter<S> highest = mBindMap.get(zero);
            if (highest == null) {
                mBindMap.put(zero, filter);
            } else {
                // Have already created bindings which clash with existing
                // bindings.
                mNeedsRebind = true;
                if (filter.getBindID() > highest.getBindID()) {
                    mBindMap.put(zero, filter);
                }
            }
            return filter;
        } else {
            final PropertyFilter<S> zero;
            if (filter.getBindID() == 0) {
                zero = filter;
            } else {
                zero = PropertyFilter.getCanonical(filter, 0);
            }
            PropertyFilter<S> highest = mBindMap.get(zero);
            if (highest == null) {
                highest = PropertyFilter.getCanonical(filter, 1);
            } else {
                highest = PropertyFilter.getCanonical(filter, highest.getBindID() + 1);
            }
            mBindMap.put(zero, highest);
            mBindMap.put(highest, highest);
            return highest;
        }
    }

    @Override
    public Filter<S> visit(ExistsFilter<S> filter, Object param) {
        if (filter.isBound()) {
            return filter;
        }
        Filter<S> boundJoinedSubFilter = doBind(filter.getJoinedSubFilter(), this);
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
