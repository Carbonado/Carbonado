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

package com.amazon.carbonado.qe;

import java.util.AbstractList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIntrospector;

/**
 * Produces unmodifiable lists of {@link OrderedProperty orderings}. Instances
 * are immutable, canonical and cached. Calls to "equals" and "hashCode" are
 * fast.
 *
 * @author Brian S O'Neill
 */
public class OrderingList<S extends Storable> extends AbstractList<OrderedProperty<S>> {
    private static final OrderingList EMPTY_LIST = new OrderingList();

    private static final Map<Class, OrderingList> cCache;

    static {
        cCache = new SoftValuedHashMap();
    }

    /**
     * Returns a canonical empty instance.
     */
    public static <S extends Storable> OrderingList<S> emptyList() {
        return EMPTY_LIST;
    }

    /**
     * Returns a canonical instance composed of the given ordering.
     *
     * @throws IllegalArgumentException if ordering property is not in S
     */
    public static <S extends Storable> OrderingList<S> get(Class<S> type, String property) {
        if (property == null) {
            return EMPTY_LIST;
        }
        return getListNode(type).nextNode(type, property);
    }

    /**
     * Returns a canonical instance composed of the given orderings.
     *
     * @throws IllegalArgumentException if any ordering property is not in S
     */
    public static <S extends Storable> OrderingList<S> get(Class<S> type, String... orderings) {
        if (orderings == null || orderings.length == 0) {
            return EMPTY_LIST;
        }

        OrderingList<S> node = getListNode(type);
        for (String property : orderings) {
            node = node.nextNode(type, property);
        }

        return node;
    }

    /**
     * Returns a canonical instance composed of the given orderings.
     */
    public static <S extends Storable> OrderingList<S> get(OrderedProperty<S>... orderings) {
        if (orderings == null || orderings.length == 0) {
            return EMPTY_LIST;
        }

        Class<S> type = orderings[0].getChainedProperty().getPrimeProperty().getEnclosingType();

        OrderingList<S> node = getListNode(type);
        for (OrderedProperty<S> property : orderings) {
            node = node.nextNode(property);
        }

        return node;
    }

    private static <S extends Storable> OrderingList<S> getListNode(Class<S> type) {
        OrderingList<S> node;
        synchronized (cCache) {
            node = (OrderingList<S>) cCache.get(type);
            if (node == null) {
                node = new OrderingList<S>();
                cCache.put(type, node);
            }
        }
        return node;
    }

    private final OrderingList<S> mParent;
    private final OrderedProperty<S> mProperty;
    private final int mSize;

    private Map<Object, OrderingList<S>> mNextNode;

    private OrderedProperty<S>[] mOrderings;
    private String[] mOrderingStrings;

    private OrderingList() {
        mParent = null;
        mProperty = null;
        mSize = 0;
    }

    private OrderingList(OrderingList<S> parent, OrderedProperty<S> property) {
        if (property == null) {
            throw new IllegalArgumentException("Ordering property is null");
        }
        mParent = parent;
        mProperty = property;
        mSize = parent.mSize + 1;
    }

    public int size() {
        return mSize;
    }

    public OrderedProperty<S> get(int index) {
        return asArray()[index];
    }

    /**
     * Returns a list which concatenates this one with the other one.
     */
    public OrderingList<S> concat(OrderingList<S> other) {
        if (size() == 0) {
            return other;
        }

        OrderingList<S> node = this;

        if (other.size() > 0) {
            for (OrderedProperty<S> property : other) {
                node = node.nextNode(property);
            }
        }

        return node;
    }

    /**
     * This method is not public because the array is not a clone.
     */
    private OrderedProperty<S>[] asArray() {
        if (mOrderings == null) {
            OrderedProperty<S>[] orderings = new OrderedProperty[mSize];
            OrderingList<S> node = this;
            for (int i=mSize; --i>=0; ) {
                orderings[i] = node.mProperty;
                node = node.mParent;
            }
            mOrderings = orderings;
        }
        return mOrderings;
    }

    /**
     * Returns the orderings as qualified string property names. Each is
     * prefixed with a '+' or '-'.
     *
     * <p>This method is not public because the array is not a clone.
     */
    String[] asStringArray() {
        if (mOrderingStrings == null) {
            String[] orderings = new String[mSize];
            OrderingList<S> node = this;
            for (int i=mSize; --i>=0; ) {
                orderings[i] = node.mProperty.toString();
                node = node.mParent;
            }
            mOrderingStrings = orderings;
        }
        return mOrderingStrings;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return super.equals(other);
    }

    private synchronized OrderingList<S> nextNode(Class<S> type, String property) {
        OrderingList<S> node;
        if (mNextNode == null) {
            mNextNode = new HashMap<Object, OrderingList<S>>();
            node = null;
        } else {
            node = mNextNode.get(property);
        }

        if (node == null) {
            OrderedProperty<S> op = OrderedProperty
                .parse(StorableIntrospector.examine(type), property);

            node = nextNode(op);
            mNextNode.put(property, node);
        }

        return node;
    }

    private synchronized OrderingList<S> nextNode(OrderedProperty<S> property) {
        OrderingList<S> node;
        if (mNextNode == null) {
            mNextNode = new HashMap<Object, OrderingList<S>>();
            node = null;
        } else {
            node = mNextNode.get(property);
        }

        if (node == null) {
            node = new OrderingList<S>(this, property);
            mNextNode.put(property, node);
        }

        return node;
    }
}
