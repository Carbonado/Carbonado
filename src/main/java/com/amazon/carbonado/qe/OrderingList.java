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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import java.util.AbstractList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIntrospector;

/**
 * Produces unmodifiable lists of {@link OrderedProperty orderings}. Instances
 * are immutable, canonical and cached. Calls to "equals" and "hashCode" are
 * fast.
 *
 * @author Brian S O'Neill
 */
public class OrderingList<S extends Storable> extends AbstractList<OrderedProperty<S>>
    implements Serializable
{
    private static final long serialVersionUID = 3692335128299485356L;

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
        OrderingList<S> list = emptyList();
        if (property != null) {
            list = list.concat(type, property);
        }
        return list;
    }

    /**
     * Returns a canonical instance composed of the given orderings.
     *
     * @throws IllegalArgumentException if any ordering property is not in S
     */
    public static <S extends Storable> OrderingList<S> get(Class<S> type, String... orderings) {
        OrderingList<S> list = emptyList();
        if (orderings != null && orderings.length > 0) {
            for (String property : orderings) {
                list = list.concat(type, property);
            }
        }
        return list;
    }

    /**
     * Returns a canonical instance composed of the given orderings.
     */
    public static <S extends Storable> OrderingList<S> get(OrderedProperty<S>... orderings) {
        OrderingList<S> list = emptyList();
        if (orderings != null && orderings.length > 0) {
            for (OrderedProperty<S> property : orderings) {
                list = list.concat(property);
            }
        }
        return list;
    }

    /**
     * Returns a canonical instance composed of the given orderings.
     */
    public static <S extends Storable> OrderingList<S> get(List<OrderedProperty<S>> orderings) {
        OrderingList<S> list = emptyList();
        if (orderings != null && orderings.size() > 0) {
            for (OrderedProperty<S> property : orderings) {
                list = list.concat(property);
            }
        }
        return list;
    }

    private static <S extends Storable> OrderingList<S> getListHead(Class<S> type) {
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

    @Override
    public int size() {
        return mSize;
    }

    @Override
    public OrderedProperty<S> get(int index) {
        return asArray()[index];
    }

    /**
     * Returns a list which concatenates this one with the given property.
     */
    public OrderingList<S> concat(Class<S> type, String property) {
        OrderingList<S> newList = this;
        if (newList == EMPTY_LIST) {
            // Cannot concat from singleton EMPTY_LIST.
            newList = getListHead(type);
        }
        return newList.nextNode(type, property);
    }

    /**
     * Returns a list which concatenates this one with the given property.
     */
    public OrderingList<S> concat(OrderedProperty<S> property) {
        OrderingList<S> newList = this;
        if (newList == EMPTY_LIST) {
            // Cannot concat from singleton EMPTY_LIST.
            newList = getListHead
                (property.getChainedProperty().getPrimeProperty().getEnclosingType());
        }
        return newList.nextNode(property);
    }

    /**
     * Returns a list which concatenates this one with the other one.
     */
    public OrderingList<S> concat(OrderingList<S> other) {
        if (size() == 0) {
            return other;
        }
        OrderingList<S> newList = this;
        if (other.size() > 0) {
            for (OrderedProperty<S> property : other) {
                newList = newList.concat(property);
            }
        }
        return newList;
    }

    /**
     * Eliminates redundant ordering properties.
     */
    public OrderingList<S> reduce() {
        if (size() == 0) {
            return this;
        }

        Set<ChainedProperty<S>> seen = new HashSet<ChainedProperty<S>>();
        OrderingList<S> newList = emptyList();

        for (OrderedProperty<S> property : this) {
            ChainedProperty<S> chained = property.getChainedProperty();
            if (!seen.contains(chained)) {
                newList = newList.concat(property);
                seen.add(chained);
            }
        }

        return newList;
    }

    /**
     * Returns this list with all orderings in reverse.
     */
    public OrderingList<S> reverseDirections() {
        if (size() == 0) {
            return this;
        }
        OrderingList<S> reversedList = emptyList();
        for (int i=0; i<size(); i++) {
            reversedList = reversedList.concat(get(i).reverse());
        }
        return reversedList;
    }

    /**
     * Returns a list with the given element replaced.
     */
    public OrderingList<S> replace(int index, OrderedProperty<S> property) {
        int size = size();
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        OrderingList<S> newList = emptyList();
        for (int i=0; i<size; i++) {
            newList = newList.concat(i == index ? property : get(i));
        }
        return newList;
    }

    @Override
    public OrderingList<S> subList(int fromIndex, int toIndex) {
        // Check for optimization opportunity.
        if (fromIndex == 0 && toIndex >= 0 && toIndex <= mSize) {
            if (toIndex == 0) {
                return emptyList();
            }
            OrderingList<S> list = this;
            while (toIndex < list.mSize) {
                list = list.mParent;
            }
            return list;
        }

        return get(super.subList(fromIndex, toIndex));
    }

    /**
     * This method is not public because the array is not a clone.
     */
    OrderedProperty<S>[] asArray() {
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

    private Object writeReplace() {
        return new Orderings(asArray());
    }

    private static class Orderings implements Externalizable {
        private static final long serialVersionUID = 1L;

        private OrderedProperty[] mOrderings;

        // Required for Externalizable.
        public Orderings() {
        }

        Orderings(OrderedProperty<?>[] orderings) {
            mOrderings = orderings;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(mOrderings);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            mOrderings = (OrderedProperty<?>[]) in.readObject();
        }

        private Object readResolve() {
            return OrderingList.get(mOrderings);
        }
    }
}
