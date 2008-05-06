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

package com.amazon.carbonado.info;

import java.io.IOException;
import java.io.Serializable;

import org.cojen.util.WeakCanonicalSet;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.util.Appender;

/**
 * Represents a property paired with a preferred ordering direction.
 *
 * @author Brian S O'Neill
 */
public class OrderedProperty<S extends Storable> implements Serializable, Appender {
    private static final long serialVersionUID = 1L;

    static WeakCanonicalSet cCanonical = new WeakCanonicalSet();

    /**
     * Returns a canonical instance.
     *
     * @throws IllegalArgumentException if property is null
     */
    public static <S extends Storable> OrderedProperty<S> get(StorableProperty<S> property,
                                                              Direction direction) {
        return get(ChainedProperty.get(property), direction);
    }

    /**
     * Returns a canonical instance.
     *
     * @throws IllegalArgumentException if property is null
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> OrderedProperty<S> get(ChainedProperty<S> property,
                                                              Direction direction) {
        return (OrderedProperty<S>) cCanonical.put(new OrderedProperty<S>(property, direction));
    }

    /**
     * Parses an ordering property, which may start with a '+' or '-' to
     * indicate direction. Prefix of '~' indicates unspecified direction. If
     * ordering prefix not specified, default direction is ascending.
     *
     * @param info Info for Storable type containing property
     * @param str string to parse
     * @throws IllegalArgumentException if any required parameter is null or
     * string format is incorrect
     */
    public static <S extends Storable> OrderedProperty<S> parse(StorableInfo<S> info,
                                                                String str)
        throws IllegalArgumentException
    {
        return parse(info, str, Direction.ASCENDING);
    }

    /**
     * Parses an ordering property, which may start with a '+' or '-' to
     * indicate direction. Prefix of '~' indicates unspecified direction.
     *
     * @param info Info for Storable type containing property
     * @param str string to parse
     * @param defaultDirection default direction if not specified in
     * string. If null, ascending order is defaulted.
     * @throws IllegalArgumentException if any required parameter is null or
     * string format is incorrect
     */
    public static <S extends Storable> OrderedProperty<S> parse(StorableInfo<S> info,
                                                                String str,
                                                                Direction defaultDirection)
        throws IllegalArgumentException
    {
        if (info == null || str == null || defaultDirection == null) {
            throw new IllegalArgumentException();
        }
        Direction direction = defaultDirection;
        if (str.length() > 0) {
            if (str.charAt(0) == '+') {
                direction = Direction.ASCENDING;
                str = str.substring(1);
            } else if (str.charAt(0) == '-') {
                direction = Direction.DESCENDING;
                str = str.substring(1);
            } else if (str.charAt(0) == '~') {
                direction = Direction.UNSPECIFIED;
                str = str.substring(1);
            }
        }
        if (direction == null) {
            direction = Direction.ASCENDING;
        }
        return get(ChainedProperty.parse(info, str), direction);
    }

    private final ChainedProperty<S> mProperty;
    private final Direction mDirection;

    private OrderedProperty(ChainedProperty<S> property, Direction direction) {
        if (property == null) {
            throw new IllegalArgumentException();
        }
        mProperty = property;
        mDirection = direction == null ? Direction.UNSPECIFIED : direction;
    }

    public ChainedProperty<S> getChainedProperty() {
        return mProperty;
    }

    public Direction getDirection() {
        return mDirection;
    }

    public OrderedProperty<S> reverse() {
        if (mDirection == Direction.UNSPECIFIED) {
            return this;
        }
        return get(mProperty, mDirection.reverse());
    }

    public OrderedProperty<S> direction(Direction direction) {
        return get(mProperty, direction);
    }

    @Override
    public int hashCode() {
        return mProperty.hashCode() + mDirection.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof OrderedProperty) {
            OrderedProperty<?> other = (OrderedProperty<?>) obj;
            return mProperty.equals(other.mProperty) && mDirection.equals(other.mDirection);
        }
        return false;
    }

    /**
     * Returns the chained property in a parseable form.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(mDirection.toCharacter());
        try {
            mProperty.appendTo(buf);
        } catch (IOException e) {
            // Not gonna happen.
        }
        return buf.toString();
    }

    public void appendTo(Appendable app) throws IOException {
        app.append(mDirection.toCharacter());
        mProperty.appendTo(app);
    }

    private Object readResolve() {
        return get(mProperty, mDirection);
    }
}
