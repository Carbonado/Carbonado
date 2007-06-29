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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.info.ChainedProperty;

/**
 * Filter tree node that performs a relational test against a specific property
 * value.
 *
 * @author Brian S O'Neill
 */
public class PropertyFilter<S extends Storable> extends Filter<S> {
    // Indicates property has been bound to a constant value.
    private static int BOUND_CONSTANT = -1;

    /**
     * Returns a canonical instance, creating a new one if there isn't one
     * already in the cache.
     *
     * @throws IllegalArgumentException if property or operator is null
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> PropertyFilter<S> getCanonical(ChainedProperty<S> property,
                                                               RelOp op,
                                                               int bindID)
    {
        return (PropertyFilter<S>) cCanonical
            .put(new PropertyFilter<S>(property, op, bindID, null));
    }

    /**
     * Returns a canonical instance, creating a new one if there isn't one
     * already in the cache.
     *
     * @throws IllegalArgumentException if property or operator is null
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> PropertyFilter<S> getCanonical(ChainedProperty<S> property,
                                                               RelOp op,
                                                               Object constant)
    {
        return (PropertyFilter<S>) cCanonical
            .put(new PropertyFilter<S>(property, op, BOUND_CONSTANT, constant));
    }

    /**
     * Returns a canonical instance, creating a new one if there isn't one
     * already in the cache.
     */
    @SuppressWarnings("unchecked")
    static <S extends Storable> PropertyFilter<S> getCanonical(PropertyFilter<S> filter,
                                                               int bindID)
    {
        if (filter.mBindID == bindID) {
            return filter;
        }
        return (PropertyFilter<S>) cCanonical
            .put(new PropertyFilter<S>(filter.getChainedProperty(),
                                       filter.getOperator(), bindID, filter.mConstant));
    }

    private final ChainedProperty<S> mProperty;
    private final RelOp mOp;

    private final int mBindID;

    private final Object mConstant;

    private transient volatile Class<?> mBoxedType;

    /**
     * @throws IllegalArgumentException if property or operator is null
     */
    private PropertyFilter(ChainedProperty<S> property, RelOp op, int bindID, Object constant) {
        super(property == null ? null : property.getPrimeProperty().getEnclosingType());
        if (op == null) {
            throw new IllegalArgumentException();
        }
        mProperty = property;
        mOp = op;
        mBindID = bindID;
        mConstant = constant;
    }

    @Override
    public PropertyFilter<S> not() {
        if (mBindID == BOUND_CONSTANT) {
            return getCanonical(mProperty, mOp.reverse(), mConstant);
        } else {
            return getCanonical(mProperty, mOp.reverse(), mBindID);
        }
    }

    /**
     * @since 1.1.1
     */
    @Override
    public List<Filter<S>> disjunctiveNormalFormSplit() {
        // Yes, the Java compiler really wants me to do a useless cast.
        return Collections.singletonList((Filter<S>) this);
    }

    /**
     * @since 1.1.1
     */
    @Override
    public List<Filter<S>> conjunctiveNormalFormSplit() {
        // Yes, the Java compiler really wants me to do a useless cast.
        return Collections.singletonList((Filter<S>) this);
    }

    public <R, P> R accept(Visitor<S, R, P> visitor, P param) {
        return visitor.visit(this, param);
    }

    public ChainedProperty<S> getChainedProperty() {
        return mProperty;
    }

    /**
     * Returns the type of the ChainedProperty.
     */
    public Class<?> getType() {
        return mProperty.getType();
    }

    /**
     * Returns the type of the ChainedProperty property, boxed into an object
     * if primitive.
     */
    public Class<?> getBoxedType() {
        if (mBoxedType == null) {
            mBoxedType = TypeDesc.forClass(getType()).toObjectType().toClass();
        }
        return mBoxedType;
    }

    public RelOp getOperator() {
        return mOp;
    }

    /**
     * Bind ID is used to distinguish this PropertyFilter instance from another
     * against the same property. For example, the filter "a = ? | a = ?"
     * references the property 'a' twice. Each '?' parameter is bound to a
     * different value, and so the bind ID for each property filter is
     * different. "a = ?[1] | a = ?[2]".
     *
     * @return assigned bind ID, or 0 if unbound
     */
    public int getBindID() {
        return mBindID;
    }

    public PropertyFilter<S> bind() {
        return mBindID == 0 ? getCanonical(this, 1) : this;
    }

    public PropertyFilter<S> unbind() {
        return mBindID == 0 ? this : getCanonical(this, 0);
    }

    public boolean isBound() {
        return mBindID != 0;
    }

    public <T extends Storable> PropertyFilter<T> asJoinedFrom(ChainedProperty<T> joinProperty) {
        if (joinProperty.getType() != getStorableType()) {
            throw new IllegalArgumentException
                ("Property is not of type \"" + getStorableType().getName() + "\": " +
                 joinProperty);
        }

        ChainedProperty<T> newProperty = joinProperty.append(getChainedProperty());

        if (isConstant()) {
            return getCanonical(newProperty, mOp, mConstant);
        } else {
            return getCanonical(newProperty, mOp, mBindID);
        }
    }

    @Override
    NotJoined notJoinedFrom(ChainedProperty<S> joinProperty,
                            Class<? extends Storable> joinPropertyType)
    {
        ChainedProperty<?> notJoinedProp = getChainedProperty();
        ChainedProperty<?> jp = joinProperty;

        while (notJoinedProp.getPrimeProperty().equals(jp.getPrimeProperty())) {
            notJoinedProp = notJoinedProp.tail();
            if (jp.getChainCount() == 0) {
                jp = null;
                break;
            }
            jp = jp.tail();
        }

        if (jp != null || notJoinedProp.equals(getChainedProperty())) {
            return super.notJoinedFrom(joinProperty, joinPropertyType);
        }

        PropertyFilter<?> notJoinedFilter;

        if (isConstant()) {
            notJoinedFilter = getCanonical(notJoinedProp, mOp, mConstant);
        } else {
            notJoinedFilter = getCanonical(notJoinedProp, mOp, mBindID);
        }

        return new NotJoined(notJoinedFilter, getOpenFilter(getStorableType()));
    }

    /**
     * Returns another PropertyFilter instance which is bound to the given constant value.
     *
     * @throws IllegalArgumentException if value is not compatible with property type
     */
    public PropertyFilter<S> constant(Object value) {
        if (mBindID == BOUND_CONSTANT) {
            if (mConstant == null) {
                if (value == null) {
                    return this;
                }
            } else if (mConstant.equals(value)) {
                return this;
            }
        }
        return getCanonical(mProperty, mOp, adaptValue(value));
    }

    /**
     * Returns the constant value of this PropertyFilter, which is valid only
     * if isConstant returns true.
     */
    public Object constant() {
        return mConstant;
    }

    /**
     * Returns true if this PropertyFilter has a constant value.
     */
    public boolean isConstant() {
        return mBindID == BOUND_CONSTANT;
    }

    void markBound() {
    }

    Filter<S> buildDisjunctiveNormalForm() {
        return this;
    }

    Filter<S> buildConjunctiveNormalForm() {
        return this;
    }

    boolean isDisjunctiveNormalForm() {
        return true;
    }

    boolean isConjunctiveNormalForm() {
        return true;
    }

    boolean isReduced() {
        return true;
    }

    void markReduced() {
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(int value) {
        Class<?> type = getBoxedType();
        if (type == Integer.class) {
            return Integer.valueOf(value);
        } else if (type == Long.class) {
            return Long.valueOf(value);
        } else if (type == Double.class) {
            return Double.valueOf(value);
        } else if (type == Number.class || type == Object.class) {
            return Integer.valueOf(value);
        }
        throw mismatch(int.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(long value) {
        Class<?> type = getBoxedType();
        if (type == Long.class) {
            return Long.valueOf(value);
        } else if (type == Number.class || type == Object.class) {
            return Long.valueOf(value);
        }
        throw mismatch(long.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(float value) {
        Class<?> type = getBoxedType();
        if (type == Float.class) {
            return Float.valueOf(value);
        } else if (type == Double.class) {
            return Double.valueOf(value);
        } else if (type == Number.class || type == Object.class) {
            return Float.valueOf(value);
        }
        throw mismatch(float.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(double value) {
        Class<?> type = getBoxedType();
        if (type == Double.class) {
            return Double.valueOf(value);
        } else if (type == Number.class || type == Object.class) {
            return Double.valueOf(value);
        }
        throw mismatch(float.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(boolean value) {
        Class<?> type = getBoxedType();
        if (type == Boolean.class || type == Object.class) {
            return Boolean.valueOf(value);
        }
        throw mismatch(boolean.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(char value) {
        Class<?> type = getBoxedType();
        if (type == Character.class || type == Object.class) {
            return Character.valueOf(value);
        } else if (type == String.class || type == CharSequence.class) {
            return String.valueOf(value);
        }
        throw mismatch(char.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(byte value) {
        Class<?> type = getBoxedType();
        if (type == Byte.class) {
            return Byte.valueOf(value);
        } else if (type == Short.class) {
            return Short.valueOf(value);
        } else if (type == Integer.class) {
            return Integer.valueOf(value);
        } else if (type == Long.class) {
            return Long.valueOf(value);
        } else if (type == Double.class) {
            return Double.valueOf(value);
        } else if (type == Float.class) {
            return Float.valueOf(value);
        } else if (type == Number.class || type == Object.class) {
            return Byte.valueOf(value);
        }
        throw mismatch(byte.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(short value) {
        Class<?> type = getBoxedType();
        if (type == Short.class) {
            return Short.valueOf(value);
        } else if (type == Integer.class) {
            return Integer.valueOf(value);
        } else if (type == Long.class) {
            return Long.valueOf(value);
        } else if (type == Double.class) {
            return Double.valueOf(value);
        } else if (type == Float.class) {
            return Float.valueOf(value);
        } else if (type == Number.class || type == Object.class) {
            return Short.valueOf(value);
        }
        throw mismatch(short.class, value);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(Object value) {
        if (getBoxedType().isInstance(value)) {
            return value;
        }

        Class<?> type = getType();

        if (value == null) {
            if (!type.isPrimitive()) {
                return value;
            }
        } else if (type.isPrimitive()) {
            TypeDesc actualPrim = TypeDesc.forClass(value.getClass()).toPrimitiveType();
            if (actualPrim != null) {
                if (type == actualPrim.toClass()) {
                    return value;
                }
                // Unbox and rebox.
                switch (actualPrim.getTypeCode()) {
                case TypeDesc.BYTE_CODE:
                    return adaptValue(((Number) value).byteValue());
                case TypeDesc.SHORT_CODE:
                    return adaptValue(((Number) value).shortValue());
                case TypeDesc.INT_CODE:
                    return adaptValue(((Number) value).intValue());
                case TypeDesc.LONG_CODE:
                    return adaptValue(((Number) value).longValue());
                case TypeDesc.FLOAT_CODE:
                    return adaptValue(((Number) value).floatValue());
                case TypeDesc.DOUBLE_CODE:
                    return adaptValue(((Number) value).doubleValue());
                case TypeDesc.BOOLEAN_CODE:
                    return adaptValue(((Boolean) value).booleanValue());
                case TypeDesc.CHAR_CODE:
                    return adaptValue(((Character) value).charValue());
                }
            }
        }

        throw mismatch(value == null ? null : value.getClass(), value);
    }

    @Override
    public int hashCode() {
        return mProperty.hashCode() * 31 + mOp.hashCode() + mBindID;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof PropertyFilter) {
            PropertyFilter<?> other = (PropertyFilter<?>) obj;
            return getStorableType() == other.getStorableType()
                && mProperty.equals(other.mProperty) && mOp.equals(other.mOp)
                && mBindID == other.mBindID
                && (mConstant == null
                    ? (other.mConstant == null)
                    : mConstant.equals(other.mConstant));
        }
        return false;
    }

    public void appendTo(Appendable app, FilterValues<S> values) throws IOException {
        mProperty.appendTo(app);
        app.append(' ');
        app.append(mOp.toString());
        app.append(' ');
        if (values != null) {
            Object value = values.getValue(this);
            if (value != null || values.isAssigned(this)) {
                app.append(String.valueOf(value));
                return;
            }
        }
        if (mBindID == BOUND_CONSTANT) {
            app.append(String.valueOf(mConstant));
        } else {
            app.append('?');
            if (mBindID > 1) {
                app.append('[').append(String.valueOf(mBindID)).append(']');
            }
        }
    }

    void appendMismatchMessage(Appendable a, Class<?> actualType, Object actualValue)
        throws IOException
    {
        if (actualType == null || actualValue == null) {
            a.append("Actual value is null, which cannot be assigned to type \"");
        } else {
            a.append("Actual value \"");
            a.append(String.valueOf(actualValue));
            a.append("\", of type \"");
            a.append(TypeDesc.forClass(actualType).getFullName());
            a.append("\", is incompatible with expected type of \"");
        }
        a.append(TypeDesc.forClass(getType()).getFullName());
        a.append('"');
    }

    private IllegalArgumentException mismatch(Class<?> actualType, Object actualValue) {
        StringBuilder b = new StringBuilder();
        try {
            appendMismatchMessage(b, actualType, actualValue);
        } catch (IOException e) {
            // Not gonna happen
        }
        return new IllegalArgumentException(b.toString());
    }
}
