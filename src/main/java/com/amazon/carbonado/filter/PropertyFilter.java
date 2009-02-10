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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collections;
import java.util.List;

import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.util.Converter;

/**
 * Filter tree node that performs a relational test against a specific property
 * value.
 *
 * @author Brian S O'Neill
 */
public class PropertyFilter<S extends Storable> extends Filter<S> {
    private static final long serialVersionUID = 1L;

    // Indicates property has been bound to a constant value.
    private static final int BOUND_CONSTANT = -1;

    private static final Converter cConverter;

    static {
        cConverter = Converter.build(Hidden.Adapter.class);
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
        if (property == null || op == null) {
            throw new IllegalArgumentException();
        }
        if (property.isOuterJoin(property.getChainCount())) {
            throw new IllegalArgumentException
                ("Last property in chain cannot be an outer join: " + property);
        }
        mProperty = property;
        mOp = op;
        mBindID = bindID;
        mConstant = constant;
    }

    @Override
    public PropertyFilter<S> not() {
        ChainedProperty<S> property = mProperty;

        if (property.getChainCount() > 0) {
            // Flip inner/outer joins.

            int chainCount = property.getChainCount();
            StorableProperty<?>[] chain = new StorableProperty[chainCount];
            for (int i=0; i<chainCount; i++) {
                chain[i] = property.getChainedProperty(i);
            }

            boolean[] outerJoin = null;
            // Flip all but the last property in the chain.
            for (int i=0; i<chainCount; i++) {
                if (!property.isOuterJoin(i)) {
                    if (outerJoin == null) {
                        outerJoin = new boolean[chainCount + 1];
                    }
                    outerJoin[i] = true;
                }
            }

            property = ChainedProperty.get(property.getPrimeProperty(), chain, outerJoin);
        }

        if (mBindID == BOUND_CONSTANT) {
            return getCanonical(property, mOp.reverse(), mConstant);
        } else {
            return getCanonical(property, mOp.reverse(), mBindID);
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

    @Override
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

    @Override
    public PropertyFilter<S> bind() {
        return mBindID == 0 ? getCanonical(this, 1) : this;
    }

    @Override
    public PropertyFilter<S> unbind() {
        return mBindID == 0 ? this : getCanonical(this, 0);
    }

    @Override
    public boolean isBound() {
        return mBindID != 0;
    }

    @Override
    public <T extends Storable> PropertyFilter<T> asJoinedFromAny(ChainedProperty<T> joinProperty){
        ChainedProperty<T> newProperty = joinProperty.append(getChainedProperty());

        if (isConstant()) {
            return getCanonical(newProperty, mOp, mConstant);
        } else {
            return getCanonical(newProperty, mOp, mBindID);
        }
    }

    @Override
    NotJoined notJoinedFromCNF(ChainedProperty<S> joinProperty) {
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
            return super.notJoinedFromCNF(joinProperty);
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

    @Override
    void markBound() {
    }

    @Override
    Filter<S> buildDisjunctiveNormalForm() {
        return this;
    }

    @Override
    Filter<S> buildConjunctiveNormalForm() {
        return this;
    }

    @Override
    boolean isDisjunctiveNormalForm() {
        return true;
    }

    @Override
    boolean isConjunctiveNormalForm() {
        return true;
    }

    @Override
    boolean isReduced() {
        return true;
    }

    @Override
    void markReduced() {
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(int value) {
        return cConverter.convert(value, getType());
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(long value) {
        return cConverter.convert(value, getType());
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(float value) {
        return cConverter.convert(value, getType());
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(double value) {
        return cConverter.convert(value, getType());
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(boolean value) {
        return cConverter.convert(value, getType());
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(char value) {
        return cConverter.convert(value, getType());
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(byte value) {
        return cConverter.convert(value, getType());
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(short value) {
        return cConverter.convert(value, Object.class);
    }

    /**
     * @throws IllegalArgumentException if type doesn't match
     */
    Object adaptValue(Object value) {
        return cConverter.convert(value, getType());
    }

    @Override
    int generateHashCode() {
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

    @Override
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

    private static class Hidden {
        public static abstract class Adapter extends Converter {
            public String convertToString(char value) {
                return String.valueOf(value);
            }

            public CharSequence convertToCharSequence(char value) {
                return String.valueOf(value);
            }

            public String convertToString(StringBuffer value) {
                return value.toString();
            }

            public String convertToString(StringBuilder value) {
                return value.toString();
            }

            public BigInteger convertToBigInteger(long value) {
                return BigInteger.valueOf(value);
            }

            public BigDecimal convertToBigDecimal(long value) {
                if (value > -10 && value < 10) {
                    return BigDecimal.valueOf(value);
                }
                // Normalize value.
                return BigDecimal.valueOf(value).stripTrailingZeros();
            }

            public BigDecimal convertToBigDecimal(double value) {
                if (value == 0) {
                    return BigDecimal.ZERO;
                }
                // Normalize value.
                return BigDecimal.valueOf(value).stripTrailingZeros();
            }

            public BigDecimal convertToBigDecimal(BigInteger value) {
                if (BigInteger.ZERO.equals(value)) {
                    return BigDecimal.ZERO;
                }
                // Normalize value.
                return new BigDecimal(value, 0).stripTrailingZeros();
            }

            public BigDecimal convertToBigDecimal(BigDecimal value) {
                if (value.compareTo(BigDecimal.ZERO) == 0) {
                    return BigDecimal.ZERO;
                }
                // Normalize value.
                return value.stripTrailingZeros();
            }
        }
    }
}
