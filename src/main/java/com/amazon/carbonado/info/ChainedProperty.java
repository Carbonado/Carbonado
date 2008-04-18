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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cojen.util.WeakCanonicalSet;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.util.Appender;

/**
 * Represents a property to filter on or to order by. Properties may be
 * specified in a simple form, like "firstName", or in a chained form, like
 * "address.state". In both forms, the first property is the "prime"
 * property. All properties that follow are chained.
 *
 * @author Brian S O'Neill
 */
public class ChainedProperty<S extends Storable> implements Serializable, Appender {
    private static final long serialVersionUID = 1L;

    static WeakCanonicalSet cCanonical = new WeakCanonicalSet();

    /**
     * Returns a canonical instance which has no chain.
     *
     * @throws IllegalArgumentException if prime is null
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> ChainedProperty<S> get(StorableProperty<S> prime) {
        return (ChainedProperty<S>) cCanonical.put(new ChainedProperty<S>(prime, null, null));
    }

    /**
     * Returns a canonical instance.
     *
     * @throws IllegalArgumentException if prime is null or if chained
     * properties are not formed properly
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> ChainedProperty<S> get(StorableProperty<S> prime,
                                                              StorableProperty<?>... chain) {
        return (ChainedProperty<S>) cCanonical.put(new ChainedProperty<S>(prime, chain, null));
    }

    /**
     * Returns a canonical instance.
     *
     * @throws IllegalArgumentException if prime is null or if chained
     * properties are not formed properly
     * @since 1.2
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> ChainedProperty<S> get(StorableProperty<S> prime,
                                                              StorableProperty<?>[] chain,
                                                              boolean[] outerJoin) {
        return (ChainedProperty<S>) cCanonical.put
            (new ChainedProperty<S>(prime, chain, outerJoin));
    }

    /**
     * Parses a chained property.
     *
     * @param info Info for Storable type containing property
     * @param str string to parse
     * @throws IllegalArgumentException if any parameter is null or string
     * format is incorrect
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> ChainedProperty<S> parse(StorableInfo<S> info, String str)
        throws IllegalArgumentException
    {
        if (info == null || str == null) {
            throw new IllegalArgumentException();
        }

        int pos = 0;
        int dot = str.indexOf('.', pos);

        String name;
        if (dot < 0) {
            name = str.trim();
        } else {
            name = str.substring(pos, dot).trim();
            pos = dot + 1;
        }

        List<Boolean> outerJoinList = null;

        if (name.startsWith("(") && name.endsWith(")")) {
            outerJoinList = new ArrayList<Boolean>(4);
            outerJoinList.add(true);
            name = name.substring(1, name.length() - 1).trim();
        }

        StorableProperty<S> prime = info.getAllProperties().get(name);

        if (prime == null) {
            throw new IllegalArgumentException
                ("Property \"" + name + "\" not found for type: \"" +
                 info.getStorableType().getName() + '"');
        }

        if (pos <= 0) {
            if (outerJoinList == null || !outerJoinList.get(0)) {
                return get(prime);
            } else {
                return get(prime, null, new boolean[] {true});
            }
        }

        List<StorableProperty<?>> chain = new ArrayList<StorableProperty<?>>(4);
        Class<?> type = prime.getType();

        while (pos > 0) {
            dot = str.indexOf('.', pos);
            if (dot < 0) {
                name = str.substring(pos).trim();
                pos = -1;
            } else {
                name = str.substring(pos, dot).trim();
                pos = dot + 1;
            }

            if (name.startsWith("(") && name.endsWith(")")) {
                if (outerJoinList == null) {
                    outerJoinList = new ArrayList<Boolean>(4);
                    // Fill in false values.
                    outerJoinList.add(false); // prime is inner join
                    for (int i=chain.size(); --i>=0; ) {
                        outerJoinList.add(false);
                    }
                }
                outerJoinList.add(true);
                name = name.substring(1, name.length() - 1).trim();
            } else if (outerJoinList != null) {
                outerJoinList.add(false);
            }

            if (Storable.class.isAssignableFrom(type)) {
                StorableInfo propInfo =
                    StorableIntrospector.examine((Class<? extends Storable>) type);
                Map<String, ? extends StorableProperty<?>> props = propInfo.getAllProperties();
                StorableProperty<?> prop = props.get(name);
                if (prop == null) {
                    throw new IllegalArgumentException
                        ("Property \"" + name + "\" not found for type: \"" +
                         type.getName() + '"');
                }
                chain.add(prop);
                type = prop.isJoin() ? prop.getJoinedType() : prop.getType();
            } else {
                throw new IllegalArgumentException
                    ("Property \"" + name + "\" not found for type \"" +
                     type.getName() + "\" because it has no properties");
            }
        }

        boolean[] outerJoin = null;
        if (outerJoinList != null) {
            outerJoin = new boolean[outerJoinList.size()];
            for (int i=outerJoinList.size(); --i>=0; ) {
                outerJoin[i] = outerJoinList.get(i);
            }
        }

        return get(prime,
                   (StorableProperty<?>[]) chain.toArray(new StorableProperty[chain.size()]),
                   outerJoin);
    }

    private final StorableProperty<S> mPrime;
    private final StorableProperty<?>[] mChain;
    private final boolean[] mOuterJoin;

    /**
     * @param prime must not be null
     * @param chain can be null if none
     * @param outerJoin can be null for all inner joins
     * @throws IllegalArgumentException if prime is null or if outer join chain is too long
     */
    private ChainedProperty(StorableProperty<S> prime, StorableProperty<?>[] chain,
                            boolean[] outerJoin)
    {
        if (prime == null) {
            throw new IllegalArgumentException("No prime property");
        }

        mPrime = prime;
        mChain = (chain == null || chain.length == 0) ? null : chain.clone();

        if (outerJoin != null) {
            int expectedLength = (chain == null ? 0 : chain.length) + 1;
            if (outerJoin.length > expectedLength) {
                throw new IllegalArgumentException
                    ("Outer join array too long: " + outerJoin.length + " > " + expectedLength);
            }
            boolean[] newOuterJoin = new boolean[expectedLength];
            System.arraycopy(outerJoin, 0, newOuterJoin, 0, outerJoin.length);
            outerJoin = newOuterJoin;
        }

        mOuterJoin = outerJoin;
    }

    public StorableProperty<S> getPrimeProperty() {
        return mPrime;
    }

    /**
     * Returns the type of the last property in the chain, or of the prime
     * property if the chain is empty.
     */
    public Class<?> getType() {
        return getLastProperty().getType();
    }

    /**
     * Returns true if any property in the chain can be null.
     *
     * @see com.amazon.carbonado.Nullable
     * @since 1.2
     */
    public boolean isNullable() {
        if (mPrime.isNullable()) {
            return true;
        }
        if (mChain != null) {
            for (StorableProperty<?> prop : mChain) {
                if (prop.isNullable()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns true if any property in the chain is derived.
     *
     * @see com.amazon.carbonado.Derived
     * @since 1.2
     */
    public boolean isDerived() {
        if (mPrime.isDerived()) {
            return true;
        }
        if (mChain != null) {
            for (StorableProperty<?> prop : mChain) {
                if (prop.isDerived()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the last property in the chain, or the prime property if chain
     * is empty.
     */
    public StorableProperty<?> getLastProperty() {
        return mChain == null ? mPrime : mChain[mChain.length - 1];
    }

    /**
     * Returns amount of properties chained from prime property, which may be
     * zero.
     */
    public int getChainCount() {
        return mChain == null ? 0 : mChain.length;
    }

    /**
     * @param index valid range is 0 to chainCount - 1
     */
    public StorableProperty<?> getChainedProperty(int index) throws IndexOutOfBoundsException {
        if (mChain == null) {
            throw new IndexOutOfBoundsException();
        } else {
            return mChain[index];
        }
    }

    /**
     * Returns true if the property at the given index should be treated as an
     * outer join. Index zero is the prime property.
     *
     * @param index valid range is 0 to chainCount
     * @since 1.2
     */
    public boolean isOuterJoin(int index) throws IndexOutOfBoundsException {
        if (index < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (mOuterJoin == null) {
            if (index > getChainCount()) {
                throw new IndexOutOfBoundsException();
            }
            return false;
        }
        return mOuterJoin[index];
    }

    /**
     * Returns a new ChainedProperty with another property appended.
     */
    public ChainedProperty<S> append(StorableProperty<?> property) {
        return append(property, false);
    }

    /**
     * Returns a new ChainedProperty with another property appended.
     *
     * @param outerJoin pass true for outer join
     * @since 1.2
     */
    public ChainedProperty<S> append(StorableProperty<?> property, boolean outerJoin) {
        if (property == null) {
            throw new IllegalArgumentException();
        }

        StorableProperty<?>[] newChain = new StorableProperty[getChainCount() + 1];
        if (newChain.length > 1) {
            System.arraycopy(mChain, 0, newChain, 0, mChain.length);
        }
        newChain[newChain.length - 1] = property;

        boolean[] newOuterJoin = mOuterJoin;

        if (outerJoin) {
            newOuterJoin = new boolean[newChain.length + 1];
            if (mOuterJoin != null) {
                System.arraycopy(mOuterJoin, 0, newOuterJoin, 0, mOuterJoin.length);
            }
            newOuterJoin[newOuterJoin.length - 1] = true;
        }

        return get(mPrime, newChain, newOuterJoin);
    }

    /**
     * Returns a new ChainedProperty with another property appended.
     */
    public ChainedProperty<S> append(ChainedProperty<?> property) {
        if (property == null) {
            throw new IllegalArgumentException();
        }

        final int propChainCount = property.getChainCount();
        if (propChainCount == 0) {
            return append(property.getPrimeProperty(), property.isOuterJoin(0));
        }

        StorableProperty<?>[] newChain =
            new StorableProperty[getChainCount() + 1 + propChainCount];

        int pos = 0;
        if (getChainCount() > 0) {
            System.arraycopy(mChain, 0, newChain, 0, mChain.length);
            pos = mChain.length;
        }

        newChain[pos++] = property.getPrimeProperty();
        for (int i=0; i<propChainCount; i++) {
            newChain[pos++] = property.getChainedProperty(i);
        }

        boolean[] newOuterJoin = mOuterJoin;

        if (property.mOuterJoin != null) {
            newOuterJoin = new boolean[newChain.length + 1];
            if (mOuterJoin != null) {
                System.arraycopy(mOuterJoin, 0, newOuterJoin, 0, mOuterJoin.length);
            }
            System.arraycopy(property.mOuterJoin, 0,
                             newOuterJoin, getChainCount() + 1,
                             property.mOuterJoin.length);
        }

        return get(mPrime, newChain, newOuterJoin);
    }

    /**
     * Returns a new ChainedProperty with the last property in the chain removed.
     *
     * @throws IllegalStateException if chain count is zero
     */
    public ChainedProperty<S> trim() {
        if (getChainCount() == 0) {
            throw new IllegalStateException();
        }
        if (getChainCount() == 1) {
            if (!isOuterJoin(0)) {
                return get(mPrime);
            } else {
                return get(mPrime, null, new boolean[] {true});
            }
        }
        StorableProperty<?>[] newChain = new StorableProperty[getChainCount() - 1];
        System.arraycopy(mChain, 0, newChain, 0, newChain.length);

        boolean[] newOuterJoin = mOuterJoin;

        if (newOuterJoin != null && newOuterJoin.length > (newChain.length + 1)) {
            newOuterJoin = new boolean[newChain.length + 1];
            System.arraycopy(mOuterJoin, 0, newOuterJoin, 0, newChain.length + 1);
        }

        return get(mPrime, newChain, newOuterJoin);
    }

    /**
     * Returns a new ChainedProperty which contains everything that follows
     * this ChainedProperty's prime property.
     *
     * @throws IllegalStateException if chain count is zero
     */
    public ChainedProperty<?> tail() {
        if (getChainCount() == 0) {
            throw new IllegalStateException();
        }
        if (getChainCount() == 1) {
            if (!isOuterJoin(1)) {
                return get(mChain[0]);
            } else {
                return get(mChain[0], null, new boolean[] {true});
            }
        }
        StorableProperty<?>[] newChain = new StorableProperty[getChainCount() - 1];
        System.arraycopy(mChain, 1, newChain, 0, newChain.length);

        boolean[] newOuterJoin = mOuterJoin;

        if (newOuterJoin != null) {
            newOuterJoin = new boolean[newChain.length + 1];
            System.arraycopy(mOuterJoin, 1, newOuterJoin, 0, mOuterJoin.length - 1);
        }

        return get(mChain[0], newChain, newOuterJoin);
    }

    @Override
    public int hashCode() {
        int hash = mPrime.hashCode();
        StorableProperty<?>[] chain = mChain;
        if (chain != null) {
            for (int i=chain.length; --i>=0; ) {
                hash = hash * 31 + chain[i].hashCode();
            }
        }
        boolean[] outerJoin = mOuterJoin;
        if (outerJoin != null) {
            for (int i=outerJoin.length; --i>=0; ) {
                if (outerJoin[i]) {
                    hash += 1;
                }
            }
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ChainedProperty) {
            ChainedProperty<?> other = (ChainedProperty<?>) obj;
            // Note: Since StorableProperty instances are not canonicalized,
            // they must be compared with the '==' operator instead of the
            // equals method. Otherwise, canonical ChainedProperty instances
            // may refer to StorableProperty instances which are no longer
            // available through the Introspector.
            if (getType() == other.getType() && mPrime == other.mPrime
                && identityEquals(mChain, other.mChain))
            {
                // Compare outer joins.
                int count = getChainCount() + 1;
                for (int i=0; i<count; i++) {
                    if (isOuterJoin(i) != other.isOuterJoin(i)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Compares objects for equality using '==' operator instead of equals method.
     */
    private static boolean identityEquals(Object[] a, Object[] a2) {
        if (a == a2) {
            return true;
        }
        if (a == null || a2 == null) {
            return false;
        }

        int length = a.length;
        if (a2.length != length) {
            return false;
        }

        for (int i=0; i<length; i++) {
            if (a[i] != a2[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the chained property formatted as "name.subname.subsubname".
     * This format is parseable only if the chain is composed of valid
     * many-to-one joins.
     */
    @Override
    public String toString() {
        if (mChain == null && !isOuterJoin(0)) {
            return mPrime.getName();
        }
        StringBuilder buf = new StringBuilder();
        try {
            appendTo(buf);
        } catch (IOException e) {
            // Not gonna happen.
        }
        return buf.toString();
    }

    /**
     * Appends the chained property formatted as "name.subname.subsubname".
     * This format is parseable only if the chain is composed of valid
     * many-to-one joins.
     */
    public void appendTo(Appendable app) throws IOException {
        appendPropTo(app, mPrime.getName(), isOuterJoin(0));
        StorableProperty<?>[] chain = mChain;
        if (chain != null) {
            for (int i=0; i<chain.length; i++) {
                app.append('.');
                appendPropTo(app, chain[i].getName(), isOuterJoin(i + 1));
            }
        }
    }

    private void appendPropTo(Appendable app, String name, boolean outer) throws IOException {
        if (outer) {
            app.append('(');
        }
        app.append(name);
        if (outer) {
            app.append(')');
        }
    }

    private Object readResolve() {
        return cCanonical.put(this);
    }
}
