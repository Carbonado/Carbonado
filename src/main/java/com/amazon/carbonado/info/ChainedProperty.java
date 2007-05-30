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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cojen.util.WeakCanonicalSet;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.util.Appender;

/**
 * Represents a property to query against or to order by. Properties may be
 * specified in a simple form, like "firstName", or in a chained form, like
 * "address.state". In both forms, the first property is the "prime"
 * property. All properties that follow are chained.
 *
 * @author Brian S O'Neill
 */
public class ChainedProperty<S extends Storable> implements Appender {
    static WeakCanonicalSet cCanonical = new WeakCanonicalSet();

    /**
     * Returns a canonical instance which has no chain.
     *
     * @throws IllegalArgumentException if prime is null
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> ChainedProperty<S> get(StorableProperty<S> prime) {
        return (ChainedProperty<S>) cCanonical.put(new ChainedProperty<S>(prime, null));
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
        return (ChainedProperty<S>) cCanonical.put(new ChainedProperty<S>(prime, chain));
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
            name = str;
        } else {
            name = str.substring(pos, dot);
            pos = dot + 1;
        }

        StorableProperty<S> prime = info.getAllProperties().get(name);

        if (prime == null) {
            throw new IllegalArgumentException
                ("Property \"" + name + "\" not found for type: \"" +
                 info.getStorableType().getName() + '"');
        }

        if (pos <= 0) {
            return get(prime);
        }

        List<StorableProperty<?>> chain = new ArrayList<StorableProperty<?>>(4);
        Class<?> type = prime.getType();

        while (pos > 0) {
            dot = str.indexOf('.', pos);
            if (dot < 0) {
                name = str.substring(pos);
                pos = -1;
            } else {
                name = str.substring(pos, dot);
                pos = dot + 1;
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

        return get(prime,
                   (StorableProperty<?>[]) chain.toArray(new StorableProperty[chain.size()]));
    }

    private final StorableProperty<S> mPrime;
    private final StorableProperty<?>[] mChain;

    /**
     * @param prime must not be null
     * @param chain can be null if none
     * @throws IllegalArgumentException if prime is null
     */
    private ChainedProperty(StorableProperty<S> prime, StorableProperty<?>[] chain) {
        if (prime == null) {
            throw new IllegalArgumentException();
        }
        mPrime = prime;
        mChain = (chain == null || chain.length == 0) ? null : chain.clone();
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

    public StorableProperty<?> getChainedProperty(int index) throws IndexOutOfBoundsException {
        if (mChain == null) {
            throw new IndexOutOfBoundsException();
        } else {
            return mChain[index];
        }
    }

    /**
     * Returns a new ChainedProperty with another property appended.
     */
    public ChainedProperty<S> append(StorableProperty<?> property) {
        StorableProperty<?>[] newChain = new StorableProperty[getChainCount() + 1];
        if (newChain.length > 1) {
            System.arraycopy(mChain, 0, newChain, 0, mChain.length);
        }
        newChain[newChain.length - 1] = property;
        return get(mPrime, newChain);
    }

    /**
     * Returns a new ChainedProperty with another property appended.
     */
    public ChainedProperty<S> append(ChainedProperty<?> property) {
        final int propChainCount = property.getChainCount();
        if (propChainCount == 0) {
            return append(property.getPrimeProperty());
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

        return get(mPrime, newChain);
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
            return get(mPrime);
        }
        StorableProperty<?>[] newChain = new StorableProperty[getChainCount() - 1];
        System.arraycopy(mChain, 0, newChain, 0, newChain.length);
        return get(mPrime, newChain);
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
            return get(mChain[0]);
        }
        StorableProperty<?>[] newChain = new StorableProperty[getChainCount() - 1];
        System.arraycopy(mChain, 1, newChain, 0, newChain.length);
        return get(mChain[0], newChain);
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
            return getType() == other.getType() && mPrime == other.mPrime
                && identityEquals(mChain, other.mChain);
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
     * Returns the chained property in a parseable form. The format is
     * "name.subname.subsubname".
     */
    @Override
    public String toString() {
        if (mChain == null) {
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
     * Appends the chained property in a parseable form. The format is
     * "name.subname.subsubname".
     */
    public void appendTo(Appendable app) throws IOException {
        app.append(mPrime.getName());
        StorableProperty<?>[] chain = mChain;
        if (chain != null) {
            app.append('.');
            for (int i=0; i<chain.length; i++) {
                if (i > 0) {
                    app.append('.');
                }
                app.append(chain[i].getName());
            }
        }
    }
}
