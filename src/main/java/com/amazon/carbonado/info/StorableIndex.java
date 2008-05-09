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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.util.Appender;

/**
 * Represents an index that must be defined for a specific {@link Storable} type.
 *
 * @author Brian S O'Neill
 * @see com.amazon.carbonado.Index
 */
public class StorableIndex<S extends Storable> implements Appender {
    /**
     * Parses an index descriptor and returns an index object.
     *
     * @param desc name descriptor, as created by {@link #getNameDescriptor}
     * @param info info on storable type
     * @return index represented by descriptor
     * @throws IllegalArgumentException if error in descriptor syntax or if it
     * refers to unknown properties
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> StorableIndex<S> parseNameDescriptor
        (String desc, StorableInfo<S> info)
        throws IllegalArgumentException
    {
        String name = info.getStorableType().getName();
        if (!desc.startsWith(name)) {
            throw new IllegalArgumentException("Descriptor starts with wrong type name: \"" +
                                               desc + "\", \"" + name + '"');
        }

        Map<String, ? extends StorableProperty<S>> allProperties = info.getAllProperties();

        List<StorableProperty<S>> properties = new ArrayList<StorableProperty<S>>();
        List<Direction> directions = new ArrayList<Direction>();
        boolean unique;

        try {
            int pos = name.length();
            if (desc.charAt(pos++) != '~') {
                throw new IllegalArgumentException("Invalid syntax");
            }

            {
                int pos2 = nextSep(desc, pos);

                String attr = desc.substring(pos, pos2);
                if (attr.equals("U")) {
                    unique = true;
                } else if (attr.equals("N")) {
                    unique = false;
                } else {
                    throw new IllegalArgumentException("Unknown attribute");
                }

                pos = pos2;
            }

            while (pos < desc.length()) {
                char sign = desc.charAt(pos++);
                if (sign == '+') {
                    directions.add(Direction.ASCENDING);
                } else if (sign == '-') {
                    directions.add(Direction.DESCENDING);
                } else if (sign == '~') {
                    directions.add(Direction.UNSPECIFIED);
                } else {
                    throw new IllegalArgumentException("Unknown property direction");
                }

                int pos2 = nextSep(desc, pos);


                String propertyName = desc.substring(pos, pos2);
                StorableProperty<S> property = allProperties.get(propertyName);
                if (property == null) {
                    throw new IllegalArgumentException("Unknown property: " + propertyName);
                }
                properties.add(property);
                pos = pos2;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid syntax");
        }

        int size = properties.size();
        if (size == 0 || size != directions.size()) {
            throw new IllegalArgumentException("No properties specified");
        }

        StorableIndex<S> index = new StorableIndex<S>
            (properties.toArray(new StorableProperty[size]),
             directions.toArray(new Direction[size]));

        return index.unique(unique);
    }

    /**
     * Find the first subsequent occurrance of '+', '-', or '~' in the string
     * or the end of line if none are there
     * @param desc string to search
     * @param pos starting position in string
     * @return position of next separator, or end of string if none present
     */
    private static int nextSep(String desc, int pos) {
        int pos2 = desc.length();  // assume we'll find none
        int candidate = desc.indexOf('+', pos);
        if (candidate > 0) {
            pos2=candidate;
        }

        candidate = desc.indexOf('-', pos);
        if (candidate>0) {
            pos2 = Math.min(candidate, pos2);
        }

        candidate = desc.indexOf('~', pos);
        if (candidate>0) {
            pos2 = Math.min(candidate, pos2);
        }
        return pos2;
    }

    private final StorableProperty<S>[] mProperties;
    private final Direction[] mDirections;
    private final boolean mUnique;
    private final boolean mClustered;

    /**
     * Creates a StorableIndex from the given properties and matching
     * directions. Both arrays must match length.
     *
     * @throws IllegalArgumentException if any argument is null, if lengths
     * do not match, or if any length is zero.
     */
    public StorableIndex(StorableProperty<S>[] properties, Direction[] directions) {
        this(properties, directions, false);
    }

    /**
     * Creates a StorableIndex from the given properties and matching
     * directions. Both arrays must match length.  Allows specification of the
     * uniqueness of the index.
     *
     * @param properties
     * @param directions
     * @param unique
     */
    public StorableIndex(StorableProperty<S>[] properties,
                         Direction[] directions,
                         boolean unique)
    {
        this(properties, directions, unique, false, true);
    }

    /**
     * Creates a StorableIndex from the given properties and matching
     * directions. Both arrays must match length.  Allows specification of the
     * uniqueness of the index as well as clustered option.
     *
     * @param properties
     * @param directions
     * @param unique
     * @param clustered
     */
    public StorableIndex(StorableProperty<S>[] properties,
                         Direction[] directions,
                         boolean unique,
                         boolean clustered)
    {
        this(properties, directions, unique, clustered, true);
    }

    /**
     * The guts of it.  All the calls within this class specify doClone=false.
     * @param properties
     * @param directions
     * @param unique
     * @param clustered
     * @param doClone
     */
    private StorableIndex(StorableProperty<S>[] properties,
                          Direction[] directions,
                          boolean unique,
                          boolean clustered,
                          boolean doClone) {
        if (properties == null || directions == null) {
            throw new IllegalArgumentException();
        }
        if (properties.length != directions.length) {
            throw new IllegalArgumentException();
        }
        if (properties.length < 1) {
            throw new IllegalArgumentException();
        }
        mProperties = doClone ? properties.clone() : properties;
        mDirections = doClone ? directions.clone() : directions;

        mUnique = unique;
        mClustered = clustered;
    }

    /**
     * Creates a StorableIndex from a StorableKey.
     *
     * @param direction optional direction to apply to each key property that
     * has an unspecified direction
     * @throws IllegalArgumentException if key is null or it has
     * no properties
     */
    @SuppressWarnings("unchecked")
    public StorableIndex(StorableKey<S> key, Direction direction) {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        Set<? extends OrderedProperty<S>> properties = key.getProperties();
        if (properties.size() < 1) {
            throw new IllegalArgumentException();
        }

        if (direction == null) {
            direction = Direction.UNSPECIFIED;
        }

        mProperties = new StorableProperty[properties.size()];
        mDirections = new Direction[properties.size()];

        int i = 0;
        for (OrderedProperty<S> prop : properties) {
            mProperties[i] = prop.getChainedProperty().getPrimeProperty();
            if (prop.getDirection() == Direction.UNSPECIFIED) {
                mDirections[i] = direction;
            } else {
                mDirections[i] = prop.getDirection();
            }
            i++;
        }

        mUnique = true;
        mClustered = false;
    }

    /**
     * Creates a StorableIndex from OrderedProperties.
     *
     * @param direction optional direction to apply to each property that
     * has an unspecified direction
     * @throws IllegalArgumentException if no properties supplied
     */
    @SuppressWarnings("unchecked")
    public StorableIndex(OrderedProperty<S>[] properties, Direction direction) {
        if (properties == null || properties.length == 0) {
            throw new IllegalArgumentException();
        }

        if (direction == null) {
            direction = Direction.UNSPECIFIED;
        }

        mProperties = new StorableProperty[properties.length];
        mDirections = new Direction[properties.length];

        int i = 0;
        for (OrderedProperty<S> prop : properties) {
            mProperties[i] = prop.getChainedProperty().getPrimeProperty();
            if (prop.getDirection() == Direction.UNSPECIFIED) {
                mDirections[i] = direction;
            } else {
                mDirections[i] = prop.getDirection();
            }
            i++;
        }

        mUnique = false;
        mClustered = false;
    }

    /**
     * Creates a StorableIndex from an IndexInfo.
     *
     * @param type type of storable index is defined for
     * @param indexInfo IndexInfo returned from storage object
     * @throws IllegalArgumentException if any argument is null, if any
     * properties are invalid, or if index info has no properties
     */
    @SuppressWarnings("unchecked")
    public StorableIndex(Class<S> type, IndexInfo indexInfo) {
        if (indexInfo == null) {
            throw new IllegalArgumentException();
        }

        Map<String, ? extends StorableProperty<S>> allProperties =
            StorableIntrospector.examine(type).getAllProperties();
        String[] propertyNames = indexInfo.getPropertyNames();
        if (propertyNames.length == 0) {
            throw new IllegalArgumentException("No properties in index info");
        }

        mProperties = new StorableProperty[propertyNames.length];
        for (int i=0; i<propertyNames.length; i++) {
            StorableProperty<S> property = allProperties.get(propertyNames[i]);
            if (property == null) {
                throw new IllegalArgumentException("Property not found: " + propertyNames[i]);
            }
            mProperties[i] = property;
        }

        mDirections = indexInfo.getPropertyDirections();
        mUnique = indexInfo.isUnique();
        mClustered = indexInfo.isClustered();
    }

    /**
     * Returns the type of storable this index applies to.
     */
    public Class<S> getStorableType() {
        return getProperty(0).getEnclosingType();
    }

    /**
     * Returns the count of properties in this index.
     */
    public int getPropertyCount() {
        return mProperties.length;
    }

    /**
     * Returns a specific property in this index.
     */
    public StorableProperty<S> getProperty(int index) {
        return mProperties[index];
    }

    /**
     * Returns a new array with all the properties in it.
     */
    public StorableProperty<S>[] getProperties() {
        return mProperties.clone();
    }

    /**
     * Returns the requested direction of a specific property in this index.
     */
    public Direction getPropertyDirection(int index) {
        return mDirections[index];
    }

    /**
     * Returns a new array with all the property directions in it.
     */
    public Direction[] getPropertyDirections() {
        return mDirections.clone();
    }

    /**
     * Returns a specific property in this index, with the direction folded in.
     */
    public OrderedProperty<S> getOrderedProperty(int index) {
        return OrderedProperty.get(mProperties[index], mDirections[index]);
    }

    /**
     * Returns a new array with all the properties in it, with directions
     * folded in.
     */
    @SuppressWarnings("unchecked")
    public OrderedProperty<S>[] getOrderedProperties() {
        OrderedProperty<S>[] ordered = new OrderedProperty[mProperties.length];
        for (int i=mProperties.length; --i>=0; ) {
            ordered[i] = OrderedProperty.get(mProperties[i], mDirections[i]);
        }
        return ordered;
    }

    public boolean isUnique() {
        return mUnique;
    }

    /**
     * Returns true if index is known to be clustered, which means it defines
     * the physical ordering of storables.
     */
    public boolean isClustered() {
        return mClustered;
    }

    /**
     * Returns a StorableIndex instance which is unique or not.
     */
    public StorableIndex<S> unique(boolean unique) {
        if (unique == mUnique) {
            return this;
        }
        return new StorableIndex<S>(mProperties, mDirections, unique, mClustered, false);
    }

    /**
     * Returns a StorableIndex instance which is clustered or not.
     */
    public StorableIndex<S> clustered(boolean clustered) {
        if (clustered == mClustered) {
            return this;
        }
        return new StorableIndex<S>(mProperties, mDirections, mUnique, clustered, false);
    }

    /**
     * Returns a StorableIndex instance with all the properties reversed.
     */
    public StorableIndex<S> reverse() {
        Direction[] directions = mDirections;

        specified: {
            for (int i=directions.length; --i>=0; ) {
                if (directions[i] != Direction.UNSPECIFIED) {
                    break specified;
                }
            }
            // Completely unspecified direction, so nothing to reverse.
            return this;
        }

        directions = directions.clone();
        for (int i=directions.length; --i>=0; ) {
            directions[i] = directions[i].reverse();
        }

        return new StorableIndex<S>(mProperties, directions, mUnique, mClustered, false);
    }

    /**
     * Returns a StorableIndex instance with all unspecified directions set to
     * the given direction. Returns this if all directions are already
     * specified.
     *
     * @param direction direction to replace all unspecified directions
     */
    public StorableIndex<S> setDefaultDirection(Direction direction) {
        Direction[] directions = mDirections;

        unspecified: {
            for (int i=directions.length; --i>=0; ) {
                if (directions[i] == Direction.UNSPECIFIED) {
                    break unspecified;
                }
            }
            // Completely specified direction, so nothing to alter.
            return this;
        }

        directions = directions.clone();
        for (int i=directions.length; --i>=0; ) {
            if (directions[i] == Direction.UNSPECIFIED) {
                directions[i] = direction;
            }
        }

        return new StorableIndex<S>(mProperties, directions, mUnique, mClustered, false);
    }

    /**
     * Returns a StorableIndex with the given property added. If this index
     * already contained the given property (regardless of sort direction),
     * this index is returned.
     *
     * @param property property to add unless already in this index
     * @param direction direction to apply to property, if added
     * @return new index with added property or this if index already contained property
     */
    public StorableIndex<S> addProperty(StorableProperty<S> property, Direction direction) {
        for (int i=mProperties.length; --i>=0; ) {
            if (mProperties[i].equals(property)) {
                return this;
            }
        }

        StorableProperty<S>[] properties = new StorableProperty[mProperties.length + 1];
        Direction[] directions = new Direction[mDirections.length + 1];

        System.arraycopy(mProperties, 0, properties, 0, mProperties.length);
        System.arraycopy(mDirections, 0, directions, 0, mDirections.length);

        properties[properties.length - 1] = property;
        directions[directions.length - 1] = direction;

        return new StorableIndex<S>(properties, directions, mUnique, mClustered, false);
    }

    /**
     * Returns a StorableIndex which is unique, possibly by appending
     * properties from the given key. If index is already unique, it is
     * returned as-is.
     */
    public StorableIndex<S> uniquify(StorableKey<S> key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        if (isUnique()) {
            return this;
        }

        StorableIndex<S> index = this;

        for (OrderedProperty<S> keyProp : key.getProperties()) {
            index = index.addProperty
                (keyProp.getChainedProperty().getPrimeProperty(), keyProp.getDirection());
        }

        return index.unique(true);
    }

    /**
     * Converts this index into a parseable name descriptor string, whose
     * general format is:
     *
     * <p>{@code <storable type>~<attr><+|-|~><property><+|-|~><property>...}
     *
     * <p>Attr is "U" for a unique index, "N" for a non-unique index.
     *
     * <p>Example: {@code my.pkg.UserInfo~N+lastName+firstName-birthDate}
     *
     * @see #parseNameDescriptor(String, StorableInfo)
     */
    public String getNameDescriptor() {
        StringBuilder b = new StringBuilder();
        b.append(getStorableType().getName());
        b.append('~');
        b.append(isUnique() ? 'U': 'N');

        int count = getPropertyCount();
        for (int i=0; i<count; i++) {
            b.append(getPropertyDirection(i).toCharacter());
            b.append(getProperty(i).getName());
        }

        return b.toString();
    }

    /**
     * Converts this index into a parseable type descriptor string, which
     * basically consists of Java type descriptors appended together. There is
     * one slight difference. Types which may be null are prefixed with a 'N'
     * character.
     */
    public String getTypeDescriptor() {
        StringBuilder b = new StringBuilder();

        int count = getPropertyCount();
        for (int i=0; i<count; i++) {
            StorableProperty property = getProperty(i);
            if (property.isNullable()) {
                b.append('N');
            }
            b.append(TypeDesc.forClass(property.getType()).getDescriptor());
        }

        return b.toString();
    }

    @Override
    public int hashCode() {
        return (mUnique ? 0 : 31)
            + Arrays.hashCode(mProperties) * 31
            + Arrays.hashCode(mDirections);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof StorableIndex) {
            StorableIndex<?> other = (StorableIndex<?>) obj;
            return isUnique() == other.isUnique()
                && isClustered() == other.isClustered()
                && Arrays.equals(mProperties, other.mProperties)
                && Arrays.equals(mDirections, other.mDirections);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("StorableIndex ");
        try {
            appendTo(b);
        } catch (IOException e) {
            // Not gonna happen.
        }
        return b.toString();
    }

    /**
     * Appends the same results as toString, but without the "StorableIndex"
     * prefix.
     */
    public void appendTo(Appendable app) throws IOException {
        app.append("{properties=[");
        int length = mProperties.length;
        for (int i=0; i<length; i++) {
            if (i > 0) {
                app.append(", ");
            }
            app.append(mDirections[i].toCharacter());
            app.append(mProperties[i].getName());
        }
        app.append(']');
        app.append(", unique=");
        app.append(String.valueOf(isUnique()));
        app.append('}');
    }
}
