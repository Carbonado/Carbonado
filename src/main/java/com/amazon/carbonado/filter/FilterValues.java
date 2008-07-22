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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.IdentityHashMap;
import java.util.Map;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.util.Appender;

/**
 * Assigns values to {@link Filter} placeholders. FilterValues instances are
 * immutable.
 *
 * @author Brian S O'Neill
 */
public class FilterValues<S extends Storable> implements Serializable, Appender {
    private static final long serialVersionUID = 1L;

    private static final Object[] NO_VALUES = new Object[0];

    static <S extends Storable> FilterValues<S>
        create(Filter<S> filter, PropertyFilterList<S> propFilterList)
    {
        return create(filter, propFilterList, null, null);
    }

    private static <S extends Storable> FilterValues<S>
        create(Filter<S> filter, PropertyFilterList<S> propFilterList,
               FilterValues<S> prevValues, Object prevValue)
    {
        FilterValues<S> fv = new FilterValues<S>(filter, propFilterList, prevValues, prevValue);
        PropertyFilter<S> propFilter;
        while (propFilterList != null
               && (propFilter = propFilterList.getPropertyFilter()).isConstant())
        {
            propFilterList = propFilterList.getNext();
            fv = new FilterValues<S>(filter, propFilterList, fv, propFilter.constant());
        }
        return fv;
    }

    private final transient Filter<S> mFilter;
    private final transient PropertyFilterList<S> mCurrentProperty;
    private final transient FilterValues<S> mPrevValues;
    private final transient Object mPrevValue;

    private transient volatile Map<PropertyFilter<S>, Object> mValueMap;

    private FilterValues(Filter<S> filter,
                         PropertyFilterList<S> propFilterList,
                         FilterValues<S> prevValues,
                         Object prevValue)
    {
        mFilter = filter;
        mCurrentProperty = propFilterList;
        mPrevValues = prevValues;
        mPrevValue = prevValue;
    }

    /**
     * Returns the Filter that this FilterValues instance applies to.
     */
    public Filter<S> getFilter() {
        return mFilter;
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(int value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(long value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(float value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(double value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(boolean value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(char value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(byte value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(short value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> with(Object value) {
        PropertyFilterList<S> current = currentProperty();
        Object obj;
        try {
            obj = current.getPropertyFilter().adaptValue(value);
        } catch (IllegalArgumentException e) {
            throw mismatch(e);
        }
        return with(current, obj);
    }

    /**
     * Returns a new FilterValues instance with the next blank parameters filled in.
     *
     * @param values parameter values to fill in; if null or empty, this
     * FilterValues instance is returned
     * @throws IllegalStateException if no blank parameters or if too many
     * parameter values supplied
     * @throws IllegalArgumentException if type doesn't match
     */
    public FilterValues<S> withValues(Object... values) {
        if (values == null) {
            return this;
        }
        if (values.length > getBlankParameterCount()) {
            throw new IllegalStateException("Too many values supplied");
        }
        FilterValues<S> filterValues = this;
        for (Object value : values) {
            filterValues = filterValues.with(value);
        }
        return filterValues;
    }

    private FilterValues<S> with(PropertyFilterList<S> current, Object value) {
        return create(mFilter, current.getNext(), this, value);
    }

    /**
     * Returns the amount of values yet to be assigned.
     *
     */
    public int getBlankParameterCount() {
        return mCurrentProperty == null ? 0 : (mCurrentProperty.getNextBlankRemaining() + 1);
    }

    /**
     * Returns the value assigned to the given PropertyFilter. If null, value
     * may be unassigned. Call getAssignedValue to have an exception thrown
     * instead.
     */
    public Object getValue(PropertyFilter<S> propFilter) {
        return getValue(propFilter, false);
    }

    /**
     * Returns the value assigned to the given PropertyFilter, throwing an
     * exception if not assigned. Call getValue to have null returned instead.
     *
     * @throws IllegalStateException if value is blank
     */
    public Object getAssignedValue(PropertyFilter<S> propFilter) throws IllegalStateException {
        return getValue(propFilter, true);
    }

    private Object getValue(PropertyFilter<S> propFilter, boolean mustBeAssigned) {
        if (propFilter.isConstant()) {
            return propFilter.constant();
        }

        Map<PropertyFilter<S>, Object> map = mValueMap;

        if (map == null) {
            FilterValues<S> prevValues = mPrevValues;
            if (prevValues == null) {
                if (mustBeAssigned) {
                    throw valueNotFound(propFilter);
                }
                return null;
            }

            if (prevValues.mCurrentProperty.getPreviousRemaining() < 3) {
                // Map would have few values in it, so don't bother building it.

                FilterValues<S> filterValues = this;
                do {
                    if (propFilter == prevValues.mCurrentProperty.getPropertyFilter()) {
                        return filterValues.mPrevValue;
                    }
                    filterValues = prevValues;
                    prevValues = prevValues.mPrevValues;
                } while (prevValues != null);

                if (mustBeAssigned) {
                    throw valueNotFound(propFilter);
                }
                return null;
            }

            map = buildValueMap();
        }

        Object value = map.get(propFilter);
        if (value == null && mustBeAssigned && !map.containsKey(propFilter)) {
            throw valueNotFound(propFilter);
        }

        return value;
    }

    /**
     * Returns true if a value is assigned to the given PropertyFilter.
     */
    public boolean isAssigned(PropertyFilter<S> propFilter) {
        if (propFilter.isConstant()) {
            return true;
        }

        Map<PropertyFilter<S>, Object> map = mValueMap;

        if (map == null) {
            FilterValues<S> prevValues = mPrevValues;
            if (prevValues == null) {
                return false;
            }

            if (prevValues.mCurrentProperty.getPreviousRemaining() < 3) {
                // Map would have few values in it, so don't bother building it.

                do {
                    if (propFilter == prevValues.mCurrentProperty.getPropertyFilter()) {
                        return true;
                    }
                    prevValues = prevValues.mPrevValues;
                } while (prevValues != null);

                return false;
            }

            map = buildValueMap();
        }

        return map.containsKey(propFilter);
    }

    private Map<PropertyFilter<S>, Object> buildValueMap() {
        Map<PropertyFilter<S>, Object> map =
            new IdentityHashMap<PropertyFilter<S>, Object>();

        FilterValues<S> filterValues = this;
        FilterValues<S> prevValues = mPrevValues;

        do {
            map.put(prevValues.mCurrentProperty.getPropertyFilter(), filterValues.mPrevValue);
            filterValues = prevValues;
            prevValues = prevValues.mPrevValues;
        } while (prevValues != null);

        mValueMap = map;
        return map;
    }

    /**
     * Returns all values in this object, including those provided by filter
     * constants. An IllegalStateException will result if any values are blank.
     *
     * @return new object array
     * @throws IllegalStateException if any values are blank
     */
    public Object[] getValues() throws IllegalStateException {
        return getValuesFor(mFilter);
    }

    /**
     * Returns all supplied values in this object. Constant filter values are
     * not included.
     *
     * @return new object array
     */
    public Object[] getSuppliedValues() {
        FilterValues<S> prevValues = mPrevValues;
        if (prevValues == null) {
            return NO_VALUES;
        }

        int i = prevValues.mCurrentProperty.getBlankCount();
        if (i == 0) {
            return NO_VALUES;
        }
        Object[] values = new Object[i];
        FilterValues filterValues = this;

        while (true) {
            if (!filterValues.mPrevValues.mCurrentProperty.getPropertyFilter().isConstant()) {
                values[--i] = filterValues.mPrevValue;
                if (i <= 0) {
                    break;
                }
            }
            filterValues = prevValues;
            prevValues = prevValues.mPrevValues;
        }

        return values;
    }

    /**
     * Returns all values in this object, as required by the given Filter. The
     * given Filter must be composed only of the same PropertyFilter instances
     * as used to construct this object. An IllegalStateException will result
     * otherwise.
     *
     * @param filter filter must be bound
     * @return new object array
     * @throws IllegalStateException if any values are blank
     */
    public Object[] getValuesFor(Filter<S> filter) throws IllegalStateException {
        // Traverse filter properties in reverse, since the filter likely was
        // used to create this FilterValues instance. If so, then no value map
        // needs to be constructed.
        PropertyFilterList<S> list = filter.getTailPropertyFilterList();
        if (list == null) {
            return NO_VALUES;
        }

        int i = list.getPreviousRemaining() + 1;
        Object[] values = new Object[i];

        FilterValues filterValues = this;
        FilterValues<S> prevValues = mPrevValues;

        for (; --i >= 0; list = list.getPrevious()) {
            PropertyFilter<S> propFilter = list.getPropertyFilter();

            Object value;

            if (prevValues != null
                && propFilter == prevValues.mCurrentProperty.getPropertyFilter()) {

                value = filterValues.mPrevValue;

                filterValues = prevValues;
                prevValues = prevValues.mPrevValues;
            } else {
                if (i > 0 || mValueMap != null) {
                    value = getAssignedValue(propFilter);
                } else {
                    // No need to force value map to be created since this is
                    // the last property to be processed. Do the same scan operation
                    // as performed by buildValueMap, except don't save the results.

                    filterValues = this;
                    prevValues = mPrevValues;

                    findValue: {
                        while (prevValues != null) {
                            if (propFilter == prevValues.mCurrentProperty.getPropertyFilter()) {
                                value = filterValues.mPrevValue;
                                break findValue;
                            }
                            filterValues = prevValues;
                            prevValues = prevValues.mPrevValues;
                        }

                        throw valueNotFound(propFilter);
                    }
                }
            }

            values[i] = value;
        }

        return values;
    }

    /**
     * Returns all supplied values in this object, as required by the given
     * Filter. Constant filter values are not included. The given Filter must
     * be composed only of the same PropertyFilter instances as used to
     * construct this object. An IllegalStateException will result otherwise.
     *
     * @param filter filter must be bound
     * @return new object array
     */
    public Object[] getSuppliedValuesFor(Filter<S> filter) throws IllegalStateException {
        // Traverse filter properties in reverse, since the filter likely was
        // used to create this FilterValues instance. If so, then no value map
        // needs to be constructed.
        PropertyFilterList<S> list = filter.getTailPropertyFilterList();
        if (list == null) {
            return NO_VALUES;
        }

        FilterValues<S> prevValues = mPrevValues;
        if (prevValues == null) {
            return NO_VALUES;
        }

        int i = list.getPreviousRemaining() + 1;

        // Array is sized assuming that no constants are encountered and all
        // filters are assigned values. Array is trimmed later if necessary.
        int valuesPos = i;
        Object[] values = new Object[valuesPos];

        FilterValues filterValues = this;

        for (; --i >= 0; list = list.getPrevious()) {
            PropertyFilter<S> propFilter = list.getPropertyFilter();

            Object value;

            if (prevValues != null
                && propFilter == prevValues.mCurrentProperty.getPropertyFilter()) {

                value = filterValues.mPrevValue;

                filterValues = prevValues;
                prevValues = prevValues.mPrevValues;

                if (propFilter.isConstant()) {
                    continue;
                }
            } else {
                if (propFilter.isConstant()) {
                    continue;
                }

                if (i > 0 || mValueMap != null) {
                    if (isAssigned(propFilter)) {
                        value = getAssignedValue(propFilter);
                    } else {
                        continue;
                    }
                } else {
                    // No need to force value map to be created since this is
                    // the last property to be processed. Do the same scan operation
                    // as performed by buildValueMap, except don't save the results.

                    filterValues = this;
                    prevValues = mPrevValues;

                    findValue: {
                        while (prevValues != null) {
                            if (propFilter == prevValues.mCurrentProperty.getPropertyFilter()) {
                                value = filterValues.mPrevValue;
                                break findValue;
                            }
                            filterValues = prevValues;
                            prevValues = prevValues.mPrevValues;
                        }

                        continue;
                    }
                }
            }

            values[--valuesPos] = value;
        }

        if (valuesPos != 0) {
            // Trim array.
            int newValuesSize = values.length - valuesPos;
            if (newValuesSize == 0) {
                values = NO_VALUES;
            } else {
                Object[] newValues = new Object[newValuesSize];
                System.arraycopy(values, valuesPos, newValues, 0, newValuesSize);
                values = newValues;
            }
        }

        return values;
    }

    private IllegalStateException valueNotFound(PropertyFilter<S> propFilter) {
        return new IllegalStateException
            ("Property value not found for: \"" + propFilter + "\" in filter \"" + this + '"');
    }

    @Override
    public int hashCode() {
        int hash = mFilter.hashCode();
        if (mPrevValue != null) {
            hash += mPrevValue.hashCode();
        }
        if (mPrevValues != null) {
            hash += mPrevValues.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof FilterValues) {
            FilterValues<?> other = (FilterValues<?>) obj;
            return mFilter.equals(other.mFilter)
                && mCurrentProperty == other.mCurrentProperty
                && (mPrevValues == null ? other.mPrevValues == null
                    : mPrevValues.equals(other.mPrevValues))
                && (mPrevValue == null ? other.mPrevValue == null
                    : mPrevValue.equals(other.mPrevValue));
        }
        return false;
    }

    /**
     * Returns the string value of the filter with any values substituted.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        try {
            appendTo(buf);
        } catch (java.io.IOException e) {
            // Not gonna happen.
        }
        return buf.toString();
    }

    public void appendTo(Appendable app) throws IOException {
        mFilter.appendTo(app, this);
    }

    private PropertyFilterList<S> currentProperty() {
        PropertyFilterList<S> current = mCurrentProperty;
        if (current == null) {
            throw new IllegalStateException("No blank parameters");
        }
        return current;
    }

    private IllegalArgumentException mismatch(IllegalArgumentException e) {
        PropertyFilterList<S> current = currentProperty();
        PropertyFilter<S> propFilter = current.getPropertyFilter();

        StringBuilder b = new StringBuilder();
        b.append(e.getMessage());

        int subFilterCount = current.getPreviousRemaining() + current.getNextRemaining() + 1;

        if (subFilterCount <= 1) {
            b.append(" for filter \"");
            b.append(propFilter);
            b.append('"');
        } else {
            b.append(" for ");
            appendOrdinal(b, current.getPreviousRemaining() + 1);
            b.append(" sub filter in \"");
            try {
                appendTo(b);
            } catch (IOException e2) {
                // Not gonna happen
            }
            b.append('"');
        }

        return new IllegalArgumentException(b.toString());
    }

    private void appendOrdinal(StringBuilder b, int value) {
        b.append(value);

        if (value != 11 && value != 12 && value != 13) {
            value = value % 10;
            switch (value) {
            default:
                break;
            case 1:
                b.append("st");
                return;
            case 2:
                b.append("nd");
                return;
            case 3:
                b.append("rd");
                return;
            }
        }

        b.append("th");
    }

    private Object writeReplace() {
        return new FaV(mFilter, getSuppliedValues());
    }

    // Filter and Values
    private static class FaV implements Externalizable {
        private static final long serialVersionUID = 1L;

        private Filter<?> mFilter;
        private Object[] mValues;

        // Required for Externalizable.
        public FaV() {
        }

        FaV(Filter<?> filter, Object[] values) {
            mFilter = filter;
            mValues = values;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(mFilter);
            out.writeObject(mValues);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            mFilter = (Filter<?>) in.readObject();
            mValues = (Object[]) in.readObject();
        }

        private Object readResolve() {
            return mFilter.initialFilterValues().withValues(mValues);
        }
    }
}
