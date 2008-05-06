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
package com.amazon.carbonado.synthetic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.amazon.carbonado.info.StorablePropertyAdapter;

/**
 * Minimal specification of a storable property for use with a {@link SyntheticStorableBuilder}.
 * Synthetic storables can be used to generate user storables.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public class SyntheticProperty implements Comparable<SyntheticProperty> {
    private final Class mType;
    private final String mName;
    private String mReadMethodName;
    private String mWriteMethodName;
    private boolean mIsNullable = false;
    private boolean mIsVersion = false;
    private StorablePropertyAdapter mAdapter;
    private List<String> mAnnotationDescs;

    /**
     * Generate a name for a bean "get" method ("is" method, for booleans).
     * @param name of the property
     * @param type return type of the property
     * @see #getReadMethodName()
     */
    public static String makeReadMethodName(String name, Class type) {
        return ((type == boolean.class) ? "is" : "get") + makeUppercase(name);
    }

    /**
     * Generate a name for a bean "set" method
     * @param name of the property
     * @see #getWriteMethodName()
     */
    public static String makeWriteMethodName(String name) {
        return "set" + makeUppercase(name);

    }

    /**
     * Every property requires minimally a name and a type
     * @param name for the property
     * @param type of the data it contains
     */
    public SyntheticProperty(String name, Class type) {
        if (name == null || type == null) {
            throw new IllegalArgumentException();
        }
        mName = name;
        mType = type;
    }

    /**
     *
     * @param name property name
     * @param type property type
     * @param isNullable true if this property can be null (default false)
     * @param isVersion true if this property is a version number (default false)
     */
    public SyntheticProperty(String name,
                             Class type,
                             boolean isNullable,
                             boolean isVersion) {
        mIsNullable = isNullable;
        mIsVersion = isVersion;
        mName = name;
        mType = type;
    }

    /**
     * @return Name of the property
     */
    public String getName() {
        return mName;
    }

    /**
     * @return type of the property
     */
    public Class getType() {
        return mType;
    }

    /**
     * @return true if the property can be null
     */
    public boolean isNullable() {
        return mIsNullable;
    }

    /**
     * @param isNullable true if the property can be null
     */
    public void setIsNullable(boolean isNullable) {
        mIsNullable = isNullable;
    }

    /**
     * @return true if the property contains the versioning information for the storable.  Note that
     * at most one property can be the version property for a given storable
     */
    public boolean isVersion() {
        return mIsVersion;
    }

    /**
     * @param isVersion true if the property should contain the versioning information for the
     * storable
     */
    public void setIsVersion(boolean isVersion) {
        mIsVersion = isVersion;
    }

    /**
     * Returns the name of the read method.
     */
    public String getReadMethodName() {
        if (mReadMethodName == null) {
            mReadMethodName = makeReadMethodName(mName, mType);
        }
        return mReadMethodName;
    }

    /**
     * Call to override default selection of read method name.
     */
    void setReadMethodName(String name) {
        mReadMethodName = name;
    }

    /**
     * Returns the name of the write method.
     */
    public String getWriteMethodName() {
        if (mWriteMethodName == null) {
            mWriteMethodName = makeWriteMethodName(mName);
        }
        return mWriteMethodName;
    }

    /**
     * Call to override default selection of write method name.
     */
    void setWriteMethodName(String name) {
        mWriteMethodName = name;
    }

    /**
     * @return the optional adapter.
     */
    public StorablePropertyAdapter getAdapter() {
        return mAdapter;
    }

    /**
     * Storables cannot currently have more than one adapter per property.
     *
     * @param adapter The adapter to set.
     */
    public void setAdapter(StorablePropertyAdapter adapter) {
        mAdapter = adapter;
    }

    /**
     * Add an arbitrary annotation to the property accessor method, as
     * specified by a descriptor.
     *
     * @see com.amazon.carbonado.util.AnnotationDescPrinter
     */
    public void addAccessorAnnotationDescriptor(String annotationDesc) {
        if (mAnnotationDescs == null) {
            mAnnotationDescs = new ArrayList<String>(4);
        }
        mAnnotationDescs.add(annotationDesc);
    }

    /**
     * Returns all the added accessor annotation descriptors in an unmodifiable list.
     */
    public List<String> getAccessorAnnotationDescriptors() {
        if (mAnnotationDescs == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(mAnnotationDescs);
    }

    /**
     * {@link Comparable} implementation.
     * @param otherProp
     */
    public int compareTo(SyntheticProperty otherProp) {
        if (this == otherProp) {
            return 0;
        }
        return mName.compareTo(otherProp.mName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SyntheticProperty)) {
            return false;
        }

        final SyntheticProperty syntheticProperty = (SyntheticProperty) o;

        if (mIsNullable != syntheticProperty.mIsNullable) {
            return false;
        }
        if (mIsVersion != syntheticProperty.mIsVersion) {
            return false;
        }
        if (mName != null ? !mName.equals(syntheticProperty.mName) : syntheticProperty.mName != null) {
            return false;
        }
        if (mType != null ? !mType.equals(syntheticProperty.mType) : syntheticProperty.mType != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        result = (mType != null ? mType.hashCode() : 0);
        result = 29 * result + (mName != null ? mName.hashCode() : 0);
        result = 29 * result + (mIsNullable ? 1 : 0);
        result = 29 * result + (mIsVersion ? 1 : 0);
        return result;
    }

    private static String makeUppercase(String name) {
        if (name.length() > 0 && !Character.isUpperCase(name.charAt(0))) {
            name = Character.toUpperCase(name.charAt(0)) + name.substring(1);
        }
        return name;
    }

    @Override
    public String toString() {
        return mName+'|'+
               (mIsNullable?"NULL|":"")+
               (mIsVersion?"VERS|":"")+
               '('+mType+')'
               ;
    }
}
