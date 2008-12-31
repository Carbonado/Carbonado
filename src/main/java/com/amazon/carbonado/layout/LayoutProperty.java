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

package com.amazon.carbonado.layout;

import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;
import com.amazon.carbonado.info.StorablePropertyAnnotation;

import com.amazon.carbonado.util.AnnotationDescPrinter;

/**
 * Describes how a property is defined in a specific generation of a storable.
 *
 * @author Brian S O'Neill
 * @see Layout
 */
public class LayoutProperty {
    private final StoredLayoutProperty mStoredLayoutProperty;

    /**
     * Creates a LayoutProperty around an existing storable.
     */
    LayoutProperty(StoredLayoutProperty storedLayoutProperty) {
        mStoredLayoutProperty = storedLayoutProperty;
    }

    /**
     * Copies properties into a freshly prepared storable. Call insert (on this
     * class) to persist it.
     *
     * @param storedLayoutProperty freshly prepared storable
     * @param property source of data to copy into storable
     */
    LayoutProperty(StoredLayoutProperty storedLayoutProperty,
                   StorableProperty<?> property,
                   long layoutID,
                   int ordinal)
    {
        mStoredLayoutProperty = storedLayoutProperty;

        storedLayoutProperty.setLayoutID(layoutID);
        storedLayoutProperty.setOrdinal(ordinal);
        storedLayoutProperty.setPropertyName(property.getName());
        storedLayoutProperty.setPropertyTypeDescriptor
            (TypeDesc.forClass(property.getType()).getDescriptor());
        storedLayoutProperty.setNullable(property.isNullable());
        storedLayoutProperty.setVersion(property.isVersion());
        storedLayoutProperty.setPrimaryKeyMember(property.isPrimaryKeyMember());

        if (property.getAdapter() != null) {
            StorablePropertyAdapter adapter = property.getAdapter();
            StorablePropertyAnnotation spa = adapter.getAnnotation();
            if (spa == null || spa.getAnnotation() == null) {
                storedLayoutProperty.setAdapterTypeName(null);
                storedLayoutProperty.setAdapterParams(null);
            } else {
                storedLayoutProperty.setAdapterTypeName(spa.getAnnotationType().getName());

                StringBuilder b = new StringBuilder();
                AnnotationDescPrinter printer = new AnnotationDescPrinter(true, b);
                printer.visit(spa.getAnnotation());

                storedLayoutProperty.setAdapterParams(b.toString());
            }
        }
    }

    public String getPropertyName() {
        return mStoredLayoutProperty.getPropertyName();
    }

    /**
     * Property type descriptor is a Java type descriptor.
     */
    public String getPropertyTypeDescriptor() {
        return mStoredLayoutProperty.getPropertyTypeDescriptor();
    }

    public Class getPropertyType() throws SupportException {
        return getPropertyType(null);
    }

    public Class getPropertyType(ClassLoader loader) throws SupportException {
        TypeDesc type = TypeDesc.forDescriptor(getPropertyTypeDescriptor());
        Class propClass = type.toClass(loader);
        if (propClass == null) {
            throw new SupportException
                ("Unable to find class \"" + type.getRootName() + "\" for property \"" +
                 getPropertyName() + '"');
        }
        return propClass;
    }

    /**
     * Returns true of property can be set to null.
     */
    public boolean isNullable() {
        return mStoredLayoutProperty.isNullable();
    }

    /**
     * Returns true if property is a member of the primary key.
     */
    public boolean isPrimaryKeyMember() {
        return mStoredLayoutProperty.isPrimaryKeyMember();
    }

    /**
     * Returns true if this property is the designated version number for the
     * Storable.
     */
    public boolean isVersion() {
        return mStoredLayoutProperty.isVersion();
    }

    /**
     * Adapter type name is a fully qualified Java class name. If property has
     * no adapter, then null is returned.
     */
    public String getAdapterTypeName() {
        return mStoredLayoutProperty.getAdapterTypeName();
    }

    /**
     * Parameters for adapter, or null if property has no explicit adapter.
     */
    public String getAdapterParams() {
        return mStoredLayoutProperty.getAdapterParams();
    }

    @Override
    public int hashCode() {
        return mStoredLayoutProperty.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof LayoutProperty) {
            StoredLayoutProperty thisStoredLayoutProperty = mStoredLayoutProperty;
            StoredLayoutProperty other = ((LayoutProperty) obj).mStoredLayoutProperty;

            boolean result = thisStoredLayoutProperty.equalProperties(other);
            if (result) {
                return result;
            }

            // Version might be only difference, which is fine.
            if (thisStoredLayoutProperty.getVersionNumber() != other.getVersionNumber()) {
                thisStoredLayoutProperty = thisStoredLayoutProperty.copy();
                thisStoredLayoutProperty.setVersionNumber(other.getVersionNumber());
                return thisStoredLayoutProperty.equalProperties(other);
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return mStoredLayoutProperty.toString();
    }

    void store() throws PersistException {
        if (!mStoredLayoutProperty.tryInsert()) {
            StoredLayoutProperty existing = mStoredLayoutProperty.copy();
            try {
                existing.load();
                existing.copyVersionProperty(mStoredLayoutProperty);
            } catch (FetchException e) {
                throw e.toPersistException();
            }
            mStoredLayoutProperty.update();
        }
    }
}
