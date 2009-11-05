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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.commons.logging.LogFactory;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.synthetic.SyntheticKey;
import com.amazon.carbonado.synthetic.SyntheticProperty;
import com.amazon.carbonado.synthetic.SyntheticStorableBuilder;
import com.amazon.carbonado.util.AnnotationDescPrinter;

import com.amazon.carbonado.capability.ResyncCapability;

/**
 * Describes the layout of a specific generation of a storable.
 *
 * @author Brian S O'Neill
 * @see LayoutFactory
 */
public class Layout {
    private static Map<Long, Class<? extends Storable>> cReconstructed;

    static {
        cReconstructed = Collections.synchronizedMap(new SoftValuedHashMap());
    }

    static Class<? extends Storable> reconstruct(final Layout layout, ClassLoader loader)
        throws FetchException, SupportException
    {
        synchronized (cReconstructed) {
            Long key = layout.getLayoutID();
            Class<? extends Storable> clazz = cReconstructed.get(key);
            if (clazz != null) {
                return clazz;
            }

            SyntheticStorableBuilder builder =
                new SyntheticStorableBuilder(layout.getStorableTypeName(), loader);

            // Make sure reconstructed class encodes the same as before.
            builder.setEvolvable(true);

            builder.setClassNameProvider(new SyntheticStorableBuilder.ClassNameProvider() {
                public String getName() {
                    return layout.getStorableTypeName();
                }

                // The name of the auto-generated class should not be made with
                // createExplicit. Otherwise, property type changes will
                // conflict, since the reconstructed class name is the same.
                public boolean isExplicit() {
                    return false;
                }
            });

            SyntheticKey primaryKey = builder.addPrimaryKey();

            for (LayoutProperty property : layout.getAllProperties()) {
                Class propClass = property.getPropertyType(loader);
                if (Query.class.isAssignableFrom(propClass) ||
                    Storable.class.isAssignableFrom(propClass)) {
                    // Accidentally stored join property in layout, caused by a
                    // bug in a previous version of Layout. Move on.
                    continue;
                }

                SyntheticProperty synthProp =
                    builder.addProperty(property.getPropertyName(), propClass);

                synthProp.setIsNullable(property.isNullable());
                synthProp.setIsVersion(property.isVersion());

                if (property.isPrimaryKeyMember()) {
                    primaryKey.addProperty(property.getPropertyName());
                }

                if (property.getAdapterTypeName() != null) {
                    String desc = property.getAdapterParams();
                    if (desc == null) {
                        desc = AnnotationDescPrinter
                            .makePlainDescriptor(property.getAdapterTypeName());
                    }
                    synthProp.addAccessorAnnotationDescriptor(desc);
                }
            }

            clazz = builder.build();

            cReconstructed.put(key, clazz);
            return clazz;
        }
    }

    private final LayoutFactory mLayoutFactory;
    private final StoredLayout mStoredLayout;
    private final LayoutOptions mOptions;

    private transient List<LayoutProperty> mAllProperties;

    /**
     * Creates a Layout around an existing storable.
     */
    Layout(LayoutFactory factory, StoredLayout storedLayout) throws CorruptEncodingException {
        mLayoutFactory = factory;
        mStoredLayout = storedLayout;

        byte[] extra = storedLayout.getExtraData();
        if (extra == null) {
            mOptions = null;
        } else {
            mOptions = new LayoutOptions();
            try {
                mOptions.decode(extra);
            } catch (IOException e) {
                throw new CorruptEncodingException(e);
            }
            mOptions.readOnly();
        }
    }

    /**
     * Copies layout information into freshly prepared storables. Call insert
     * (on this class) to persist them.
     */
    Layout(LayoutFactory factory, StorableInfo<?> info, LayoutOptions options, long layoutID) {
        mLayoutFactory = factory;

        StoredLayout storedLayout = factory.mLayoutStorage.prepare();
        mStoredLayout = storedLayout;

        storedLayout.setLayoutID(layoutID);
        storedLayout.setStorableTypeName(info.getStorableType().getName());
        storedLayout.setCreationTimestamp(System.currentTimeMillis());
        try {
            storedLayout.setCreationUser(System.getProperty("user.name"));
        } catch (SecurityException e) {
            // Can't get user, no big deal.
        }
        try {
            storedLayout.setCreationHost(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            // Can't get host, no big deal.
        } catch (SecurityException e) {
            // Can't get host, no big deal.
        }

        if (options == null) {
            mOptions = null;
        } else {
            options.readOnly();
            storedLayout.setExtraData(options.encode());
            mOptions = options;
        }

        Collection<? extends StorableProperty<?>> properties = info.getAllProperties().values();
        List<LayoutProperty> list = new ArrayList<LayoutProperty>(properties.size());
        int ordinal = 0;
        for (StorableProperty<?> property : properties) {
            if (property.isDerived() || property.isJoin()) {
                continue;
            }
            StoredLayoutProperty storedLayoutProperty = mLayoutFactory.mPropertyStorage.prepare();
            list.add(new LayoutProperty(storedLayoutProperty, property, layoutID, ordinal));
            ordinal++;
        }

        mAllProperties = Collections.unmodifiableList(list);
    }

    /**
     * Returns a unique identifier for this layout.
     */
    public long getLayoutID() {
        return mStoredLayout.getLayoutID();
    }

    /**
     * Storable type name is a fully qualified Java class name.
     */
    public String getStorableTypeName() {
        return mStoredLayout.getStorableTypeName();
    }

    /**
     * Returns the generation of this layout, where zero represents the first
     * generation.
     */
    public int getGeneration() {
        return mStoredLayout.getGeneration();
    }

    /**
     * Returns all the non-primary key properties of this layout, in their
     * proper order.
     */
    public List<LayoutProperty> getDataProperties() throws FetchException {
        List<LayoutProperty> all = getAllProperties();
        List<LayoutProperty> data = new ArrayList<LayoutProperty>(all.size() - 1);
        for (LayoutProperty property : all) {
            if (!property.isPrimaryKeyMember()) {
                data.add(property);
            }
        }
        return Collections.unmodifiableList(data);
    }

    /**
     * Returns all the properties of this layout, in their proper order.
     */
    public List<LayoutProperty> getAllProperties() throws FetchException {
        if (mAllProperties == null) {
            Cursor <StoredLayoutProperty> cursor = mLayoutFactory.mPropertyStorage
                .query("layoutID = ?")
                .with(mStoredLayout.getLayoutID())
                .orderBy("ordinal")
                .fetch();

            try {
                List<LayoutProperty> list = new ArrayList<LayoutProperty>();

                while (cursor.hasNext()) {
                    list.add(new LayoutProperty(cursor.next()));
                }

                mAllProperties = Collections.unmodifiableList(list);
            } finally {
                cursor.close();
            }
        }

        return mAllProperties;
    }

    /**
     * Returns the date and time for when this layout generation was created.
     */
    public DateTime getCreationDateTime() {
        return new DateTime(mStoredLayout.getCreationTimestamp());
    }

    /**
     * Returns the user that created this layout generation.
     */
    public String getCreationUser() {
        return mStoredLayout.getCreationUser();
    }

    /**
     * Returns the host machine that created this generation.
     */
    public String getCreationHost() {
        return mStoredLayout.getCreationHost();
    }

    /**
     * Returns additional options, or null if none.
     *
     * @return read-only object or null
     */
    public LayoutOptions getOptions() {
        return mOptions;
    }

    /**
     * Returns the layout for a particular generation of this layout's type.
     *
     * @throws FetchNoneException if generation not found
     */
    public Layout getGeneration(int generation) throws FetchNoneException, FetchException {
        final String filter = "storableTypeName = ? & generation = ?";

        StoredLayout storedLayout;
        try {
            storedLayout = mLayoutFactory.mLayoutStorage
                .query(filter)
                .with(getStorableTypeName()).with(generation)
                .loadOne();
        } catch (FetchNoneException e) {
            // Try to resync with a master.
            ResyncCapability cap =
                mLayoutFactory.mRepository.getCapability(ResyncCapability.class);

            if (cap == null) {
                throw e;
            }

            try {
                cap.resync(mLayoutFactory.mLayoutStorage.getStorableType(), 1.0,
                           filter, getStorableTypeName(), generation);
            } catch (RepositoryException e2) {
                // Uh oh, double trouble. Log this one and throw original exception.
                LogFactory.getLog(Layout.class).error("Unable to resync layout", e2);
                throw e;
            }

            storedLayout = mLayoutFactory.mLayoutStorage
                .query(filter)
                .with(getStorableTypeName()).with(generation)
                .loadOne();

            // Make sure all the properties are re-sync'd too.
            try {
                cap.resync(mLayoutFactory.mPropertyStorage.getStorableType(), 1.0,
                           "layoutID = ?", storedLayout.getLayoutID());
            } catch (RepositoryException e2) {
                // Uh oh, double trouble. Log this one and throw original exception.
                LogFactory.getLog(Layout.class).error("Unable to resync layout", e2);
                throw e;
            }
        }

        return new Layout(mLayoutFactory, storedLayout);
    }

    /**
     * Returns the previous known generation of the storable's layout, or null
     * if none.
     *
     * @return a layout with a lower generation, or null if none
     */
    public Layout previousGeneration() throws FetchException {
        Cursor<StoredLayout> cursor = mLayoutFactory.mLayoutStorage
            .query("storableTypeName = ? & generation < ?")
            .with(getStorableTypeName()).with(getGeneration())
            .orderBy("-generation")
            .fetch();

        try {
            if (cursor.hasNext()) {
                return new Layout(mLayoutFactory, cursor.next());
            }
        } finally {
            cursor.close();
        }

        return null;
    }

    /**
     * Returns the next known generation of the storable's layout, or null
     * if none.
     *
     * @return a layout with a higher generation, or null if none
     */
    public Layout nextGeneration() throws FetchException {
        Cursor<StoredLayout> cursor =
            mLayoutFactory.mLayoutStorage.query("storableTypeName = ? & generation > ?")
            .with(getStorableTypeName()).with(getGeneration())
            .orderBy("+generation")
            .fetch();

        try {
            if (cursor.hasNext()) {
                return new Layout(mLayoutFactory, cursor.next());
            }
        } finally {
            cursor.close();
        }

        return null;
    }

    /**
     * Reconstructs the storable type defined by this layout by returning an
     * auto-generated class. The reconstructed storable type will not contain
     * everything in the original, but rather the minimum required to decode
     * persisted instances.
     */
    public Class<? extends Storable> reconstruct() throws FetchException, SupportException {
        return reconstruct(null);
    }

    /**
     * Reconstructs the storable type defined by this layout by returning an
     * auto-generated class. The reconstructed storable type will not contain
     * everything in the original, but rather the minimum required to decode
     * persisted instances.
     *
     * @param loader optional ClassLoader to load reconstruct class into, if it
     * has not been loaded yet
     */
    public Class<? extends Storable> reconstruct(ClassLoader loader)
        throws FetchException, SupportException
    {
        Class<? extends Storable> reconstructed = reconstruct(this, loader);
        mLayoutFactory.registerReconstructed(reconstructed, this);
        return reconstructed;
    }

    @Override
    public int hashCode() {
        long id = getLayoutID();
        return ((int) id) ^ (int) (id >> 32);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Layout) {
            Layout other = (Layout) obj;
            try {
                return mStoredLayout.equals(other.mStoredLayout) && equalLayouts(other);
            } catch (FetchException e) {
                return false;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Layout {type=").append(getStorableTypeName());
        b.append(", generation=").append(getGeneration());
        b.append(", properties={");
        try {
            List<LayoutProperty> props = getAllProperties();
            for (int i=0; i<props.size(); i++) {
                if (i > 0) {
                    b.append(", ");
                }
                b.append(props.get(i));
            }
        } catch (FetchException e) {
            b.append(e.toString());
        }
        b.append("}}");
        return b.toString();
    }

    /**
     * Returns true if the given layout matches this one. Layout ID,
     * generation, and creation info is not considered in the comparison.
     */
    public boolean equalLayouts(Layout layout) throws FetchException {
        if (this == layout) {
            return true;
        }
        return getStorableTypeName().equals(layout.getStorableTypeName())
            && getAllProperties().equals(layout.getAllProperties());
    }

    // Assumes caller is in a transaction.
    void insert(int generation) throws PersistException {
        if (mAllProperties == null) {
            throw new IllegalStateException();
        }
        mStoredLayout.setGeneration(generation);
        if (!mStoredLayout.tryInsert()) {
            StoredLayout existing = mLayoutFactory.mLayoutStorage.prepare();
            mStoredLayout.copyPrimaryKeyProperties(existing);
            try {
                existing.load();
            } catch (FetchException e) {
                throw e.toPersistException();
            }
            mStoredLayout.setVersionNumber(existing.getVersionNumber());
            mStoredLayout.update();
        }
        for (LayoutProperty property : mAllProperties) {
            property.store();
        }
    }
}
