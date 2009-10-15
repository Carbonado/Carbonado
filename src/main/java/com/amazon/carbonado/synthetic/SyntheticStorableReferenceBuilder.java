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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.gen.CodeBuilderUtil;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.BeanComparator;

/**
 * A SyntheticStorableReference defines new kinds of Storables from an existing
 * master storable. This is used in situations when additional information about
 * a storable needs to be tracked -- eg, for an index, or for caching. The
 * storable may optionally have completely new, synthetic properties added.
 *
 * <P>
 * All primary key properties of the master storable will also be provided by the
 * derived storable. Three special methods will be provided:
 * <ul>
 * <li>getMaster - retrieves the original storable</li>
 * <li>setAllProperties - sets the properties the syntheticReference has in
 * common with the master to the values of the master instance</li>
 * <li>isConsistent - verifies that the properties the syntheticReference has
 * in common with the master are consistent with an instance of the master,
 * meaning that they are in the same state and, if set, equal.</li>
 * </ul>
 *
 * @author Brian S O'Neill
 * @author Don Schneider
 * @author David Rosenstrauch
 */
public class SyntheticStorableReferenceBuilder<S extends Storable>
        implements SyntheticBuilder {

    // The property setter will be called something like "copyFromMaster_0"
    private static final String COPY_FROM_MASTER_PREFIX = "copyFromMaster_";

    private static final String COPY_TO_MASTER_PK_PREFIX = "copyToMasterPk_";

    private static final String IS_CONSISTENT_PREFIX = "getIsConsistent_";

    // Information about the storable from which this one is derived
    // private StorableInfo<S> mBaseStorableInfo;
    private Class<S> mMasterStorableClass;

    // Stashed copy of the results of calling StorableIntrospector.examine(...)
    // on the master storable class.
    private StorableInfo<S> mMasterStorableInfo;

    // This guy will actually do the work
    private SyntheticStorableBuilder mBuilder;

    // Primary key of generated storable.
    SyntheticKey mPrimaryKey;

    // Elements added to primary key to ensure uniqueness
    private Set<String> mExtraPkProps;

    // True if the specified properties for this derived reference class are sufficient to
    // uniquely identify a unique instance of the referent.
    private boolean mIsUnique = true;

    // The list of properties explicitly added to this reference builder
    private List<SyntheticProperty> mUserProps;

    // This list of properties the master and this one have in common
    // The StorableProperties that get added to this list
    // are retrieved from the master.
    private List<StorableProperty> mCommonProps;

    String mCopyFromMasterMethodName;
    String mIsConsistentMethodName;
    String mCopyToMasterPkMethodName;

    // The result of building.
    private SyntheticStorableReferenceAccess mReferenceAccess;

    /**
     * @param storableClass
     *            class of the storable that will be referenced by this
     *            synthetic. The name for the synthetic storable will be based
     *            on this class's name, decorated with the properties which
     *            participate in the primary key for the synthetic storable.
     */
    public SyntheticStorableReferenceBuilder(Class<S> storableClass,
                                             boolean isUnique) {
        this(storableClass, null, isUnique);
    }

    /**
     * @param storableClass
     *            class of the storable that will be referenced by this
     *            synthetic
     * @param baseName
     *            of the generated synthetic. Note that for some repositories
     *            this name will be visible across the entire repository, so it
     *            is good practice to include namespace information to guarantee
     *            uniqueness.
     * @param isUnique
     *             true if the properties that are explicitly identified as primary
     *             key properites are sufficient to uniquely identify the index object.
     */
    public SyntheticStorableReferenceBuilder(Class<S> storableClass,
                                             final String baseName,
                                             boolean isUnique) {
        mMasterStorableClass = storableClass;
        // Stash this away for later reference
        mMasterStorableInfo = StorableIntrospector.examine(storableClass);

        mIsUnique = isUnique;
        mBuilder = new SyntheticStorableBuilder(storableClass.getCanonicalName()
                                                + (baseName != null? "_" + baseName : ""),
                                                storableClass.getClassLoader());
        mBuilder.setClassNameProvider(new ReferenceClassNameProvider(isUnique));

        mPrimaryKey = mBuilder.addPrimaryKey();
        mExtraPkProps = new LinkedHashSet<String>();

        mUserProps = new ArrayList<SyntheticProperty>();
        mCommonProps = new ArrayList<StorableProperty>();

    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#prepare()
     */
    public ClassFileBuilder prepare() throws SupportException {

        // Add a property to the reference for each primary key in the master
        List<StorableProperty> masterPkProps = new ArrayList<StorableProperty>
            (mMasterStorableInfo.getPrimaryKeyProperties().values());

        // Sort master primary keys, to ensure consistent behavior.
        Collections.sort(masterPkProps,
                         BeanComparator.forClass(StorableProperty.class).orderBy("name"));

        for (StorableProperty masterPkProp : masterPkProps) {
            // Some of them may have already been added as explicit
            // primary keys, to articulate sort direction.
            if (!mBuilder.hasProperty(masterPkProp.getName())) {
                addProperty(masterPkProp);
                // For non-unique indexes, *all* master pk properties are members of the
                // generated primary key, in order to support duplicates.
                if (!mIsUnique) {
                    mPrimaryKey.addProperty(masterPkProp.getName());
                    mExtraPkProps.add(masterPkProp.getName());
                }
            }
        }

        // Ensure that a version property exists in the index entry, if the
        // user storable had one.
        if (!mBuilder.isVersioned()) {
            StorableProperty versionProperty = mMasterStorableInfo.getVersionProperty();
            if (versionProperty != null) {
                addProperty(versionProperty);
            }
        }

        ClassFileBuilder cfg = mBuilder.prepare();
        addSpecialMethods(cfg.getClassFile());
        return cfg;
    }

    /**
     * Build and return access to the generated storable reference class.
     *
     * @since 1.2.1
     */
    public SyntheticStorableReferenceAccess getReferenceAccess() {
        if (mReferenceAccess == null) {
            Class<? extends Storable> referenceClass = mBuilder.getStorableClass();
            mReferenceAccess = new SyntheticStorableReferenceAccess
                (mMasterStorableClass, referenceClass, this);
        }
        return mReferenceAccess;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#getStorableClass()
     */
    public Class<? extends Storable> getStorableClass() throws IllegalStateException {
        return getReferenceAccess().getReferenceClass();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#build()
     */
    public Class<? extends Storable> build() throws SupportException {
        prepare();
        return getStorableClass();
    }

    /**
     * Add a property to the primary key which is a member of the Storable type
     * being referenced by this one.
     *
     * @param name
     */
    public SyntheticProperty addKeyProperty(String name, Direction direction) {
        StorableProperty<S> prop = mMasterStorableInfo.getAllProperties().get(name);
        if (prop == null) {
            throw new IllegalArgumentException(name + " is not a property of "
                    + mMasterStorableInfo.getName());
        }

        mPrimaryKey.addProperty(name, direction);

        SyntheticProperty result = addProperty(prop);
        mUserProps.add(result);
        return result;
    }

    /**
     * @see com.amazon.carbonado.synthetic.SyntheticStorableBuilder#addProperty(java.lang.String,
     *      java.lang.Class)
     */
    public SyntheticProperty addProperty(String name, Class type) {
        SyntheticProperty result = mBuilder.addProperty(name, type);
        mUserProps.add(result);
        return result;
    }

    /**
     * @see com.amazon.carbonado.synthetic.SyntheticStorableBuilder#addProperty(com.amazon.carbonado.synthetic.SyntheticProperty)
     */
    public SyntheticProperty addProperty(SyntheticProperty prop) {
        SyntheticProperty result = mBuilder.addProperty(prop);
        mUserProps.add(result);
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#hasProperty(java.lang.String)
     */
    public boolean hasProperty(String name) {
        return mBuilder.hasProperty(name);
    }

    /**
     * @return Returns the indexProps.
     */
    public List<SyntheticProperty> getUserProps() {
        return mUserProps;
    }

    public SyntheticKey addPrimaryKey() {
        return mBuilder.addPrimaryKey();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#addAlternateKey() Note that
     *      using this method for a SyntheticReference being used as an alternate key is
     *      not well defined.
     */
    public SyntheticKey addAlternateKey() {
        return mBuilder.addAlternateKey();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#addIndex() Note that
     *      using this method for a SyntheticReference being used as an index is
     *      not well defined.
     */
    public SyntheticIndex addIndex() {
        return mBuilder.addIndex();
    }

    /*
     * (non-Javadoc)
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#getName()
     */
    public Object getName() {
        return mBuilder.getName();
    }

    /**
     * True if the generated derived class should be considered unique. If
     * non-unique, all properties are added to the primary key so there will be
     * no conflicts between various derived classes derived from the same base
     * storable.
     */
    public boolean isUnique() {
        return mIsUnique;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#isVersioned()
     */
    public boolean isVersioned() {
        return mBuilder.isVersioned();
    }

    /**
     * Sets all the primary key properties of the given master, using the
     * applicable properties of the given index entry.
     *
     * @param indexEntry source of property values
     * @param master master whose primary key properties will be set
     * @deprecated call getReferenceAccess
     */
    @Deprecated
    public void copyToMasterPrimaryKey(Storable indexEntry, S master) throws FetchException {
        getReferenceAccess().copyToMasterPrimaryKey(indexEntry, master);
    }

    /**
     * Sets all the properties of the given index entry, using the applicable
     * properties of the given master.
     *
     * @param indexEntry index entry whose properties will be set
     * @param master source of property values
     * @deprecated call getReferenceAccess
     */
    @Deprecated
    public void copyFromMaster(Storable indexEntry, S master) throws FetchException {
        getReferenceAccess().copyFromMaster(indexEntry, master);
    }

    /**
     * Returns true if the properties of the given index entry match those
     * contained in the master, excluding any version property. This will
     * always return true after a call to copyFromMaster.
     *
     * @param indexEntry
     *            index entry whose properties will be tested
     * @param master
     *            source of property values
     * @deprecated call getReferenceAccess
     */
    @Deprecated
    public boolean isConsistent(Storable indexEntry, S master) throws FetchException {
        return getReferenceAccess().isConsistent(indexEntry, master);
    }

    /**
     * Returns a comparator for ordering index entries.
     * @deprecated call getReferenceAccess
     */
    @Deprecated
    public Comparator<? extends Storable> getComparator() {
        return getReferenceAccess().getComparator();
    }

    /**
     * Create methods for copying properties and testing properties.
     *
     * @throws amazon.carbonado.SupportException
     */
    private void addSpecialMethods(ClassFile cf) throws SupportException {
        // Generate safe names for special methods.
        {
            mCopyToMasterPkMethodName =
                generateSafeMethodName(mMasterStorableInfo, COPY_TO_MASTER_PK_PREFIX);

            mCopyFromMasterMethodName =
                generateSafeMethodName(mMasterStorableInfo, COPY_FROM_MASTER_PREFIX);

            mIsConsistentMethodName =
                generateSafeMethodName(mMasterStorableInfo, IS_CONSISTENT_PREFIX);
        }

        // Add methods which copies properties between master and index entry.
        addCopyMethod(cf, mCopyFromMasterMethodName);
        addCopyMethod(cf, mCopyToMasterPkMethodName);

        TypeDesc masterStorableType = TypeDesc.forClass(mMasterStorableClass);

        // Add a method which tests all properties of index entry object
        // against master object, excluding version and derived properties.
        {
            TypeDesc[] params = new TypeDesc[] {masterStorableType};
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, mIsConsistentMethodName, TypeDesc.BOOLEAN, params);
            CodeBuilder b = new CodeBuilder(mi);

            for (StorableProperty prop : mCommonProps) {
                if (prop.isVersion() || prop.isDerived()) {
                    continue;
                }
                Label propsAreEqual = b.createLabel();
                addPropertyTest(b, prop, b.getParameter(0), propsAreEqual);
                propsAreEqual.setLocation();
            }

            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);
        }
    }

    private void addCopyMethod(ClassFile cf, String methodName) throws SupportException {
        TypeDesc masterStorableType = TypeDesc.forClass(mMasterStorableClass);

        // void copyXxMaster(Storable master)
        TypeDesc[] params = new TypeDesc[] { masterStorableType };
        MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, methodName, null, params);
        CodeBuilder b = new CodeBuilder(mi);

        boolean toMasterPk;
        if (methodName.equals(mCopyToMasterPkMethodName)) {
            toMasterPk = true;
        } else if (methodName.equals(mCopyFromMasterMethodName)) {
            toMasterPk = false;
        } else {
            throw new IllegalArgumentException();
        }

        for (StorableProperty prop : mCommonProps) {
            if (toMasterPk && !prop.isPrimaryKeyMember()) {
                continue;
            }

            TypeDesc propType = TypeDesc.forClass(prop.getType());

            if (toMasterPk) {
                if (prop.getWriteMethod() == null) {
                    throw new SupportException
                        ("Property does not have a public mutator method: " + prop);
                }
                b.loadLocal(b.getParameter(0));
                b.loadThis();
                b.invokeVirtual(prop.getReadMethodName(), propType, null);
                b.invoke(prop.getWriteMethod());
            } else if (methodName.equals(mCopyFromMasterMethodName)) {
                if (prop.getReadMethod() == null) {
                    throw new SupportException
                        ("Property does not have a public accessor method: " + prop);
                }
                b.loadThis();
                b.loadLocal(b.getParameter(0));
                b.invoke(prop.getReadMethod());
                b.invokeVirtual(prop.getWriteMethodName(), null, new TypeDesc[] {propType});
            }

        }

        b.returnVoid();
    }

    /**
     * Generate code to test equality of two properties.
     * @param b
     * @param property
     * @param otherInstance
     * @param propsAreEqual
     */
    private static void addPropertyTest(CodeBuilder b,
                                        StorableProperty<?> property,
                                        LocalVariable otherInstance,
                                        Label propsAreEqual)
    {
        TypeDesc propertyType = TypeDesc.forClass(property.getType());

        b.loadThis();
        b.invokeVirtual(property.getReadMethodName(), propertyType, null);
        b.loadLocal(otherInstance);
        b.invoke(property.getReadMethod());
        CodeBuilderUtil.addValuesEqualCall(b,
                                           propertyType,
                                           true,  // test for null
                                           propsAreEqual,
                                           true); // branch to propsAreEqual when equal

        // Property values differ, so return false.
        b.loadConstant(false);
        b.returnValue(TypeDesc.BOOLEAN);
    }

    /**
     * Generates a property name which doesn't clash with any already defined.
     */
    private String generateSafeMethodName(StorableInfo info, String prefix) {
        Class type = info.getStorableType();

        // Try a few times to generate a unique name. There's nothing special
        // about choosing 100 as the limit.
        int value = 0;
        for (int i = 0; i < 100; i++) {
            String name = prefix + value;
            if (!methodExists(type, name)) {
                return name;
            }
            value = name.hashCode();
        }

        throw new InternalError("Unable to create unique method name starting with: "
                + prefix);
    }

    /**
     * Look for conflicting method names
     */
    private boolean methodExists(Class clazz, String name) {
        Method[] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals(name)) {
                return true;
            }
        }

        if (clazz.getSuperclass() != null
                && methodExists(clazz.getSuperclass(), name)) {
            return true;
        }

        Class[] interfaces = clazz.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            if (methodExists(interfaces[i], name)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param prop
     *            StorableProperty to add to the list of properties to generate.
     *            This method will set the nullable and version annotations, but
     *            not the primary key or direction annotations. Warning: this
     *            should only be called with properties that are also in the
     *            master
     * @return resulting SyntheticProperty
     */
    private SyntheticProperty addProperty(StorableProperty prop) {
        SyntheticProperty refProp = mBuilder.addProperty(prop.getName(),
                                                         prop.getType());
        refProp.setReadMethodName(prop.getReadMethodName());
        refProp.setWriteMethodName(prop.getWriteMethodName());
        refProp.setIsNullable(prop.isNullable());
        refProp.setIsVersion(prop.isVersion());

        // If property has an adapter, make sure it is still applied here.
        if (prop.getAdapter() != null) {
            refProp.setAdapter(prop.getAdapter());
        }
        mCommonProps.add(prop);
        return refProp;
    }

    /**
     * Mechanism for making the name correspond to the semantics of a reference
     * (an index, really)
     */
    class ReferenceClassNameProvider implements ClassNameProvider {
        boolean mUniquely;

        public ReferenceClassNameProvider(boolean unique) {
            super();
            mUniquely = unique;
        }

        public String getName() {
            StringBuilder b = new StringBuilder();
            b.append(SyntheticStorableReferenceBuilder.this.getName());
            b.append('~');
            b.append(mUniquely ? 'U' : 'N');

            Iterator<String> props =
                SyntheticStorableReferenceBuilder.this.mPrimaryKey.getProperties();

            while (props.hasNext()) {
                String prop = props.next();
                if (mExtraPkProps.contains(prop)) {
                    continue;
                }
                if (prop.charAt(0) != '+' && prop.charAt(0) != '-') {
                    b.append('~');
                }
                b.append(prop);
            }
            return b.toString();
        }

        public boolean isExplicit() {
            return true;
        }
    }
}

