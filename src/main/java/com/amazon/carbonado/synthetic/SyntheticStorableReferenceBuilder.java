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
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Join;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.spi.CodeBuilderUtil;
import com.amazon.carbonado.util.ThrowUnchecked;

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
 */
public class SyntheticStorableReferenceBuilder<S extends Storable>
        implements SyntheticBuilder {

    // The property setter will be called something like "setAllProperties_0"
    private static final String ALL_PROPERTIES_PREFIX = "setAllProperties_";

    private static final String MASTER_PROPERTY_PREFIX = "master_";

    private static final String IS_CONSISTENT_PREFIX = "getIsConsistent_";

    // Information about the storable from which this one is derived
    // private StorableInfo<S> mBaseStorableInfo;
    private Class mMasterStorableClass;

    // Stashed copy of the results of calling StorableIntrospector.examine(...)
    // on the master storable class.
    private StorableInfo<S> mMasterStorableInfo;

    // This guy will actually do the work
    private SyntheticStorableBuilder mBuilder;

    // Primary key of generated storable.
    private SyntheticKey mPrimaryKey;

    // Elements added to primary key to ensure uniqueness
    private Set<String> mExtraPkProps;

    // True if the specified properties for this derived reference class are sufficient to
    // uniquely identify a unique instance of the referent.
    private boolean mIsUnique = true;

    // The result of building
    private Class<? extends Storable> mSyntheticClass;

    // The list of properties explicitly added to this reference builder
    private List<SyntheticProperty> mUserProps;

    // This list of properties the master and this one have in common
    // The StorableProperties that get added to this list
    // are retrieved from the master.
    private List<StorableProperty> mCommonProps;

    // The builder generates and retains a reference to various
    // methods which make it possible to implement the "StorableReferenceBean"
    // interface
    // (if there were any need to formalize it): loadMaster,
    // copyCommonProperties,
    // setAllProperties, and isConsistent
    private String mNameForSetAllPropertiesMethod;

    private Method mSetAllPropertiesMethod;

    private String mNameForIsConsistentMethod;

    private Method mIsConsistentMethod;

    private String mNameForGetMasterMethod;

    private Method mGetMasterMethod;

    private Comparator<? extends Storable> mComparator;

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

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#getStorableClass()
     */
    public Class<? extends Storable> getStorableClass() throws IllegalStateException {
        if (null == mSyntheticClass) {
            mSyntheticClass = mBuilder.getStorableClass();

            // We need a comparator which follows the same order as the generated
            // storable.  We can't construct it until we get here
            {
                BeanComparator bc = BeanComparator.forClass(mSyntheticClass);
                Iterator<String> props = mPrimaryKey.getProperties();
                while (props.hasNext()) {
                    String prop = props.next();
                    // BeanComparator knows how to handle the '+' or '-' prefix.
                    bc = bc.orderBy(prop);
                    bc = bc.caseSensitive();
                }
                mComparator = bc;
            }
        }
        return mSyntheticClass;
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
        if (null == prop) {
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
     * Loads the master object referenced by the given index entry.
     *
     * @param indexEntry
     *            index entry which points to master
     * @return master or null if missing
     */
    public S loadMaster(Storable indexEntry) throws FetchException {
        if (null == mGetMasterMethod) {
            try {
                mGetMasterMethod = getStorableClass().getMethod(mNameForGetMasterMethod,
                                                                (Class[]) null);
            } catch (NoSuchMethodException e) {
                throw new UndeclaredThrowableException(e);
            }
        }

        try {
            return (S) mGetMasterMethod.invoke(indexEntry, (Object[]) null);
        } catch (Exception e) {
            ThrowUnchecked.fireFirstDeclaredCause(e, FetchException.class);
            // Not reached.
            return null;
        }
    }

    /**
     * Sets all the properties of the given index entry, using the applicable
     * properties of the given master.
     *
     * @param indexEntry
     *            index entry whose properties will be set
     * @param master
     *            source of property values
     */
    public void setAllProperties(Storable indexEntry, S master) {

        if (null == mSetAllPropertiesMethod) {
            try {
                mSetAllPropertiesMethod = mSyntheticClass.getMethod(mNameForSetAllPropertiesMethod,
                                                                    mMasterStorableClass);
            } catch (NoSuchMethodException e) {
                throw new UndeclaredThrowableException(e);
            }
        }

        try {
            mSetAllPropertiesMethod.invoke(indexEntry, master);
        } catch (Exception e) {
            ThrowUnchecked.fireFirstDeclaredCause(e);
        }
    }

    /**
     * Returns true if the properties of the given index entry match those
     * contained in the master, excluding any version property. This will
     * always return true after a call to setAllProperties.
     *
     * @param indexEntry
     *            index entry whose properties will be tested
     * @param master
     *            source of property values
     */
    public boolean isConsistent(Storable indexEntry, S master) {

        if (null == mIsConsistentMethod) {
            try {
                mIsConsistentMethod = mSyntheticClass.getMethod(mNameForIsConsistentMethod,
                                                                mMasterStorableClass);
            } catch (NoSuchMethodException e) {
                throw new UndeclaredThrowableException(e);
            }
        }

        try {
            return (Boolean) mIsConsistentMethod.invoke(indexEntry, master);
        } catch (Exception e) {
            ThrowUnchecked.fireFirstDeclaredCause(e);
            // Not reached.
            return false;
        }
    }

    /**
     * Returns a comparator for ordering index entries.
     */
    public Comparator<? extends Storable> getComparator() {
        return mComparator;
    }

    /**
     * Create methods for copying properties to and from and for looking up the
     * master object
     *
     * @throws amazon.carbonado.SupportException
     */
    private void addSpecialMethods(ClassFile cf) throws SupportException {
        TypeDesc masterStorableType = TypeDesc.forClass(mMasterStorableClass);
        // The set master method is used to load the master object and set it
        // into the master property of this reference storable
        String nameForSetMasterMethod;

        // Generate safe names for special methods.
        {
            String safeName = generateSafePropertyName(mMasterStorableInfo,
                                                       MASTER_PROPERTY_PREFIX);
            // Don't need to pass the class in, it's not a boolean so we'll get
            // a "get<foo>" flavor read method
            mNameForGetMasterMethod = SyntheticProperty.makeReadMethodName(safeName,
                                                                           Object.class);
            nameForSetMasterMethod = SyntheticProperty.makeWriteMethodName(safeName);
            mNameForSetAllPropertiesMethod = generateSafeMethodName(mMasterStorableInfo,
                                                                    ALL_PROPERTIES_PREFIX);
            mNameForIsConsistentMethod = generateSafeMethodName(mMasterStorableInfo, IS_CONSISTENT_PREFIX);
        }

        // Add a method which sets all properties of index entry object from
        // master object.
        {
            // void setAllProperties(Storable master)
            TypeDesc[] params = new TypeDesc[] { masterStorableType };
            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC,
                                         mNameForSetAllPropertiesMethod,
                                         null,
                                         params);
            CodeBuilder b = new CodeBuilder(mi);

            // Set Join property: this.setMaster(master)
            // (stash a reference to the master object)
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeVirtual(nameForSetMasterMethod, null, params);

            // Copy across all the properties. At this point they've
            // all been added to the property list
            for (StorableProperty prop : mCommonProps) {
                if (prop.isPrimaryKeyMember()) {
                    // No need to set this property, since setting join to
                    // master already took care of it.
                    continue;
                }
                b.loadThis();
                b.loadLocal(b.getParameter(0));
                if (prop.getReadMethod() == null) {
                    throw new SupportException("Property does not have a public accessor method: "
                            + prop);
                }
                b.invoke(prop.getReadMethod());
                b.invokeVirtual(prop.getWriteMethodName(),
                                null,
                                new TypeDesc[] { TypeDesc.forClass(prop.getType()) });

            }
            b.returnVoid();
        }

        // Add a join property for looking up master object. Since binding is
        // based on primary key properties which match to index entry object,
        // the join is natural -- no need to specify internal and external
        // properties.
        {
            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC_ABSTRACT,
                                         mNameForGetMasterMethod,
                                         masterStorableType,
                                         null);
            mi.addException(TypeDesc.forClass(FetchException.class));
            mi.addRuntimeVisibleAnnotation(TypeDesc.forClass(Join.class));
            mi.addRuntimeVisibleAnnotation(TypeDesc.forClass(Nullable.class));

            cf.addMethod(Modifiers.PUBLIC_ABSTRACT,
                         nameForSetMasterMethod,
                         null,
                         new TypeDesc[] { masterStorableType });
        }

        // Add a method which tests all properties of index entry object
        // against master object, excluding the version property.
        {
            TypeDesc[] params = new TypeDesc[] {masterStorableType};
            MethodInfo mi = cf.addMethod
                (Modifiers.PUBLIC, mNameForIsConsistentMethod, TypeDesc.BOOLEAN, params);
            CodeBuilder b = new CodeBuilder(mi);

            for (StorableProperty prop : mCommonProps) {
                if (prop.isVersion()) {
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

//        if (!methodExists(type, prefix)) {
//            return prefix;
//        }

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
     * Generates a property name which doesn't clash with any already defined.
     */
    protected String generateSafePropertyName(StorableInfo info, String prefix)
    {
        Map<String, ? extends StorableProperty> properties = info.getAllProperties();
        Class type = info.getStorableType();

        // Try a few times to generate unique name. There's nothing special
        // about choosing 100 as the limit.
        for (int i = 0; i < 100; i++) {
            String name = prefix + i;
            if (properties.containsKey(name)) {
                continue;
            }
            if (methodExists(type, SyntheticProperty.makeReadMethodName(name,
                                                                        type))) {
                continue;
            }
            if (methodExists(type, SyntheticProperty.makeWriteMethodName(name))) {
                continue;
            }
            return name;
        }

        throw new InternalError("Unable to create unique property name starting with: "
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

