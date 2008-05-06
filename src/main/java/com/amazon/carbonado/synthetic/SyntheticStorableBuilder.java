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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.List;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.classfile.attribute.Annotation;
import org.cojen.util.ClassInjector;

import com.amazon.carbonado.AlternateKeys;
import com.amazon.carbonado.Index;
import com.amazon.carbonado.Indexes;
import com.amazon.carbonado.Key;
import com.amazon.carbonado.Name;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Version;

import com.amazon.carbonado.info.StorablePropertyAdapter;
import com.amazon.carbonado.layout.Unevolvable;
import com.amazon.carbonado.util.AnnotationBuilder;
import com.amazon.carbonado.util.AnnotationDescParser;

/**
 * Allows the definition of very simple synthetic storables. Only a primary key
 * index can be defined; at least one property must be a primary key property. A
 * property can be nullable and can be specified as the version property.
 *
 * This class acts both as builder factory and as builder.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 * @author David Rosenstrauch
 */
public class SyntheticStorableBuilder
        implements SyntheticBuilder {

    /**
     * DEFAULT ClassNameProvider
     */
    private class DefaultProvider implements ClassNameProvider {
        DefaultProvider() {
        }

        public String getName() {
            StringBuilder b = new StringBuilder();
            if (null == SyntheticStorableBuilder.this.getName()) {
                b.append("synth");
            } else {
                b.append(SyntheticStorableBuilder.this.getName());
            }

            // Append primary keys first.

            Iterator<String> props = SyntheticStorableBuilder.this.mPrimaryKey.getProperties();
            Set<String> propSet = new HashSet<String>();

            while (props.hasNext()) {
                String prop = props.next();
                if (prop.charAt(0) != '+' && prop.charAt(0) != '-') {
                    propSet.add(prop);
                    b.append('~');
                } else {
                    propSet.add(prop.substring(1));
                }
                b.append(prop);
            }

            // Append remaining properties.
            List<SyntheticProperty> list = SyntheticStorableBuilder.this.getPropertyList();

            for (SyntheticProperty prop : list) {
                if (!propSet.contains(prop.getName())) {
                    b.append('~');
                    b.append(prop.getName());
                }
            }

            return b.toString();
        }

        public boolean isExplicit() {
            return true;
        }
    }

    /**
     * Name for the generated storable
     */
    private String mName;

    /**
     * Set of properties to add to the storable.
     */
    private List<SyntheticProperty> mPropertyList;

    private SyntheticKey mPrimaryKey;

    /**
     * List of alternate keys for this storable
     */
    private List<SyntheticKey> mAlternateKeys;

    /**
     * List of indexes (in addition to the primary and alternate keys) for this storable
     */
    private List<SyntheticIndex> mExtraIndexes;

    /**
     * The partially hydrogenated classfile, together with the injector
     * which can make it into a class.
     */
    private StorableClassFileBuilder mClassFileGenerator;

    /**
     * Lazily instanced -- this is the point of this class. See
     * {@link #getStorableClass}
     */
    private Class<? extends Storable> mStorableClass;

    /**
     * Used to generate the classname for this class.
     */
    private ClassNameProvider mClassNameProvider;

    /**
     * Class loader to use for the synthetic
     */
    private ClassLoader mLoader;

    /**
     * When false, generated class implements Unevolvable.
     */
    private boolean mEvolvable = false;

    /**
     * @param name base name for the generated class.  This is usually a fully qualified
     * name, a la "com.amazon.carbonado.storables.happy.synthetic.storable"
     * @param loader {@link ClassLoader} to use for the generated class
     */
    public SyntheticStorableBuilder(String name, ClassLoader loader) {
        mName = name;
        mLoader = loader;
        mPropertyList = new ArrayList<SyntheticProperty>();
        mAlternateKeys = new ArrayList<SyntheticKey>();
        mExtraIndexes = new ArrayList<SyntheticIndex>();
        mClassNameProvider = new DefaultProvider();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#prepare()
     */
    public ClassFileBuilder prepare() throws SupportException {
        if (mPrimaryKey == null) {
            throw new IllegalStateException("Primary key not defined");
        }

        // Clear the cached result, if any
        mStorableClass = null;

        mClassFileGenerator = new StorableClassFileBuilder(
             mClassNameProvider,
             mLoader,
             SyntheticStorableBuilder.class,
             mEvolvable);
        ClassFile cf = mClassFileGenerator.getClassFile();

        for (SyntheticProperty prop : mPropertyList) {
            definePropertyBeanMethods(cf, prop);
        }

        definePrimaryKey(cf);
        defineAlternateKeys(cf);
        defineIndexes(cf);

        return mClassFileGenerator;
    }

    public Class<? extends Storable> getStorableClass() {
        if (null == mStorableClass) {
            mStorableClass = mClassFileGenerator.build();
        }
        return mStorableClass;
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

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#addProperty(java.lang.String,
     *      java.lang.Class)
     */
    public SyntheticProperty addProperty(String name, Class type) {
        SyntheticProperty prop = new SyntheticProperty(name, type);
        mPropertyList.add(prop);
        return prop;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#addProperty(com.amazon.carbonado.synthetic.SyntheticProperty)
     */
    public SyntheticProperty addProperty(SyntheticProperty prop) {
        mPropertyList.add(prop);
        return prop;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#hasProperty(java.lang.String)
     */
    public boolean hasProperty(String name) {
        for (SyntheticProperty prop : mPropertyList) {
            if (prop.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#addPrimaryKey()
     */
    public SyntheticKey addPrimaryKey() {
        if (mPrimaryKey == null) {
            mPrimaryKey = new SyntheticKey();
        }
        return mPrimaryKey;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#addAlternateKey()
     */
    public SyntheticKey addAlternateKey() {
        SyntheticKey alternateKey = new SyntheticKey();
        mAlternateKeys.add(alternateKey);
        return alternateKey;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#addIndex()
     */
    public SyntheticIndex addIndex() {
        SyntheticIndex index = new SyntheticIndex();
        mExtraIndexes.add(index);
        return index;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazon.carbonado.synthetic.SyntheticBuilder#isVersioned()
     */
    public boolean isVersioned() {
        for (SyntheticProperty prop : mPropertyList) {
            if (prop.isVersion()) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return Returns the classNameProvider.
     */
    public ClassNameProvider getClassNameProvider() {
        return mClassNameProvider;
    }

    /**
     * @param classNameProvider
     *            The classNameProvider to set.
     */
    public void setClassNameProvider(ClassNameProvider classNameProvider) {
        mClassNameProvider = classNameProvider;
    }

    /**
     * By default, generated storable implements the Unevolvable marker
     * interface, which can affect how it is encoded. It usually does not make
     * sense to support storable evolution new versions can be (and often will
     * be) given different names.
     *
     * <p>Pass in true to change from the default behavior, and not implement
     * Unevolvable. When doing so, a ClassNameProvider should also be provided
     * to ensure consistent naming which does not include property names.
     */
    public void setEvolvable(boolean evolvable) {
        mEvolvable = evolvable;
    }

    /**
     * Decorate a classfile with the @PrimaryKey for this synthetic storable.
     *
     * @param cf ClassFile to decorate
     */
    private void definePrimaryKey(ClassFile cf) {
        if (mPrimaryKey == null) {
            return;
        }

        // Add primary key annotation
        //
        // @PrimaryKey(value={"+/-propName", "+/-propName", ...})

        Annotation pk = cf.addRuntimeVisibleAnnotation(TypeDesc.forClass(PrimaryKey.class));

        Annotation.MemberValue[] props =
            new Annotation.MemberValue[mPrimaryKey.getPropertyCount()];
        pk.putMemberValue("value", props);
        int propPos = 0;
        Iterator<String> propNames = mPrimaryKey.getProperties();
        while (propNames.hasNext()) {
            String propName = propNames.next();
            props[propPos] = pk.makeMemberValue(propName);
            propPos++;
        }
    }

    /**
     * Decorate a classfile with the @AlternateKeys for this synthetic storable.
     *
     * @param cf ClassFile to decorate
     */
    private void defineAlternateKeys(ClassFile cf) {
        // Add alternate keys annotation
        //
        // @AlternateKeys(value={
        //     @Key(value={"+/-propName", "+/-propName", ...})
        // })
        defineIndexes(cf, mAlternateKeys, AlternateKeys.class, Key.class);
    }

    /**
     * Decorate a classfile with the @Indexes for this synthetic storable.
     *
     * @param cf ClassFile to decorate
     */
    private void defineIndexes(ClassFile cf) {
        // Add indexes annotation
        //
        // @Indexes(value={
        //     @Index(value={"+/-propName", "+/-propName", ...})
        // })
        defineIndexes(cf, mExtraIndexes, Indexes.class, Index.class);
    }

    private void defineIndexes(ClassFile cf,
                               List<? extends SyntheticPropertyList> definedIndexes,
                               Class annotationGroupClass,
                               Class annotationClass) {
        if (definedIndexes.size() == 0) {
            return;
        }

        Annotation indexSet = cf.addRuntimeVisibleAnnotation(TypeDesc.forClass(annotationGroupClass));

        Annotation.MemberValue[] indexes = new Annotation.MemberValue[definedIndexes.size()];

        // indexSet.value -> indexes
        indexSet.putMemberValue("value", indexes);

        int position = 0;
        for (SyntheticPropertyList extraIndex : definedIndexes) {
            Annotation index = addIndex(indexSet, indexes, position++, annotationClass);

            Annotation.MemberValue[] indexProps =
                new Annotation.MemberValue[extraIndex.getPropertyCount()];
            index.putMemberValue("value", indexProps);
            int propPos = 0;
            Iterator<String> propNames = extraIndex.getProperties();
            while (propNames.hasNext()) {
                String propName = propNames.next();
                indexProps[propPos] = index.makeMemberValue(propName);
                propPos++;
            }
        }
    }

    /**
     * @param annotator
     *            source of annotation (eg, makeAnnotation and makeMemberValue)
     * @param indexes
     * @param position
     * @param annotationClass TODO
     * @return
     */
    private Annotation addIndex(Annotation annotator,
                                Annotation.MemberValue[] indexes,
                                int position,
                                Class annotationClass)
    {
        assert (indexes.length > position);

        Annotation index = annotator.makeAnnotation();
        index.setType(TypeDesc.forClass(annotationClass));
        indexes[position] = annotator.makeMemberValue(index);
        return index;
    }

    /**
     * Add the get & set methods for this property
     *
     * @return true if version property was added
     */
    protected boolean definePropertyBeanMethods(ClassFile cf,
                                                SyntheticProperty property)
    {
        TypeDesc propertyType = TypeDesc.forClass(property.getType());
        // Add property get method.
        final MethodInfo mi = cf.addMethod(Modifiers.PUBLIC_ABSTRACT,
                                           property.getReadMethodName(),
                                           propertyType,
                                           null);

        if (property.getName() != null) {
            // Define @Name
            Annotation ann = mi.addRuntimeVisibleAnnotation(TypeDesc.forClass(Name.class));
            ann.putMemberValue("value", property.getName());
        }

        if (property.isNullable()) {
            mi.addRuntimeVisibleAnnotation(TypeDesc.forClass(Nullable.class));
        }
        boolean versioned = false;
        if (property.isVersion()) {
            mi.addRuntimeVisibleAnnotation(TypeDesc.forClass(Version.class));
            versioned = true;
        }

        if (property.getAdapter() != null) {
            StorablePropertyAdapter adapter = property.getAdapter();
            Annotation ann = mi.addRuntimeVisibleAnnotation
                (TypeDesc.forClass(adapter.getAnnotation().getAnnotationType()));
            java.lang.annotation.Annotation jann = adapter.getAnnotation().getAnnotation();
            if (jann != null) {
                new AnnotationBuilder().visit(jann, ann);
            }
        }

        List<String> annotationDescs = property.getAccessorAnnotationDescriptors();
        if (annotationDescs != null && annotationDescs.size() > 0) {
            for (String desc : annotationDescs) {
                new AnnotationDescParser(desc) {
                    @Override
                    protected Annotation buildRootAnnotation(TypeDesc rootAnnotationType) {
                        return mi.addRuntimeVisibleAnnotation(rootAnnotationType);
                    }
                }.parse(null);
            }
        }

        // Add property set method.
        cf.addMethod(Modifiers.PUBLIC_ABSTRACT,
                     property.getWriteMethodName(),
                     null,
                     new TypeDesc[] { propertyType });

        return versioned;
    }

    /**
     * Frequently used by the {@link SyntheticBuilder.ClassNameProvider} as a
     * basis for the generated classname
     * @return builder name
     */
    protected String getName() {
        return mName;
    }

    /**
     * Frequently used by the {@link SyntheticBuilder.ClassNameProvider} as a
     * basis for the generated classname
     * @return properties for this storable
     */
    protected List<SyntheticProperty> getPropertyList() {
        return mPropertyList;
    }

    @Override
    public String toString() {
        return mName + mPropertyList.toString();
    }

    /**
     * This really belongs in Cojen -- it's just the injector and classfile,
     * together again
     *
     * @author Don Schneider
     *
     */
    static class StorableClassFileBuilder extends ClassFileBuilder {

        /**
         * Initialize the injector and classfile, defining the basic information
         * for a synthetic class
         *
         * @param className
         *            name with which to christen this class -- must be globally
         *            unique
         * @param sourceClass
         *            class to credit with creating the class, usually the class
         *            making the call
         */
        StorableClassFileBuilder(ClassNameProvider nameProvider,
                                 ClassLoader loader,
                                 Class sourceClass,
                                 boolean evolvable) {
            String className = nameProvider.getName();

            if (nameProvider.isExplicit()) {
                mInjector = ClassInjector.createExplicit(className, loader);
            } else {
                mInjector = ClassInjector.create(className, loader);
            }

            mClassFile = new ClassFile(mInjector.getClassName());

            Modifiers modifiers = mClassFile.getModifiers().toAbstract(true);
            mClassFile.setModifiers(modifiers);
            mClassFile.addInterface(Storable.class);
            if (!evolvable) {
                mClassFile.addInterface(Unevolvable.class);
            }
            mClassFile.markSynthetic();
            mClassFile.setSourceFile(sourceClass.getName());
            mClassFile.setTarget("1.5");

            mClassFile.addDefaultConstructor();
        }
    }
}
