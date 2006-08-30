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

package com.amazon.carbonado.cursor;

import java.lang.reflect.Field;

import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.FieldInfo;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;

import org.cojen.util.ClassInjector;
import org.cojen.util.KeyFactory;
import org.cojen.util.SoftValuedHashMap;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.util.QuickConstructorGenerator;

import com.amazon.carbonado.spi.CodeBuilderUtil;
import static com.amazon.carbonado.spi.CommonMethodNames.*;

/**
 * Given two joined types <i>A</i> and <i>B</i>, this factory converts a cursor
 * over type <i>A</i> into a cursor over type <i>B</i>. For example, consider
 * two storable types, Employer and Person. A query filter for persons with a
 * given employer might look like: {@code "employer.name = ?" }  The join can be
 * manually implemented by querying for employers and then using this factory
 * to produce persons:
 *
 * <pre>
 * JoinedCursorFactory&lt;Employer, Person&gt; factory = new JoinedCursorFactory&lt;Employer, Person&gt;
 *     (repo, Person.class, "employer", Employer.class);
 *
 * Cursor&lt;Employer&gt; employerCursor = repo.storageFor(Employer.class)
 *     .query("name = ?").with(...).fetch();
 *
 * Cursor&lt;Person&gt; personCursor = factory.join(employerCursor);
 * </pre>
 *
 * Chained properties are supported as well. A query filter for persons with an
 * employer in a given state might look like: {@code "employer.address.state = ?" }
 * The join can be manually implemented as:
 *
 * <pre>
 * JoinedCursorFactory&lt;Address, Person&gt; factory = new JoinedCursorFactory&lt;Address, Person&gt;
 *     (repo, Person.class, "employer.address", Address.class);
 *
 * Cursor&lt;Address&gt; addressCursor = repo.storageFor(Address.class)
 *     .query("state = ?").with(...).fetch();
 *
 * Cursor&lt;Person&gt; personCursor = factory.join(addressCursor);
 * </pre>
 *
 * @author Brian S O'Neill
 */
public class JoinedCursorFactory<A, B extends Storable> {
    private static final String STORAGE_FIELD_NAME = "storage";
    private static final String QUERY_FIELD_NAME = "query";
    private static final String QUERY_FILTER_FIELD_NAME = "queryFilter";
    private static final String ACTIVE_A_FIELD_NAME = "active";

    private static final Map<Object, Class> cJoinerCursorClassCache;

    static {
        cJoinerCursorClassCache = new SoftValuedHashMap();
    }

    private static synchronized <B extends Storable> Joiner<?, B>
        newBasicJoiner(StorableProperty<B> bToAProperty, Storage<B> bStorage)
        throws FetchException
    {
        Class<?> aType = bToAProperty.getType();

        final Object key = KeyFactory.createKey
            (new Object[] {aType,
                           bToAProperty.getEnclosingType(),
                           bToAProperty.getName()});

        Class clazz = cJoinerCursorClassCache.get(key);

        if (clazz == null) {
            clazz = generateBasicJoinerCursor(aType, bToAProperty);
            cJoinerCursorClassCache.put(key, clazz);
        }

        // Transforming cursor class may need a Query to operate on.
        Query<B> bQuery = null;
        try {
            String filter = (String) clazz.getField(QUERY_FILTER_FIELD_NAME).get(null);
            bQuery = bStorage.query(filter);
        } catch (NoSuchFieldException e) {
        } catch (IllegalAccessException e) {
        }

        BasicJoiner.Factory<?, B> factory = (BasicJoiner.Factory<?, B>) QuickConstructorGenerator
            .getInstance(clazz, BasicJoiner.Factory.class);

        return new BasicJoiner(factory, bStorage, bQuery);
    }

    private static <B extends Storable> Class<Cursor<B>>
        generateBasicJoinerCursor(Class<?> aType, StorableProperty<B> bToAProperty)
    {
        final int propCount = bToAProperty.getJoinElementCount();

        // Determine if join is one-to-one, in which case slightly more optimal
        // code can be generated.
        boolean isOneToOne = true;
        for (int i=0; i<propCount; i++) {
            if (!bToAProperty.getInternalJoinElement(i).isPrimaryKeyMember()) {
                isOneToOne = false;
                break;
            }
            if (!bToAProperty.getExternalJoinElement(i).isPrimaryKeyMember()) {
                isOneToOne = false;
                break;
            }
        }

        Class<B> bType = bToAProperty.getEnclosingType();

        String packageName;
        {
            String name = bType.getName();
            int index = name.lastIndexOf('.');
            if (index >= 0) {
                packageName = name.substring(0, index);
            } else {
                packageName = "";
            }
        }

        ClassLoader loader = bType.getClassLoader();

        ClassInjector ci = ClassInjector.create(packageName + ".JoinedCursor", loader);
        Class superclass = isOneToOne ? TransformedCursor.class : MultiTransformedCursor.class;
        ClassFile cf = new ClassFile(ci.getClassName(), superclass);
        cf.markSynthetic();
        cf.setSourceFile(JoinedCursorFactory.class.getName());
        cf.setTarget("1.5");

        final TypeDesc queryType = TypeDesc.forClass(Query.class);
        final TypeDesc cursorType = TypeDesc.forClass(Cursor.class);
        final TypeDesc storageType = TypeDesc.forClass(Storage.class);
        final TypeDesc storableType = TypeDesc.forClass(Storable.class);

        if (isOneToOne) {
            cf.addField(Modifiers.PRIVATE.toFinal(true), STORAGE_FIELD_NAME, storageType);
        } else {
            // Field to hold query which fetches type B.
            cf.addField(Modifiers.PRIVATE.toFinal(true), QUERY_FIELD_NAME, queryType);
        }

        boolean canSetAReference = bToAProperty.getWriteMethod() != null;

        if (canSetAReference && !isOneToOne) {
            // Field to hold active A storable.
            cf.addField(Modifiers.PRIVATE, ACTIVE_A_FIELD_NAME, TypeDesc.forClass(aType));
        }

        // Constructor accepts a Storage and Query, but Storage is only used
        // for one-to-one, and Query is only used for one-to-many.
        {
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC,
                                              new TypeDesc[] {cursorType, storageType, queryType});
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadLocal(b.getParameter(0)); // pass A cursor to superclass
            b.invokeSuperConstructor(new TypeDesc[] {cursorType});

            if (isOneToOne) {
                b.loadThis();
                b.loadLocal(b.getParameter(1)); // push B storage to stack
                b.storeField(STORAGE_FIELD_NAME, storageType);
            } else {
                b.loadThis();
                b.loadLocal(b.getParameter(2)); // push B query to stack
                b.storeField(QUERY_FIELD_NAME, queryType);
            }

            b.returnVoid();
        }

        // For one-to-many, a query is needed. Save the query filter in a
        // public static field to be grabbed later.
        if (!isOneToOne) {
            StringBuilder queryBuilder = new StringBuilder();

            for (int i=0; i<propCount; i++) {
                if (i > 0) {
                    queryBuilder.append(" & ");
                }
                queryBuilder.append(bToAProperty.getInternalJoinElement(i).getName());
                queryBuilder.append(" = ?");
            }

            FieldInfo fi = cf.addField(Modifiers.PUBLIC.toStatic(true).toFinal(true),
                                       QUERY_FILTER_FIELD_NAME, TypeDesc.STRING);
            fi.setConstantValue(queryBuilder.toString());
        }

        // Implement the transform method.
        if (isOneToOne) {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED, "transform", TypeDesc.OBJECT,
                                         new TypeDesc[] {TypeDesc.OBJECT});
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable aVar = b.createLocalVariable(null, storableType);
            b.loadLocal(b.getParameter(0));
            b.checkCast(TypeDesc.forClass(aType));
            b.storeLocal(aVar);

            // Prepare B storable.
            b.loadThis();
            b.loadField(STORAGE_FIELD_NAME, storageType);
            b.invokeInterface(storageType, PREPARE_METHOD_NAME, storableType, null);
            LocalVariable bVar = b.createLocalVariable(null, storableType);
            b.checkCast(TypeDesc.forClass(bType));
            b.storeLocal(bVar);

            // Copy pk property values from A to B.
            for (int i=0; i<propCount; i++) {
                StorableProperty<B> internal = bToAProperty.getInternalJoinElement(i);
                StorableProperty<?> external = bToAProperty.getExternalJoinElement(i);

                b.loadLocal(bVar);
                b.loadLocal(aVar);
                b.invoke(external.getReadMethod());
                b.invoke(internal.getWriteMethod());
            }

            // tryLoad b.
            b.loadLocal(bVar);
            b.invokeInterface(storableType, TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);
            Label wasLoaded = b.createLabel();
            b.ifZeroComparisonBranch(wasLoaded, "!=");

            b.loadNull();
            b.returnValue(storableType);

            wasLoaded.setLocation();

            if (canSetAReference) {
                b.loadLocal(bVar);
                b.loadLocal(aVar);
                b.invoke(bToAProperty.getWriteMethod());
            }

            b.loadLocal(bVar);
            b.returnValue(storableType);
        } else {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED, "transform", cursorType,
                                         new TypeDesc[] {TypeDesc.OBJECT});
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable aVar = b.createLocalVariable(null, storableType);
            b.loadLocal(b.getParameter(0));
            b.checkCast(TypeDesc.forClass(aType));
            b.storeLocal(aVar);

            if (canSetAReference) {
                b.loadThis();
                b.loadLocal(aVar);
                b.storeField(ACTIVE_A_FIELD_NAME, TypeDesc.forClass(aType));
            }

            // Populate query parameters.
            b.loadThis();
            b.loadField(QUERY_FIELD_NAME, queryType);

            for (int i=0; i<propCount; i++) {
                StorableProperty<?> external = bToAProperty.getExternalJoinElement(i);
                b.loadLocal(aVar);
                b.invoke(external.getReadMethod());

                TypeDesc bindType = CodeBuilderUtil.bindQueryParam(external.getType());
                CodeBuilderUtil.convertValue(b, external.getType(), bindType.toClass());
                b.invokeInterface(queryType, WITH_METHOD_NAME, queryType,
                                  new TypeDesc[] {bindType});
            }

            // Now fetch and return.
            b.invokeInterface(queryType, FETCH_METHOD_NAME, cursorType, null);
            b.returnValue(cursorType);
        }

        if (canSetAReference && !isOneToOne) {
            // Override the "next" method to set A object on B.
            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, "next", TypeDesc.OBJECT, null);
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.invokeSuper(TypeDesc.forClass(MultiTransformedCursor.class),
                          "next", TypeDesc.OBJECT, null);
            b.checkCast(TypeDesc.forClass(bType));
            b.dup();

            b.loadThis();
            b.loadField(ACTIVE_A_FIELD_NAME, TypeDesc.forClass(aType));
            b.invoke(bToAProperty.getWriteMethod());

            b.returnValue(storableType);
        }

        return (Class<Cursor<B>>) ci.defineClass(cf);
    }

    private final Joiner<A, B> mJoiner;

    /**
     * @param repo access to storage instances for properties
     * @param bType type of <i>B</i> instances
     * @param bToAProperty property of <i>B</i> type which maps to instances of
     * <i>A</i> type.
     * @param aType type of <i>A</i> instances
     * @throws IllegalArgumentException if property type is not <i>A</i>
     */
    public JoinedCursorFactory(Repository repo,
                               Class<B> bType,
                               String bToAProperty,
                               Class<A> aType)
        throws SupportException, FetchException, RepositoryException
    {
        this(repo,
             ChainedProperty.parse(StorableIntrospector.examine(bType), bToAProperty),
             aType);
    }

    /**
     * @param repo access to storage instances for properties
     * @param bToAProperty property of <i>B</i> type which maps to instances of
     * <i>A</i> type.
     * @param aType type of <i>A</i> instances
     * @throws IllegalArgumentException if property type is not <i>A</i>
     */
    public JoinedCursorFactory(Repository repo,
                               ChainedProperty<B> bToAProperty,
                               Class<A> aType)
        throws SupportException, FetchException, RepositoryException
    {
        if (bToAProperty.getType() != aType) {
            throw new IllegalArgumentException
                ("Property is not of type \"" + aType.getName() + "\": " +
                 bToAProperty);
        }

        StorableProperty<B> primeB = bToAProperty.getPrimeProperty();
        Storage<B> primeBStorage = repo.storageFor(primeB.getEnclosingType());

        Joiner joiner = newBasicJoiner(primeB, primeBStorage);

        int chainCount = bToAProperty.getChainCount();
        for (int i=0; i<chainCount; i++) {
            StorableProperty prop = bToAProperty.getChainedProperty(i);
            Storage storage = repo.storageFor(prop.getEnclosingType());

            joiner = new MultiJoiner(newBasicJoiner(prop, storage), joiner);
        }

        mJoiner = (Joiner<A, B>) joiner;
    }

    /**
     * Given a cursor over <i>A</i>, returns a new cursor over joined property
     * of type <i>B</i>.
     */
    public Cursor<B> join(Cursor<A> cursor) {
        return mJoiner.join(cursor);
    }

    private static interface Joiner<A, B extends Storable> {
        Cursor<B> join(Cursor<A> cursor);
    }

    /**
     * Support for joins without an intermediate hop.
     */
    private static class BasicJoiner<A, B extends Storable> implements Joiner<A, B> {
        private final Factory<A, B> mJoinerFactory;
        private final Storage<B> mBStorage;
        private final Query<B> mBQuery;

        BasicJoiner(Factory<A, B> factory, Storage<B> bStorage, Query<B> bQuery) {
            mJoinerFactory = factory;
            mBStorage = bStorage;
            mBQuery = bQuery;
        }

        public Cursor<B> join(Cursor<A> cursor) {
            return mJoinerFactory.newJoinedCursor(cursor, mBStorage, mBQuery);
        }

        /**
         * Needs to be public for {@link QuickConstructorGenerator}.
         */
        public static interface Factory<A, B extends Storable> {
            Cursor<B> newJoinedCursor(Cursor<A> cursor, Storage<B> bStorage, Query<B> bQuery);
        }
    }

    /**
     * Support for joins with an intermediate hop -- multi-way joins.
     */
    private static class MultiJoiner<A, X extends Storable, B extends Storable>
        implements Joiner<A, B>
    {
        private final Joiner<A, X> mAToMid;
        private final Joiner<X, B> mMidToB;

        MultiJoiner(Joiner<A, X> aToMidJoiner, Joiner<X, B> midToBJoiner) {
            mAToMid = aToMidJoiner;
            mMidToB = midToBJoiner;
        }

        public Cursor<B> join(Cursor<A> cursor) {
            return mMidToB.join(mAToMid.join(cursor));
        }
    }
}
