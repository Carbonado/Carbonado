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
 * Given two joined types <i>source</i> and <i>target</i>, this factory
 * converts a cursor over type <i>source</i> into a cursor over type
 * <i>target</i>. For example, consider two storable types, Employer and
 * Person. A query filter for persons with a given employer might look like:
 * {@code "employer.name = ?" }  The join can be manually implemented by
 * querying for employers (the source) and then using this factory to produce
 * persons (the target):
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
 * @see TransformedCursor
 * @see MultiTransformedCursor
 * @param <S> source type, can be anything
 * @param <T> target type, must be a Storable
 */
public class JoinedCursorFactory<S, T extends Storable> {
    private static final String STORAGE_FIELD_NAME = "storage";
    private static final String QUERY_FIELD_NAME = "query";
    private static final String QUERY_FILTER_FIELD_NAME = "queryFilter";
    private static final String ACTIVE_SOURCE_FIELD_NAME = "active";

    private static final Map<Object, Class> cJoinerCursorClassCache;

    static {
        cJoinerCursorClassCache = new SoftValuedHashMap();
    }

    private static synchronized <T extends Storable> Joiner<?, T>
        newBasicJoiner(StorableProperty<T> targetToSourceProperty, Storage<T> targetStorage)
        throws FetchException
    {
        Class<?> sourceType = targetToSourceProperty.getType();

        final Object key = KeyFactory.createKey
            (new Object[] {sourceType,
                           targetToSourceProperty.getEnclosingType(),
                           targetToSourceProperty.getName()});

        Class clazz = cJoinerCursorClassCache.get(key);

        if (clazz == null) {
            clazz = generateBasicJoinerCursor(sourceType, targetToSourceProperty);
            cJoinerCursorClassCache.put(key, clazz);
        }

        // Transforming cursor class may need a Query to operate on.
        Query<T> targetQuery = null;
        try {
            String filter = (String) clazz.getField(QUERY_FILTER_FIELD_NAME).get(null);
            targetQuery = targetStorage.query(filter);
        } catch (NoSuchFieldException e) {
        } catch (IllegalAccessException e) {
        }

        BasicJoiner.Factory<?, T> factory = (BasicJoiner.Factory<?, T>) QuickConstructorGenerator
            .getInstance(clazz, BasicJoiner.Factory.class);

        return new BasicJoiner(factory, targetStorage, targetQuery);
    }

    private static <T extends Storable> Class<Cursor<T>>
        generateBasicJoinerCursor(Class<?> sourceType, StorableProperty<T> targetToSourceProperty)
    {
        final int propCount = targetToSourceProperty.getJoinElementCount();

        // Determine if join is one-to-one, in which case slightly more optimal
        // code can be generated.
        boolean isOneToOne = true;
        for (int i=0; i<propCount; i++) {
            if (!targetToSourceProperty.getInternalJoinElement(i).isPrimaryKeyMember()) {
                isOneToOne = false;
                break;
            }
            if (!targetToSourceProperty.getExternalJoinElement(i).isPrimaryKeyMember()) {
                isOneToOne = false;
                break;
            }
        }

        Class<T> targetType = targetToSourceProperty.getEnclosingType();

        String packageName;
        {
            String name = targetType.getName();
            int index = name.lastIndexOf('.');
            if (index >= 0) {
                packageName = name.substring(0, index);
            } else {
                packageName = "";
            }
        }

        ClassLoader loader = targetType.getClassLoader();

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
            // Field to hold query which fetches type T.
            cf.addField(Modifiers.PRIVATE.toFinal(true), QUERY_FIELD_NAME, queryType);
        }

        boolean canSetSourceReference = targetToSourceProperty.getWriteMethod() != null;

        if (canSetSourceReference && !isOneToOne) {
            // Field to hold active S storable.
            cf.addField(Modifiers.PRIVATE, ACTIVE_SOURCE_FIELD_NAME,
                        TypeDesc.forClass(sourceType));
        }

        // Constructor accepts a Storage and Query, but Storage is only used
        // for one-to-one, and Query is only used for one-to-many.
        {
            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC,
                                              new TypeDesc[] {cursorType, storageType, queryType});
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadLocal(b.getParameter(0)); // pass S cursor to superclass
            b.invokeSuperConstructor(new TypeDesc[] {cursorType});

            if (isOneToOne) {
                b.loadThis();
                b.loadLocal(b.getParameter(1)); // push T storage to stack
                b.storeField(STORAGE_FIELD_NAME, storageType);
            } else {
                b.loadThis();
                b.loadLocal(b.getParameter(2)); // push T query to stack
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
                queryBuilder.append(targetToSourceProperty.getInternalJoinElement(i).getName());
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

            LocalVariable sourceVar = b.createLocalVariable(null, storableType);
            b.loadLocal(b.getParameter(0));
            b.checkCast(TypeDesc.forClass(sourceType));
            b.storeLocal(sourceVar);

            // Prepare T storable.
            b.loadThis();
            b.loadField(STORAGE_FIELD_NAME, storageType);
            b.invokeInterface(storageType, PREPARE_METHOD_NAME, storableType, null);
            LocalVariable targetVar = b.createLocalVariable(null, storableType);
            b.checkCast(TypeDesc.forClass(targetType));
            b.storeLocal(targetVar);

            // Copy pk property values from S to T.
            for (int i=0; i<propCount; i++) {
                StorableProperty<T> internal = targetToSourceProperty.getInternalJoinElement(i);
                StorableProperty<?> external = targetToSourceProperty.getExternalJoinElement(i);

                b.loadLocal(targetVar);
                b.loadLocal(sourceVar);
                b.invoke(external.getReadMethod());
                b.invoke(internal.getWriteMethod());
            }

            // tryLoad target.
            b.loadLocal(targetVar);
            b.invokeInterface(storableType, TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);
            Label wasLoaded = b.createLabel();
            b.ifZeroComparisonBranch(wasLoaded, "!=");

            b.loadNull();
            b.returnValue(storableType);

            wasLoaded.setLocation();

            if (canSetSourceReference) {
                b.loadLocal(targetVar);
                b.loadLocal(sourceVar);
                b.invoke(targetToSourceProperty.getWriteMethod());
            }

            b.loadLocal(targetVar);
            b.returnValue(storableType);
        } else {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED, "transform", cursorType,
                                         new TypeDesc[] {TypeDesc.OBJECT});
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable sourceVar = b.createLocalVariable(null, storableType);
            b.loadLocal(b.getParameter(0));
            b.checkCast(TypeDesc.forClass(sourceType));
            b.storeLocal(sourceVar);

            if (canSetSourceReference) {
                b.loadThis();
                b.loadLocal(sourceVar);
                b.storeField(ACTIVE_SOURCE_FIELD_NAME, TypeDesc.forClass(sourceType));
            }

            // Populate query parameters.
            b.loadThis();
            b.loadField(QUERY_FIELD_NAME, queryType);

            for (int i=0; i<propCount; i++) {
                StorableProperty<?> external = targetToSourceProperty.getExternalJoinElement(i);
                b.loadLocal(sourceVar);
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

        if (canSetSourceReference && !isOneToOne) {
            // Override the "next" method to set S object on T.
            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, "next", TypeDesc.OBJECT, null);
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.invokeSuper(TypeDesc.forClass(MultiTransformedCursor.class),
                          "next", TypeDesc.OBJECT, null);
            b.checkCast(TypeDesc.forClass(targetType));
            b.dup();

            b.loadThis();
            b.loadField(ACTIVE_SOURCE_FIELD_NAME, TypeDesc.forClass(sourceType));
            b.invoke(targetToSourceProperty.getWriteMethod());

            b.returnValue(storableType);
        }

        return (Class<Cursor<T>>) ci.defineClass(cf);
    }

    private final Joiner<S, T> mJoiner;

    /**
     * @param repo access to storage instances for properties
     * @param targetType type of <i>target</i> instances
     * @param targetToSourceProperty property of <i>target</i> type which maps
     * to instances of <i>source</i> type.
     * @param sourceType type of <i>source</i> instances
     * @throws IllegalArgumentException if property type is not <i>source</i>
     */
    public JoinedCursorFactory(Repository repo,
                               Class<T> targetType,
                               String targetToSourceProperty,
                               Class<S> sourceType)
        throws SupportException, FetchException, RepositoryException
    {
        this(repo,
             ChainedProperty.parse(StorableIntrospector.examine(targetType),
                                   targetToSourceProperty),
             sourceType);
    }

    /**
     * @param repo access to storage instances for properties
     * @param targetToSourceProperty property of <i>target</i> type which maps
     * to instances of <i>source</i> type.
     * @param sourceType type of <i>source</i> instances
     * @throws IllegalArgumentException if property type is not <i>source</i>
     */
    public JoinedCursorFactory(Repository repo,
                               ChainedProperty<T> targetToSourceProperty,
                               Class<S> sourceType)
        throws SupportException, FetchException, RepositoryException
    {
        if (targetToSourceProperty.getType() != sourceType) {
            throw new IllegalArgumentException
                ("Property is not of type \"" + sourceType.getName() + "\": " +
                 targetToSourceProperty);
        }

        StorableProperty<T> primeTarget = targetToSourceProperty.getPrimeProperty();
        Storage<T> primeTargetStorage = repo.storageFor(primeTarget.getEnclosingType());

        Joiner joiner = newBasicJoiner(primeTarget, primeTargetStorage);

        int chainCount = targetToSourceProperty.getChainCount();
        for (int i=0; i<chainCount; i++) {
            StorableProperty prop = targetToSourceProperty.getChainedProperty(i);
            Storage storage = repo.storageFor(prop.getEnclosingType());

            joiner = new MultiJoiner(newBasicJoiner(prop, storage), joiner);
        }

        mJoiner = (Joiner<S, T>) joiner;
    }

    /**
     * Given a cursor over type <i>source</i>, returns a new cursor over joined
     * property of type <i>target</i>.
     */
    public Cursor<T> join(Cursor<S> cursor) {
        return mJoiner.join(cursor);
    }

    private static interface Joiner<S, T extends Storable> {
        Cursor<T> join(Cursor<S> cursor);
    }

    /**
     * Support for joins without an intermediate hop.
     */
    private static class BasicJoiner<S, T extends Storable> implements Joiner<S, T> {
        private final Factory<S, T> mJoinerFactory;
        private final Storage<T> mTargetStorage;
        private final Query<T> mTargetQuery;

        BasicJoiner(Factory<S, T> factory, Storage<T> targetStorage, Query<T> targetQuery) {
            mJoinerFactory = factory;
            mTargetStorage = targetStorage;
            mTargetQuery = targetQuery;
        }

        public Cursor<T> join(Cursor<S> cursor) {
            return mJoinerFactory.newJoinedCursor(cursor, mTargetStorage, mTargetQuery);
        }

        /**
         * Needs to be public for {@link QuickConstructorGenerator}.
         */
        public static interface Factory<S, T extends Storable> {
            Cursor<T> newJoinedCursor(Cursor<S> cursor,
                                      Storage<T> targetStorage, Query<T> targetQuery);
        }
    }

    /**
     * Support for joins with an intermediate hop -- multi-way joins.
     */
    private static class MultiJoiner<S, X extends Storable, T extends Storable>
        implements Joiner<S, T>
    {
        private final Joiner<S, X> mSourceToMid;
        private final Joiner<X, T> mMidToTarget;

        MultiJoiner(Joiner<S, X> sourceToMidJoiner, Joiner<X, T> midToTargetJoiner) {
            mSourceToMid = sourceToMidJoiner;
            mMidToTarget = midToTargetJoiner;
        }

        public Cursor<T> join(Cursor<S> cursor) {
            return mMidToTarget.join(mSourceToMid.join(cursor));
        }
    }
}
