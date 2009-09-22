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

package com.amazon.carbonado.gen;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.math.BigInteger;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodDesc;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.Opcode;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.lob.Lob;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableKey;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;
import com.amazon.carbonado.info.StorablePropertyAnnotation;
import com.amazon.carbonado.info.StorablePropertyConstraint;

import com.amazon.carbonado.raw.DataDecoder;
import com.amazon.carbonado.raw.DataEncoder;
import com.amazon.carbonado.raw.GenericEncodingStrategy;

import static com.amazon.carbonado.gen.CommonMethodNames.*;

/**
 * Generates and caches abstract implementations of {@link Storable}
 * types. This greatly simplifies the process of defining new kinds of {@link
 * Repository Repositories}, since most of the mundane code generation is taken
 * care of.
 *
 * @author Brian S O'Neill
 * @author Don Schneider
 * @author Tobias Holgers
 * @see MasterStorableGenerator
 * @see DelegateStorableGenerator
 * @since 1.2
 */
public final class StorableGenerator<S extends Storable> {

    // Note: All generated fields/methods have a "$" character in them to
    // prevent name collisions with any inherited fields/methods. User storable
    // properties are defined as fields which exactly match the property
    // name. We don't want collisions with those either. Legal bean properties
    // cannot have "$" in them, so there's nothing to worry about.

    /** Name of protected abstract method in generated storable */
    public static final String
        DO_TRY_LOAD_METHOD_NAME   = "doTryLoad$",
        DO_TRY_INSERT_METHOD_NAME = "doTryInsert$",
        DO_TRY_UPDATE_METHOD_NAME = "doTryUpdate$",
        DO_TRY_DELETE_METHOD_NAME = "doTryDelete$";

    /**
     * Name of protected method in generated storable which checks that
     * primary keys are initialized, throwing an exception otherwise.
     */
    public static final String
        CHECK_PK_FOR_INSERT_METHOD_NAME = "checkPkForInsert$",
        CHECK_PK_FOR_UPDATE_METHOD_NAME = "checkPkForUpdate$",
        CHECK_PK_FOR_DELETE_METHOD_NAME = "checkPkForDelete$";

    /**
     * Name of protected method in generated storable that returns false if any
     * primary keys are uninitialized.
     */
    public static final String IS_PK_INITIALIZED_METHOD_NAME = "isPkInitialized$";

    /**
     * Name prefix of protected method in generated storable that returns false
     * if a specific alternate key is uninitialized. The complete name is
     * formed by the prefix appended with the zero-based alternate key number.
     */
    public static final String IS_ALT_KEY_INITIALIZED_PREFIX = "isAltKeyInitialized$";

    /**
     * Name of protected method in generated storable that returns false if any
     * non-nullable, non-pk properties are uninitialized.
     */
    public static final String IS_REQUIRED_DATA_INITIALIZED_METHOD_NAME =
        "isRequiredDataInitialized$";

    /**
     * Name of protected method in generated storable that returns false if
     * version property is uninitialized. If no version property exists, then
     * this method is not defined.
     */
    public static final String IS_VERSION_INITIALIZED_METHOD_NAME = "isVersionInitialized$";

    /**
     * Name of protected method which must be called after load to identify all
     * properties as valid and to fire any load triggers.
     *
     * @since 1.2
     */
    public static final String LOAD_COMPLETED_METHOD_NAME = "loadCompleted$";

    /**
     * Prefix of protected field in generated storable that holds property
     * states. Each property consumes two bits to hold its state, and so each
     * 32-bit field holds states for up to 16 properties.
     */
    public static final String PROPERTY_STATE_FIELD_NAME = "propertyState$";

    /** Adapter field names are propertyName + "$adapter$" + number */
    public static final String ADAPTER_FIELD_ELEMENT = "$adapter$";

    /** Constraint field names are propertyName + "$constraint$" + number */
    public static final String CONSTRAINT_FIELD_ELEMENT = "$constraint$";

    /** Reference to TriggerSupport instance */
    public static final String SUPPORT_FIELD_NAME = "support$";

    /** Property state indicating that property has never been set, loaded, or saved */
    public static final int PROPERTY_STATE_UNINITIALIZED = 0;
    /** Property state indicating that property has been set, but not saved */
    public static final int PROPERTY_STATE_DIRTY = 3;
    /** Property state indicating that property value reflects a clean value */
    public static final int PROPERTY_STATE_CLEAN = 1;
    /** Property state mask is 3, to cover the two bits used by a property state */
    public static final int PROPERTY_STATE_MASK = 3;

    // Private method which returns a property's state.
    private static final String PROPERTY_STATE_EXTRACT_METHOD_NAME = "extractState$";

    private static final String PRIVATE_INSERT_METHOD_NAME = "insert$";
    private static final String PRIVATE_UPDATE_METHOD_NAME = "update$";
    private static final String PRIVATE_DELETE_METHOD_NAME = "delete$";

    // Cache of generated abstract classes.
    private static Map<Class, Reference<Class<? extends Storable>>> cAbstractCache;

    static {
        cAbstractCache = new WeakIdentityMap();
    }

    // When true, calls to get uninitialized properties throw an
    // IllegalStateException. By default it is false, but the default will
    // change to true when query projection is supported.
    private static final boolean cStrictAccess;

    static {
        String value = System.getProperty(StorableGenerator.class.getName() + ".strictAccess");
        cStrictAccess = value != null && value.equals("true");
    }

    // There are three flavors of equals methods, used by addEqualsMethod.
    private static final int EQUAL_KEYS = 0;
    private static final int EQUAL_PROPERTIES = 1;
    private static final int EQUAL_FULL = 2;

    private static final String UNCAUGHT_METHOD_NAME = "uncaught$";

    private static final String INSERT_OP = "Insert";
    private static final String UPDATE_OP = "Update";
    private static final String DELETE_OP = "Delete";

    // Different uses for generated property switch statements.
    private static final int SWITCH_FOR_STATE = 1, SWITCH_FOR_GET = 2, SWITCH_FOR_SET = 3;

    /**
     * Returns an abstract implementation of the given Storable type, which is
     * fully thread-safe. The Storable type itself may be an interface or a
     * class. If it is a class, then it must not be final, and it must have a
     * public, no-arg constructor. The constructor signature for the returned
     * abstract class is defined as follows:
     *
     * <pre>
     * /**
     *  * @param support  Access to triggers
     *  *&#047;
     * public &lt;init&gt;(TriggerSupport support);
     * </pre>
     *
     * <p>Subclasses must implement the following abstract protected methods,
     * whose exact names are defined by constants in this class:
     *
     * <pre>
     * // Load the object by examining the primary key.
     * protected abstract boolean doTryLoad() throws FetchException;
     *
     * // Insert the object into the storage layer.
     * protected abstract boolean doTryInsert() throws PersistException;
     *
     * // Update the object in the storage.
     * protected abstract boolean doTryUpdate() throws PersistException;
     *
     * // Delete the object from the storage layer by the primary key.
     * protected abstract boolean doTryDelete() throws PersistException;
     * </pre>
     *
     * A set of protected hook methods are provided which ensure that all
     * primary keys are initialized before performing a repository
     * operation. Subclasses may override them, if they are capable of filling
     * in unspecified primary keys. One such example is applying a sequence on
     * insert.
     *
     * <pre>
     * // Throws exception if any primary keys are uninitialized.
     * // Actual method name defined by CHECK_PK_FOR_INSERT_METHOD_NAME.
     * protected void checkPkForInsert() throws IllegalStateException;
     *
     * // Throws exception if any primary keys are uninitialized.
     * // Actual method name defined by CHECK_PK_FOR_UPDATE_METHOD_NAME.
     * protected void checkPkForUpdate() throws IllegalStateException;
     *
     * // Throws exception if any primary keys are uninitialized.
     * // Actual method name defined by CHECK_PK_FOR_DELETE_METHOD_NAME.
     * protected void checkPkForDelete() throws IllegalStateException;
     * </pre>
     *
     * Each property value is defined as a protected field whose name and type
     * matches the property. Subclasses should access these fields directly
     * during loading and storing. For loading, it bypasses constraint
     * checks. For both, it provides better performance.
     *
     * <p>Subclasses also have access to a set of property state bits stored
     * in protected int fields. Subclasses are not responsible for updating
     * these values. The intention is that these states may be used by
     * subclasses to support partial updates. They may otherwise be ignored.
     *
     * <p>As a convenience, protected methods are provided to test and alter
     * the property state bits. Subclass constructors that fill all properties
     * with loaded values must call loadCompleted to ensure all properties are
     * identified as being valid and to fire any load triggers.
     *
     * <pre>
     * // Returns true if all primary key properties have been set.
     * protected boolean isPkInitialized();
     *
     * // Returns true if all required data properties are set.
     * // A required data property is a non-nullable, non-primary key.
     * protected boolean isRequiredDataInitialized();
     *
     * // Returns true if a version property has been set.
     * // Note: This method is not generated if there is no version property.
     * protected boolean isVersionInitialized();
     *
     * // Must be called after load to identify all properties as valid
     * // and to fire any load triggers.
     * // Actual method name defined by LOAD_COMPLETED_METHOD_NAME.
     * protected void loadCompleted() throws FetchException;
     * </pre>
     *
     * Property state field names are defined by the concatenation of
     * {@code PROPERTY_STATE_FIELD_NAME} and a zero-based decimal
     * number. To determine which field holds a particular property's state,
     * the field number is computed as the property number divided by 16. The
     * specific two-bit state position is the remainder of this division times 2.
     *
     * @throws com.amazon.carbonado.MalformedTypeException if Storable type is not well-formed
     * @throws IllegalArgumentException if type is null
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> Class<? extends S> getAbstractClass(Class<S> type)
        throws IllegalArgumentException
    {
        synchronized (cAbstractCache) {
            Class<? extends S> abstractClass;
            Reference<Class<? extends Storable>> ref = cAbstractCache.get(type);
            if (ref != null) {
                abstractClass = (Class<? extends S>) ref.get();
                if (abstractClass != null) {
                    return abstractClass;
                }
            }
            abstractClass = new StorableGenerator<S>(type).generateAndInjectClass();
            cAbstractCache.put(type, new SoftReference<Class<? extends Storable>>(abstractClass));
            return abstractClass;
        }
    }

    private final Class<S> mStorableType;
    private final TypeDesc mSupportType;
    private final StorableInfo<S> mInfo;
    private final Map<String, ? extends StorableProperty<S>> mAllProperties;

    private final ClassInjector mClassInjector;
    private final ClassFile mClassFile;

    private StorableGenerator(Class<S> storableType) {
        mStorableType = storableType;
        mSupportType = TypeDesc.forClass(TriggerSupport.class);
        mInfo = StorableIntrospector.examine(storableType);
        mAllProperties = mInfo.getAllProperties();

        mClassInjector = ClassInjector.create
            (storableType.getName(), storableType.getClassLoader());
        mClassFile = CodeBuilderUtil.createStorableClassFile
            (mClassInjector, storableType, true,
             StorableGenerator.class.getName());
    }

    private Class<? extends S> generateAndInjectClass() {
        generateClass();
        Class abstractClass = mClassInjector.defineClass(mClassFile);
        return abstractClass;
    }

    private void generateClass() {
        // Use this static method for passing uncaught exceptions.
        defineUncaughtExceptionHandler();

        // private final TriggerSupport support;
        mClassFile.addField(Modifiers.PROTECTED.toFinal(true),
                            SUPPORT_FIELD_NAME,
                            mSupportType);

        {
            // Add protected constructor.
            TypeDesc[] params = {mSupportType};

            final int supportParam = 0;
            MethodInfo mi = mClassFile.addConstructor(Modifiers.PROTECTED, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.invokeSuperConstructor(null);

            //// this.support = support
            b.loadThis();
            b.loadLocal(b.getParameter(supportParam));
            b.storeField(SUPPORT_FIELD_NAME, mSupportType);

            b.returnVoid();
        }

        // Add static fields for adapters and constraints, and create static
        // initializer to populate fields.
        {
            // CodeBuilder for static initializer, defined only if there's
            // something to put in it.
            CodeBuilder clinit = null;

            // Adapter and constraint fields are protected static.
            final Modifiers fieldModifiers = Modifiers.PROTECTED.toStatic(true).toFinal(true);

            // Add adapter field.
            for (StorableProperty property : mAllProperties.values()) {
                StorablePropertyAdapter spa = property.getAdapter();
                if (spa == null) {
                    continue;
                }

                String fieldName = property.getName() + ADAPTER_FIELD_ELEMENT + 0;
                TypeDesc adapterType = TypeDesc.forClass
                    (spa.getAdapterConstructor().getDeclaringClass());
                mClassFile.addField(fieldModifiers, fieldName, adapterType);

                if (clinit == null) {
                    clinit = new CodeBuilder(mClassFile.addInitializer());
                }

                // Assign value to new field.
                // admin$adapter$0 = new YesNoAdapter.Adapter
                //     (UserInfo.class, "admin", annotation);

                clinit.newObject(adapterType);
                clinit.dup();
                clinit.loadConstant(TypeDesc.forClass(mStorableType));
                clinit.loadConstant(property.getName());

                // Generate code to load property annotation third parameter.
                loadPropertyAnnotation(clinit, property, spa.getAnnotation());

                clinit.invoke(spa.getAdapterConstructor());
                clinit.storeStaticField(fieldName, adapterType);
            }

            // Add contraint fields.
            for (StorableProperty property : mAllProperties.values()) {
                int count = property.getConstraintCount();
                for (int i=0; i<count; i++) {
                    StorablePropertyConstraint spc = property.getConstraint(i);
                    String fieldName = property.getName() + CONSTRAINT_FIELD_ELEMENT + i;
                    TypeDesc constraintType = TypeDesc.forClass
                        (spc.getConstraintConstructor().getDeclaringClass());
                    mClassFile.addField(fieldModifiers, fieldName, constraintType);

                    if (clinit == null) {
                        clinit = new CodeBuilder(mClassFile.addInitializer());
                    }

                    // Assign value to new field.
                    // admin$constraint$0 = new LengthConstraint.Constraint
                    //     (UserInfo.class, "firstName", annotation);

                    clinit.newObject(constraintType);
                    clinit.dup();
                    clinit.loadConstant(TypeDesc.forClass(mStorableType));
                    clinit.loadConstant(property.getName());

                    // Generate code to load property annotation third parameter.
                    loadPropertyAnnotation(clinit, property, spc.getAnnotation());

                    clinit.invoke(spc.getConstraintConstructor());
                    clinit.storeStaticField(fieldName, constraintType);
                }
            }

            if (clinit != null) {
                // Must return else verifier complains.
                clinit.returnVoid();
            }
        }

        // Add property fields and methods.
        // Also remember ordinal of optional version property for use later.
        int versionOrdinal = -1;
        {
            int maxOrdinal = mAllProperties.size() - 1;
            boolean requireStateField = false;

            for (StorableProperty<S> property : mAllProperties.values()) {
                int ordinal = property.getNumber();

                if (!property.isDerived() && property.isVersion()) {
                    versionOrdinal = ordinal;
                }

                final String name = property.getName();
                final TypeDesc type = TypeDesc.forClass(property.getType());

                if (!property.isDerived()) {
                    if (property.isJoin()) {
                        // Mark as transient since join properties can be
                        // reconstructed from the other fields.
                        mClassFile.addField(Modifiers.PRIVATE.toTransient(true), name, type);
                        requireStateField = true;
                    } else {
                        // Double words are volatile to prevent word tearing
                        // without explicit synchronization. When strict access
                        // is enabled, all access methods are synchronized anyhow.
                        boolean isVolatile = (!cStrictAccess) && type.isDoubleWord();
                        mClassFile.addField(Modifiers.PROTECTED.toVolatile(isVolatile),
                                            name, type);
                        requireStateField = true;
                    }
                }

                final String stateFieldName = PROPERTY_STATE_FIELD_NAME + (ordinal >> 4);
                if (ordinal == maxOrdinal || ((ordinal & 0xf) == 0xf)) {
                    if (requireStateField) {
                        mClassFile.addField
                            (Modifiers.PROTECTED, stateFieldName, TypeDesc.INT);
                    }
                    requireStateField = false;
                }

                // Add read method.
                buildReadMethod: if (!property.isDerived()) {
                    Method readMethod = property.getReadMethod();

                    MethodInfo mi;
                    if (readMethod != null) {
                        mi = mClassFile.addMethod(readMethod);
                    } else {
                        // Add a synthetic protected read method.
                        String readName = property.getReadMethodName();
                        mi = mClassFile.addMethod(Modifiers.PROTECTED, readName, type, null);
                        mi.markSynthetic();
                        if (property.isJoin()) {
                            mi.addException(TypeDesc.forClass(FetchException.class));
                        }
                    }

                    if (cStrictAccess || property.isJoin()) {
                        // Synchronization is required for join property
                        // accessors, as they may alter bit masks. Synchronization is
                        // also required when strict access is enabled.
                        mi.setModifiers(mi.getModifiers().toSynchronized(true));
                    }

                    // Now add code that actually gets the property value.
                    CodeBuilder b = new CodeBuilder(mi);

                    if (!property.isJoin()) {
                        if (cStrictAccess) {
                            // Make sure property state allows access.
                            b.loadThis();
                            b.loadField(stateFieldName, TypeDesc.INT);
                            b.loadConstant(PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2));
                            b.math(Opcode.IAND);
                            Label isValid = b.createLabel();
                            b.ifZeroComparisonBranch(isValid, "!=");
                            CodeBuilderUtil.throwConcatException
                                (b, IllegalStateException.class,
                                 "Cannot access uninitialized property: ", name);
                            isValid.setLocation();
                        }
                    } else {
                        // Join properties support on-demand loading.

                        // Check if property has been loaded.
                        b.loadThis();
                        b.loadField(stateFieldName, TypeDesc.INT);
                        b.loadConstant(PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2));
                        b.math(Opcode.IAND);
                        Label isLoaded = b.createLabel();
                        b.ifZeroComparisonBranch(isLoaded, "!=");

                        // Store loaded join result here.
                        LocalVariable join = b.createLocalVariable(name, type);

                        // Check if any internal properties are nullable, but
                        // the matching external property is not. If so, load
                        // each of these special internal values and check if
                        // null. If null, short-circuit the load and use null
                        // as the join result.

                        Label shortCircuit = b.createLabel();
                        buildShortCircuit: {
                            int count = property.getJoinElementCount();
                            nullPossible: {
                                for (int i=0; i<count; i++) {
                                    StorableProperty internal = property.getInternalJoinElement(i);
                                    StorableProperty external = property.getExternalJoinElement(i);
                                    if (internal.isNullable() && !external.isNullable()) {
                                        break nullPossible;
                                    }
                                }
                                break buildShortCircuit;
                            }

                            for (int i=0; i<count; i++) {
                                StorableProperty internal = property.getInternalJoinElement(i);
                                StorableProperty external = property.getExternalJoinElement(i);
                                if (internal.isNullable() && !external.isNullable()) {
                                    loadThisProperty(b, internal);
                                    Label notNull = b.createLabel();
                                    b.ifNullBranch(notNull, false);
                                    b.loadNull();
                                    b.storeLocal(join);
                                    b.branch(shortCircuit);
                                    notNull.setLocation();
                                }
                            }
                        }

                        // Get the storage for the join type.
                        loadStorageForFetch(b, TypeDesc.forClass(property.getJoinedType()));
                        TypeDesc storageType = TypeDesc.forClass(Storage.class);

                        // There are two ways that property can be loaded. The
                        // general form is to use a Query. Calling load on the
                        // property itself is preferred, but it is only
                        // possible if the join is against a key and all
                        // external properties have a write method.

                        boolean canUseDirectForm = !property.isQuery();

                        if (canUseDirectForm) {
                            int joinCount = property.getJoinElementCount();
                            for (int i=0; i<joinCount; i++) {
                                StorableProperty external = property.getExternalJoinElement(i);
                                if (external.getWriteMethod() == null) {
                                    canUseDirectForm = false;
                                }
                            }
                        }

                        final TypeDesc storableDesc = TypeDesc.forClass(Storable.class);

                        if (canUseDirectForm) {
                            // Generate direct load form.

                            // Storage instance is already on the stack... replace it
                            // with an instance of the joined type.
                            b.invokeInterface
                                (storageType, PREPARE_METHOD_NAME, storableDesc, null);
                            b.checkCast(type);
                            b.storeLocal(join);

                            // Set the keys on the joined type.
                            int count = property.getJoinElementCount();
                            for (int i=0; i<count; i++) {
                                b.loadLocal(join);
                                StorableProperty internal = property.getInternalJoinElement(i);
                                StorableProperty external = property.getExternalJoinElement(i);
                                loadThisProperty(b, internal);
                                CodeBuilderUtil.convertValue
                                    (b, internal.getType(), external.getType());
                                b.invoke(external.getWriteMethod());
                            }

                            // Now load the object.
                            b.loadLocal(join);
                            // Always call "try load", even for non-nullable joins.
                            b.invokeInterface
                                (storableDesc, TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);
                            Label wasLoaded = b.createLabel();
                            b.ifZeroComparisonBranch(wasLoaded, "!=");
                            // Not loaded, so replace joined object with null.
                            b.loadNull();
                            b.storeLocal(join);
                            wasLoaded.setLocation();
                        } else {
                            // Generate query load form.

                            // Storage instance is already on the stack... replace it
                            // with a Query. First, we need to define the query string.

                            StringBuilder queryBuilder = new StringBuilder();

                            // Set the keys on the joined type.
                            int count = property.getJoinElementCount();
                            for (int i=0; i<count; i++) {
                                if (i > 0) {
                                    queryBuilder.append(" & ");
                                }
                                queryBuilder.append(property.getExternalJoinElement(i).getName());
                                queryBuilder.append(" = ?");
                            }

                            b.loadConstant(queryBuilder.toString());
                            TypeDesc queryType = TypeDesc.forClass(Query.class);
                            b.invokeInterface(storageType, QUERY_METHOD_NAME, queryType,
                                              new TypeDesc[]{TypeDesc.STRING});

                            // Now fill in the parameters of the query.
                            for (int i=0; i<count; i++) {
                                StorableProperty<S> internal = property.getInternalJoinElement(i);
                                loadThisProperty(b, internal);
                                TypeDesc bindType =
                                    CodeBuilderUtil.bindQueryParam(internal.getType());
                                CodeBuilderUtil.convertValue
                                    (b, internal.getType(), bindType.toClass());
                                b.invokeInterface(queryType, WITH_METHOD_NAME, queryType,
                                                  new TypeDesc[]{bindType});
                            }

                            // Now run the query.
                            if (property.isQuery()) {
                                // Just save and return the query.
                                b.storeLocal(join);
                            } else {
                                // Always call "try load", even for non-nullable joins.
                                b.invokeInterface
                                    (queryType, TRY_LOAD_ONE_METHOD_NAME, storableDesc, null);
                                b.checkCast(type);
                                b.storeLocal(join);
                            }
                        }

                        // Store loaded property.
                        shortCircuit.setLocation();
                        b.loadThis();
                        b.loadLocal(join);
                        b.storeField(property.getName(), type);

                        // Add code to identify this property as being loaded,
                        // except if value is null and join is not nullable.

                        if (!property.isNullable()) {
                            b.loadLocal(join);
                            b.ifNullBranch(isLoaded, true);
                        }

                        b.loadThis();
                        b.loadThis();
                        b.loadField(stateFieldName, TypeDesc.INT);
                        b.loadConstant(PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2));
                        b.math(Opcode.IOR);
                        b.storeField(stateFieldName, TypeDesc.INT);

                        isLoaded.setLocation();
                    }

                    // Load property value and return it.

                    loadThisProperty(b, property);
                    b.returnValue(type);
                }

                // Add write method.
                buildWriteMethod: if (!property.isDerived() && !property.isQuery()) {
                    Method writeMethod = property.getWriteMethod();

                    MethodInfo mi;
                    if (writeMethod != null) {
                        mi = mClassFile.addMethod(writeMethod);
                    } else {
                        // Add a synthetic protected write method.
                        String writeName = property.getWriteMethodName();
                        mi = mClassFile.addMethod(Modifiers.PROTECTED, writeName, null,
                                                  new TypeDesc[]{type});
                        mi.markSynthetic();
                    }

                    mi.setModifiers(mi.getModifiers().toSynchronized(true));
                    CodeBuilder b = new CodeBuilder(mi);

                    // Primary keys cannot be altered if state is "clean".
                    if (property.isPrimaryKeyMember()) {
                        b.loadThis();
                        b.loadField(stateFieldName, TypeDesc.INT);
                        b.loadConstant(PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2));
                        b.math(Opcode.IAND);
                        b.loadConstant(PROPERTY_STATE_CLEAN << ((ordinal & 0xf) * 2));
                        Label isMutable = b.createLabel();
                        b.ifComparisonBranch(isMutable, "!=");
                        CodeBuilderUtil.throwException
                            (b, IllegalStateException.class, "Cannot alter primary key");
                        isMutable.setLocation();
                    }

                    int spcCount = property.getConstraintCount();

                    boolean nullNotAllowed =
                        !property.getType().isPrimitive() &&
                        !property.isJoin() && !property.isNullable();

                    if (nullNotAllowed || spcCount > 0) {
                        // Add constraint checks.
                        Label skipConstraints = b.createLabel();

                        if (nullNotAllowed) {
                            // Don't allow null value to be set.
                            b.loadLocal(b.getParameter(0));
                            Label notNull = b.createLabel();
                            b.ifNullBranch(notNull, false);
                            CodeBuilderUtil.throwConcatException
                                (b, IllegalArgumentException.class,
                                 "Cannot set property \"", property.getName(), "\" to null");
                            notNull.setLocation();
                        } else {
                            // Don't invoke constraints if value is null.
                            if (!property.getType().isPrimitive()) {
                                b.loadLocal(b.getParameter(0));
                                b.ifNullBranch(skipConstraints, true);
                            }
                        }

                        // Add code to invoke constraints.

                        for (int spcIndex = 0; spcIndex < spcCount; spcIndex++) {
                            StorablePropertyConstraint spc = property.getConstraint(spcIndex);
                            String fieldName =
                                property.getName() + CONSTRAINT_FIELD_ELEMENT + spcIndex;
                            TypeDesc constraintType = TypeDesc.forClass
                                (spc.getConstraintConstructor().getDeclaringClass());
                            b.loadStaticField(fieldName, constraintType);
                            b.loadLocal(b.getParameter(0));
                            b.convert
                                (b.getParameter(0).getType(), TypeDesc.forClass
                                 (spc.getConstrainMethod().getParameterTypes()[0]));
                            b.invoke(spc.getConstrainMethod());
                        }

                        skipConstraints.setLocation();
                    }

                    Label setValue = b.createLabel();

                    if (!property.isJoin() || Lob.class.isAssignableFrom(property.getType())) {
                        Label markDirty = b.createLabel();

                        if (Lob.class.isAssignableFrom(property.getType())) {
                            // Contrary to how standard properties are managed,
                            // only mark dirty if value changed. Exception is made
                            // for null -- always mark dirty. This allows LOB property
                            // to be updated to null without having to load it.
                            b.loadLocal(b.getParameter(0));
                            b.ifNullBranch(markDirty, true);

                            loadThisProperty(b, property);
                            LocalVariable tempProp = b.createLocalVariable(null, type);
                            b.storeLocal(tempProp);
                            b.loadLocal(tempProp);
                            b.ifNullBranch(markDirty, true);

                            b.loadLocal(tempProp);
                            b.loadLocal(b.getParameter(0));
                            CodeBuilderUtil.addValuesEqualCall(b, type, false, setValue, true);
                        }

                        markDirty.setLocation();
                        markOrdinaryPropertyDirty(b, property);
                    } else {
                        b.loadLocal(b.getParameter(0));
                        if (property.isNullable()) {
                            // Don't attempt to extract internal properties from null.
                            b.ifNullBranch(setValue, true);
                        } else {
                            Label notNull = b.createLabel();
                            b.ifNullBranch(notNull, false);
                            CodeBuilderUtil.throwConcatException
                                (b, IllegalArgumentException.class,
                                 "Non-nullable join property \"", property.getName(),
                                 "\" cannot be set to null");
                            notNull.setLocation();
                        }

                        // Copy internal properties from joined object.
                        int count = property.getJoinElementCount();
                        for (int i=0; i<count; i++) {
                            StorableProperty internal = property.getInternalJoinElement(i);
                            if (internal.getWriteMethod() == null) {
                                continue;
                            }
                            StorableProperty external = property.getExternalJoinElement(i);

                            b.loadLocal(b.getParameter(0));
                            b.invoke(external.getReadMethod());
                            CodeBuilderUtil.convertValue
                                (b, external.getType(), internal.getType());

                            LocalVariable newInternalPropVar =
                                b.createLocalVariable(null, TypeDesc.forClass(internal.getType()));
                            b.storeLocal(newInternalPropVar);

                            // Since join properties may be pre-loaded, they
                            // are set via the public write method. If internal
                            // property is clean and equal to new value, then
                            // don't set internal property. Doing so would mark
                            // it as dirty, which is not the right behavior
                            // when pre-loading join properties. The internal
                            // properties should remain clean.

                            Label setInternalProp = b.createLabel();

                            // Access state of internal property directly.
                            int ord = internal.getNumber();
                            b.loadThis();
                            b.loadField(PROPERTY_STATE_FIELD_NAME + (ord >> 4), TypeDesc.INT);
                            b.loadConstant(PROPERTY_STATE_MASK << ((ord & 0xf) * 2));
                            b.math(Opcode.IAND);
                            b.loadConstant(PROPERTY_STATE_CLEAN << ((ord & 0xf) * 2));
                            // If not clean, skip equal check.
                            b.ifComparisonBranch(setInternalProp, "!=");

                            // If new internal property value is equal to
                            // existing value, skip setting it.
                            b.loadThis();
                            b.invoke(internal.getReadMethod());
                            b.loadLocal(newInternalPropVar);
                            Label skipSetInternalProp = b.createLabel();
                            CodeBuilderUtil.addValuesEqualCall
                                (b, TypeDesc.forClass(internal.getType()),
                                 true, skipSetInternalProp, true);

                            setInternalProp.setLocation();

                            // Call set method to ensure that state bits are
                            // properly adjusted.
                            b.loadThis();
                            b.loadLocal(newInternalPropVar);
                            b.invoke(internal.getWriteMethod());

                            skipSetInternalProp.setLocation();
                        }

                        // Add code to identify this property as being loaded.
                        b.loadThis();
                        b.loadThis();
                        b.loadField(stateFieldName, TypeDesc.INT);
                        b.loadConstant(PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2));
                        b.math(Opcode.IOR);
                        b.storeField(stateFieldName, TypeDesc.INT);
                    }

                    // Now add code that actually sets the property value.

                    setValue.setLocation();

                    b.loadThis();
                    b.loadLocal(b.getParameter(0));
                    b.storeField(property.getName(), type);

                    b.returnVoid();
                }

                // Add optional protected adapted read methods.
                if (property.getAdapter() != null) {
                    // End name with '$' to prevent any possible collisions.
                    String readName = property.getReadMethodName() + '$';

                    StorablePropertyAdapter adapter = property.getAdapter();

                    for (Method adaptMethod : adapter.findAdaptMethodsFrom(type.toClass())) {
                        TypeDesc toType = TypeDesc.forClass(adaptMethod.getReturnType());
                        MethodInfo mi = mClassFile.addMethod
                            (Modifiers.PROTECTED, readName, toType, null);
                        mi.markSynthetic();

                        // Now add code that actually gets the property value and
                        // then invokes adapt method.
                        CodeBuilder b = new CodeBuilder(mi);

                        // Push adapter class to stack.
                        String fieldName = property.getName() + ADAPTER_FIELD_ELEMENT + 0;
                        TypeDesc adapterType = TypeDesc.forClass
                            (adapter.getAdapterConstructor().getDeclaringClass());
                        b.loadStaticField(fieldName, adapterType);

                        // Load property value.
                        loadThisProperty(b, property);

                        b.invoke(adaptMethod);
                        b.returnValue(toType);
                    }
                }

                // Add optional protected adapted write methods.

                // Note: Calling these methods does not affect any state bits.
                // They are only intended to be used by subclasses during loading.

                if (property.getAdapter() != null &&
                    (!property.isDerived() || (property.getWriteMethod() != null)))
                {
                    // End name with '$' to prevent any possible collisions.
                    String writeName = property.getWriteMethodName() + '$';

                    StorablePropertyAdapter adapter = property.getAdapter();

                    for (Method adaptMethod : adapter.findAdaptMethodsTo(type.toClass())) {
                        TypeDesc fromType = TypeDesc.forClass(adaptMethod.getParameterTypes()[0]);
                        MethodInfo mi = mClassFile.addMethod
                            (Modifiers.PROTECTED, writeName, null, new TypeDesc[] {fromType});
                        mi.markSynthetic();
                        mi.setModifiers(mi.getModifiers().toSynchronized(true));

                        // Now add code that actually adapts parameter and then
                        // stores the property value.
                        CodeBuilder b = new CodeBuilder(mi);

                        // Push this in preparation for storing a field.
                        b.loadThis();

                        // Push adapter class to stack.
                        String fieldName = property.getName() + ADAPTER_FIELD_ELEMENT + 0;
                        TypeDesc adapterType = TypeDesc.forClass
                            (adapter.getAdapterConstructor().getDeclaringClass());
                        b.loadStaticField(fieldName, adapterType);

                        b.loadLocal(b.getParameter(0));
                        b.invoke(adaptMethod);
                        storeProperty(b, property, type);

                        b.returnVoid();
                    }
                }

                addPropertyBridges(property);
            }
        }

        // Add tryLoad method which delegates to abstract doTryLoad method.
        addTryLoad: {
            // Define the tryLoad method.
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC.toSynchronized(true),
                 TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);

            if (mi == null) {
                break addTryLoad;
            }

            mi.addException(TypeDesc.forClass(FetchException.class));

            CodeBuilder b = new CodeBuilder(mi);

            // Check that primary key is initialized.
            b.loadThis();
            b.invokeVirtual(IS_PK_INITIALIZED_METHOD_NAME, TypeDesc.BOOLEAN, null);
            Label pkInitialized = b.createLabel();
            b.ifZeroComparisonBranch(pkInitialized, "!=");

            Label loaded = b.createLabel();
            Label notLoaded = b.createLabel();

            if (mInfo.getAlternateKeyCount() == 0) {
                CodeBuilderUtil.throwException(b, IllegalStateException.class,
                                               "Primary key not fully specified");
            } else {
                // If any alternate keys, check them too.

                // Load our Storage, in preparation for query against it.
                loadStorageForFetch(b, TypeDesc.forClass(mStorableType));

                Label runQuery = b.createLabel();
                TypeDesc queryType = TypeDesc.forClass(Query.class);

                for (int i=0; i<mInfo.getAlternateKeyCount(); i++) {
                    b.loadThis();
                    b.invokeVirtual(IS_ALT_KEY_INITIALIZED_PREFIX + i, TypeDesc.BOOLEAN, null);
                    Label noAltKey = b.createLabel();
                    b.ifZeroComparisonBranch(noAltKey, "==");

                    StorableKey<S> altKey = mInfo.getAlternateKey(i);

                    // Form query filter.
                    StringBuilder queryBuilder = new StringBuilder();
                    for (OrderedProperty<S> op : altKey.getProperties()) {
                        if (queryBuilder.length() > 0) {
                            queryBuilder.append(" & ");
                        }
                        queryBuilder.append(op.getChainedProperty().toString());
                        queryBuilder.append(" = ?");
                    }

                    // Get query instance from Storage already loaded on stack.
                    b.loadConstant(queryBuilder.toString());
                    b.invokeInterface(TypeDesc.forClass(Storage.class),
                                      QUERY_METHOD_NAME, queryType,
                                      new TypeDesc[]{TypeDesc.STRING});

                    // Now fill in the parameters of the query.
                    for (OrderedProperty<S> op : altKey.getProperties()) {
                        StorableProperty<S> prop = op.getChainedProperty().getPrimeProperty();
                        loadThisProperty(b, prop);
                        TypeDesc bindType = CodeBuilderUtil.bindQueryParam(prop.getType());
                        CodeBuilderUtil.convertValue(b, prop.getType(), bindType.toClass());
                        b.invokeInterface(queryType, WITH_METHOD_NAME, queryType,
                                          new TypeDesc[]{bindType});
                    }

                    b.branch(runQuery);

                    noAltKey.setLocation();
                }

                CodeBuilderUtil.throwException(b, IllegalStateException.class,
                                               "Primary or alternate key not fully specified");

                // Run query sitting on the stack.
                runQuery.setLocation();

                // Locally disable load triggers, to hide the fact that we're
                // using a query to load by alternate key.

                b.loadThis();
                b.loadField(SUPPORT_FIELD_NAME, mSupportType);
                b.invoke(lookupMethod(mSupportType.toClass(), "locallyDisableLoadTrigger"));

                // try-finally start label
                Label disableTriggerStart = b.createLabel().setLocation();

                b.invokeInterface(queryType, TRY_LOAD_ONE_METHOD_NAME,
                                  TypeDesc.forClass(Storable.class), null);
                LocalVariable fetchedVar = b.createLocalVariable(null, TypeDesc.OBJECT);
                b.storeLocal(fetchedVar);

                // try-finally end label
                Label disableTriggerEnd = b.createLabel().setLocation();

                b.loadThis();
                b.loadField(SUPPORT_FIELD_NAME, mSupportType);
                b.invoke(lookupMethod(mSupportType.toClass(), "locallyEnableLoadTrigger"));

                // If query fetch is null, then object not found. Return false.
                b.loadLocal(fetchedVar);
                b.ifNullBranch(notLoaded, true);

                // Copy all properties from fetched object into this one.

                // Allow copy to destroy everything, including primary key.
                b.loadThis();
                b.invokeVirtual(MARK_ALL_PROPERTIES_DIRTY, null, null);

                b.loadLocal(fetchedVar);
                b.checkCast(TypeDesc.forClass(mStorableType));
                b.loadThis();
                b.invokeInterface(TypeDesc.forClass(Storable.class),
                                  COPY_ALL_PROPERTIES, null,
                                  new TypeDesc[] {TypeDesc.forClass(Storable.class)});

                b.branch(loaded);

                // Handler for exception when load trigger is disabled.
                b.exceptionHandler(disableTriggerStart, disableTriggerEnd, null);
                LocalVariable exceptionVar = b.createLocalVariable(null, TypeDesc.OBJECT);
                b.storeLocal(exceptionVar);
                b.loadThis();
                b.loadField(SUPPORT_FIELD_NAME, mSupportType);
                b.invoke(lookupMethod(mSupportType.toClass(), "locallyEnableLoadTrigger"));
                b.loadLocal(exceptionVar);
                b.throwObject();
            }

            pkInitialized.setLocation();

            // Call doTryLoad and mark all properties as clean if load succeeded.
            b.loadThis();
            b.invokeVirtual(DO_TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);

            b.ifZeroComparisonBranch(notLoaded, "==");

            loaded.setLocation();
            // Only indicate load completed if doTryLoad returned true.
            b.loadThis();
            b.invokeVirtual(LOAD_COMPLETED_METHOD_NAME, null, null);
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);

            notLoaded.setLocation();
            // Mark properties dirty, to be consistent with a delete side-effect.
            b.loadThis();
            b.invokeVirtual(MARK_PROPERTIES_DIRTY, null, null);
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);

            // Define the abstract method.
            mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toAbstract(true),
                 DO_TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(FetchException.class));
        }

        // Add load method which calls tryLoad.
        addLoad: {
            // Define the load method.
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC.toSynchronized(true),
                 LOAD_METHOD_NAME, null, null);

            if (mi == null) {
                break addLoad;
            }

            mi.addException(TypeDesc.forClass(FetchException.class));

            CodeBuilder b = new CodeBuilder(mi);

            // Call tryLoad and throw an exception if false returned.
            b.loadThis();
            b.invokeVirtual(TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);

            Label wasNotLoaded = b.createLabel();
            b.ifZeroComparisonBranch(wasNotLoaded, "==");
            b.returnVoid();

            wasNotLoaded.setLocation();

            TypeDesc noMatchesType = TypeDesc.forClass(FetchNoneException.class);
            b.newObject(noMatchesType);
            b.dup();
            b.loadThis();
            b.invokeVirtual(TO_STRING_KEY_ONLY_METHOD_NAME, TypeDesc.STRING, null);
            b.invokeConstructor(noMatchesType, new TypeDesc[] {TypeDesc.STRING});
            b.throwObject();
        }

        final TypeDesc triggerType = TypeDesc.forClass(Trigger.class);
        final TypeDesc transactionType = TypeDesc.forClass(Transaction.class);

        // Add insert(boolean forTry) method which delegates to abstract doTryInsert method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PRIVATE.toSynchronized(true),
                 PRIVATE_INSERT_METHOD_NAME, TypeDesc.BOOLEAN, new TypeDesc[] {TypeDesc.BOOLEAN});
            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable forTryVar = b.getParameter(0);
            LocalVariable triggerVar = b.createLocalVariable(null, triggerType);
            LocalVariable txnVar = b.createLocalVariable(null, transactionType);
            LocalVariable stateVar = b.createLocalVariable(null, TypeDesc.OBJECT);

            Label tryStart = addGetTriggerAndEnterTxn
                (b, INSERT_OP, forTryVar, false, triggerVar, txnVar, stateVar);

            // Perform pk check after trigger has run, to allow it to define pk.
            requirePkInitialized(b, CHECK_PK_FOR_INSERT_METHOD_NAME);

            // Call doTryInsert.
            b.loadThis();
            b.invokeVirtual(DO_TRY_INSERT_METHOD_NAME, TypeDesc.BOOLEAN, null);

            Label notInserted = b.createLabel();
            b.ifZeroComparisonBranch(notInserted, "==");

            addTriggerAfterAndExitTxn
                (b, INSERT_OP, forTryVar, false, triggerVar, txnVar, stateVar);

            // Only mark properties clean if doTryInsert returned true.
            b.loadThis();
            b.invokeVirtual(MARK_ALL_PROPERTIES_CLEAN, null, null);
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);

            notInserted.setLocation();
            addTriggerFailedAndExitTxn(b, INSERT_OP, triggerVar, txnVar, stateVar);

            b.loadLocal(forTryVar);
            Label isForTry = b.createLabel();
            b.ifZeroComparisonBranch(isForTry, "!=");

            TypeDesc constraintType = TypeDesc.forClass(UniqueConstraintException.class);
            b.newObject(constraintType);
            b.dup();
            b.loadThis();
            b.invokeVirtual(TO_STRING_METHOD_NAME, TypeDesc.STRING, null);
            b.invokeConstructor(constraintType, new TypeDesc[] {TypeDesc.STRING});
            b.throwObject();

            isForTry.setLocation();
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);

            addTriggerFailedAndExitTxn
                (b, INSERT_OP, forTryVar, false, triggerVar, txnVar, stateVar, tryStart);

            // Define the abstract method.
            mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toAbstract(true),
                 DO_TRY_INSERT_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
        }

        // Add insert method which calls insert(forTry = false)
        addInsert: {
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, INSERT_METHOD_NAME, null, null);

            if (mi == null) {
                break addInsert;
            }

            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadConstant(false);
            b.invokePrivate(PRIVATE_INSERT_METHOD_NAME, TypeDesc.BOOLEAN,
                            new TypeDesc[] {TypeDesc.BOOLEAN});
            b.pop();
            b.returnVoid();
        }

        // Add tryInsert method which calls insert(forTry = true)
        addTryInsert: {
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, TRY_INSERT_METHOD_NAME, TypeDesc.BOOLEAN, null);

            if (mi == null) {
                break addTryInsert;
            }

            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadConstant(true);
            b.invokePrivate(PRIVATE_INSERT_METHOD_NAME, TypeDesc.BOOLEAN,
                            new TypeDesc[] {TypeDesc.BOOLEAN});
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add update(boolean forTry) method which delegates to abstract doTryUpdate method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PRIVATE.toSynchronized(true),
                 PRIVATE_UPDATE_METHOD_NAME, TypeDesc.BOOLEAN, new TypeDesc[] {TypeDesc.BOOLEAN});
            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            requirePkInitialized(b, CHECK_PK_FOR_UPDATE_METHOD_NAME);

            // If version property is present, it too must be initialized. The
            // versionOrdinal variable was set earlier, when properties were defined.
            if (versionOrdinal >= 0) {
                b.loadThis();
                b.loadField(PROPERTY_STATE_FIELD_NAME + (versionOrdinal >> 4), TypeDesc.INT);
                b.loadConstant(PROPERTY_STATE_MASK << ((versionOrdinal & 0xf) * 2));
                b.math(Opcode.IAND);
                Label versionIsSet = b.createLabel();
                b.ifZeroComparisonBranch(versionIsSet, "!=");
                CodeBuilderUtil.throwException
                    (b, IllegalStateException.class, "Version not set");
                versionIsSet.setLocation();
            }

            LocalVariable forTryVar = b.getParameter(0);
            LocalVariable triggerVar = b.createLocalVariable(null, triggerType);
            LocalVariable txnVar = b.createLocalVariable(null, transactionType);
            LocalVariable stateVar = b.createLocalVariable(null, TypeDesc.OBJECT);

            Label tryStart = addGetTriggerAndEnterTxn
                (b, UPDATE_OP, forTryVar, false, triggerVar, txnVar, stateVar);

            // Call doTryUpdate.
            b.loadThis();
            b.invokeVirtual(DO_TRY_UPDATE_METHOD_NAME, TypeDesc.BOOLEAN, null);

            Label notUpdated = b.createLabel();
            b.ifZeroComparisonBranch(notUpdated, "==");

            addTriggerAfterAndExitTxn
                (b, UPDATE_OP, forTryVar, false, triggerVar, txnVar, stateVar);

            // Only mark properties clean if doUpdate returned true.
            b.loadThis();
            // Note: all properties marked clean because doUpdate should have
            // loaded values for all properties.
            b.invokeVirtual(MARK_ALL_PROPERTIES_CLEAN, null, null);
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);

            notUpdated.setLocation();
            addTriggerFailedAndExitTxn(b, UPDATE_OP, triggerVar, txnVar, stateVar);

            // Mark properties dirty, to be consistent with a delete side-effect.
            b.loadThis();
            b.invokeVirtual(MARK_PROPERTIES_DIRTY, null, null);

            b.loadLocal(forTryVar);
            Label isForTry = b.createLabel();
            b.ifZeroComparisonBranch(isForTry, "!=");

            TypeDesc persistNoneType = TypeDesc.forClass(PersistNoneException.class);
            b.newObject(persistNoneType);
            b.dup();
            b.loadConstant("Cannot update missing object: ");
            b.loadThis();
            b.invokeVirtual(TO_STRING_METHOD_NAME, TypeDesc.STRING, null);
            b.invokeVirtual(TypeDesc.STRING, "concat",
                            TypeDesc.STRING, new TypeDesc[] {TypeDesc.STRING});
            b.invokeConstructor(persistNoneType, new TypeDesc[] {TypeDesc.STRING});
            b.throwObject();

            isForTry.setLocation();
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);

            addTriggerFailedAndExitTxn
                (b, UPDATE_OP, forTryVar, false, triggerVar, txnVar, stateVar, tryStart);

            // Define the abstract method.
            mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toAbstract(true),
                 DO_TRY_UPDATE_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
        }

        // Add update method which calls update(forTry = false)
        addUpdate: {
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, UPDATE_METHOD_NAME, null, null);

            if (mi == null) {
                break addUpdate;
            }

            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadConstant(false);
            b.invokePrivate(PRIVATE_UPDATE_METHOD_NAME, TypeDesc.BOOLEAN,
                            new TypeDesc[] {TypeDesc.BOOLEAN});
            b.pop();
            b.returnVoid();
        }

        // Add tryUpdate method which calls update(forTry = true)
        addTryUpdate: {
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, TRY_UPDATE_METHOD_NAME, TypeDesc.BOOLEAN, null);

            if (mi == null) {
                break addTryUpdate;
            }

            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadConstant(true);
            b.invokePrivate(PRIVATE_UPDATE_METHOD_NAME, TypeDesc.BOOLEAN,
                            new TypeDesc[] {TypeDesc.BOOLEAN});
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add delete(boolean forTry) method which delegates to abstract doTryDelete method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PRIVATE.toSynchronized(true),
                 PRIVATE_DELETE_METHOD_NAME, TypeDesc.BOOLEAN, new TypeDesc[] {TypeDesc.BOOLEAN});
            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            requirePkInitialized(b, CHECK_PK_FOR_DELETE_METHOD_NAME);

            LocalVariable forTryVar = b.getParameter(0);
            LocalVariable triggerVar = b.createLocalVariable(null, triggerType);
            LocalVariable txnVar = b.createLocalVariable(null, transactionType);
            LocalVariable stateVar = b.createLocalVariable(null, TypeDesc.OBJECT);

            Label tryStart = addGetTriggerAndEnterTxn
                (b, DELETE_OP, forTryVar, false, triggerVar, txnVar, stateVar);

            // Call doTryDelete.
            b.loadThis();
            b.invokeVirtual(DO_TRY_DELETE_METHOD_NAME, TypeDesc.BOOLEAN, null);

            b.loadThis();
            b.invokeVirtual(MARK_PROPERTIES_DIRTY, null, null);

            Label notDeleted = b.createLabel();
            b.ifZeroComparisonBranch(notDeleted, "==");

            addTriggerAfterAndExitTxn
                (b, DELETE_OP, forTryVar, false, triggerVar, txnVar, stateVar);
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);

            notDeleted.setLocation();
            addTriggerFailedAndExitTxn(b, DELETE_OP, triggerVar, txnVar, stateVar);

            b.loadLocal(forTryVar);
            Label isForTry = b.createLabel();
            b.ifZeroComparisonBranch(isForTry, "!=");

            TypeDesc persistNoneType = TypeDesc.forClass(PersistNoneException.class);
            b.newObject(persistNoneType);
            b.dup();
            b.loadConstant("Cannot delete missing object: ");
            b.loadThis();
            b.invokeVirtual(TO_STRING_METHOD_NAME, TypeDesc.STRING, null);
            b.invokeVirtual(TypeDesc.STRING, "concat",
                            TypeDesc.STRING, new TypeDesc[] {TypeDesc.STRING});
            b.invokeConstructor(persistNoneType, new TypeDesc[] {TypeDesc.STRING});
            b.throwObject();

            isForTry.setLocation();
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);

            addTriggerFailedAndExitTxn
                (b, DELETE_OP, forTryVar, false, triggerVar, txnVar, stateVar, tryStart);

            // Define the abstract method.
            mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toAbstract(true),
                 DO_TRY_DELETE_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(TypeDesc.forClass(PersistException.class));
        }

        // Add delete method which calls delete(forTry = false)
        addDelete: {
            // Define the delete method.
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, DELETE_METHOD_NAME, null, null);

            if (mi == null) {
                break addDelete;
            }

            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadConstant(false);
            b.invokePrivate(PRIVATE_DELETE_METHOD_NAME, TypeDesc.BOOLEAN,
                            new TypeDesc[] {TypeDesc.BOOLEAN});
            b.pop();
            b.returnVoid();
        }

        // Add tryDelete method which calls delete(forTry = true)
        addTryDelete: {
            // Define the delete method.
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, TRY_DELETE_METHOD_NAME, TypeDesc.BOOLEAN, null);

            if (mi == null) {
                break addTryDelete;
            }

            mi.addException(TypeDesc.forClass(PersistException.class));

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadConstant(true);
            b.invokePrivate(PRIVATE_DELETE_METHOD_NAME, TypeDesc.BOOLEAN,
                            new TypeDesc[] {TypeDesc.BOOLEAN});
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Add storableType method
        addStorableType: {
            final TypeDesc type = TypeDesc.forClass(mStorableType);
            final TypeDesc storableClassType = TypeDesc.forClass(Class.class);
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, STORABLE_TYPE_METHOD_NAME, storableClassType, null);

            if (mi == null) {
                break addStorableType;
            }

            CodeBuilder b = new CodeBuilder(mi);
            b.loadConstant(type);
            b.returnValue(storableClassType);
        }

        // Add copy method.
        addCopy: {
            TypeDesc type = TypeDesc.forClass(mInfo.getStorableType());

            // Add copy method.
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC.toSynchronized(true),
                 COPY_METHOD_NAME, mClassFile.getType(), null);

            if (mi == null) {
                break addCopy;
            }

            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.invokeVirtual(CLONE_METHOD_NAME, TypeDesc.OBJECT, null);
            b.checkCast(mClassFile.getType());

            b.returnValue(type);
        }

        // Part of properly defining copy method, except needs to be added even
        // if copy method was not added because it is inherited and final.
        CodeBuilderUtil.defineCopyBridges(mClassFile, mInfo.getStorableType());

        // Create all the property copier methods.
        // Boolean params: pkProperties, versionProperty, dataProperties, unequalOnly, dirtyOnly
        addCopyPropertiesMethod(COPY_ALL_PROPERTIES,
                                true, true, true, false, false);
        addCopyPropertiesMethod(COPY_PRIMARY_KEY_PROPERTIES,
                                true, false, false, false, false);
        addCopyPropertiesMethod(COPY_VERSION_PROPERTY,
                                false, true, false, false, false);
        addCopyPropertiesMethod(COPY_UNEQUAL_PROPERTIES,
                                false, true, true, true, false);
        addCopyPropertiesMethod(COPY_DIRTY_PROPERTIES,
                                false, true, true, false,  true);

        // Define hasDirtyProperties method.
        addHasDirtyProps: {
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, HAS_DIRTY_PROPERTIES, TypeDesc.BOOLEAN, null);

            if (mi == null) {
                break addHasDirtyProps;
            }

            CodeBuilder b = new CodeBuilder(mi);
            Label isDirty = b.createLabel();
            branchIfDirty(b, false, isDirty);
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);
            isDirty.setLocation();
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Define isPropertyUninitialized, isPropertyDirty, and isPropertyClean methods.
        addPropertyStateExtractMethod();
        addPropertyStateCheckMethod(IS_PROPERTY_UNINITIALIZED, PROPERTY_STATE_UNINITIALIZED);
        addPropertyStateCheckMethod(IS_PROPERTY_DIRTY, PROPERTY_STATE_DIRTY);
        addPropertyStateCheckMethod(IS_PROPERTY_CLEAN, PROPERTY_STATE_CLEAN);

        // Define isPropertySupported method.
        addIsPropertySupported: {
            MethodInfo mi = addMethodIfNotFinal
                (Modifiers.PUBLIC, IS_PROPERTY_SUPPORTED,
                 TypeDesc.BOOLEAN, new TypeDesc[] {TypeDesc.STRING});

            if (mi == null) {
                break addIsPropertySupported;
            }

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadField(SUPPORT_FIELD_NAME, mSupportType);
            b.loadLocal(b.getParameter(0));
            b.invokeInterface(mSupportType, "isPropertySupported", TypeDesc.BOOLEAN,
                              new TypeDesc[] {TypeDesc.STRING});
            b.returnValue(TypeDesc.BOOLEAN);
        }

        // Define reflection-like methods for manipulating properties by name.
        addGetPropertyValueMethod();
        addSetPropertyValueMethod();
        addPropertyMapMethod();

        // Define serialization methods.
        addWriteToMethod();
        addReadFromMethod();

        // Define standard object methods.
        addHashCodeMethod();
        addEqualsMethod(EQUAL_FULL);
        addEqualsMethod(EQUAL_KEYS);
        addEqualsMethod(EQUAL_PROPERTIES);
        addToStringMethod(false);
        addToStringMethod(true);

        addMarkCleanMethod(MARK_PROPERTIES_CLEAN);
        addMarkCleanMethod(MARK_ALL_PROPERTIES_CLEAN);
        addMarkDirtyMethod(MARK_PROPERTIES_DIRTY);
        addMarkDirtyMethod(MARK_ALL_PROPERTIES_DIRTY);

        // Define loadCompleted method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED, LOAD_COMPLETED_METHOD_NAME, null, null);
            mi.addException(TypeDesc.forClass(FetchException.class));

            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.invokeVirtual(MARK_ALL_PROPERTIES_CLEAN, null, null);

            // Now invoke trigger.
            b.loadThis();
            b.loadField(SUPPORT_FIELD_NAME, mSupportType);
            b.invoke(lookupMethod(mSupportType.toClass(), "getLoadTrigger"));
            LocalVariable triggerVar =
                b.createLocalVariable(null, TypeDesc.forClass(Trigger.class));
            b.storeLocal(triggerVar);
            b.loadLocal(triggerVar);
            Label noTrigger = b.createLabel();
            b.ifNullBranch(noTrigger, true);
            b.loadLocal(triggerVar);
            b.loadThis();
            b.invoke(lookupMethod(triggerVar.getType().toClass(), "afterLoad", Object.class));

            // In case trigger modified the properties, make sure they're still clean.
            b.loadThis();
            b.invokeVirtual(MARK_ALL_PROPERTIES_CLEAN, null, null);

            noTrigger.setLocation();
            b.returnVoid();
        }

        {
            // Define protected isPkInitialized method.
            addIsInitializedMethod
                (IS_PK_INITIALIZED_METHOD_NAME, mInfo.getPrimaryKeyProperties());

            // Define protected methods to check if alternate key is initialized.
            addAltKeyMethods:
            for (int i=0; i<mInfo.getAlternateKeyCount(); i++) {
                Map<String, StorableProperty<S>> altProps =
                    new LinkedHashMap<String, StorableProperty<S>>();

                StorableKey<S> altKey = mInfo.getAlternateKey(i);

                for (OrderedProperty<S> op : altKey.getProperties()) {
                    ChainedProperty<S> cp = op.getChainedProperty();
                    if (cp.getChainCount() > 0) {
                        // This should not be possible.
                        continue addAltKeyMethods;
                    }
                    StorableProperty<S> property = cp.getPrimeProperty();
                    altProps.put(property.getName(), property);
                }

                addIsInitializedMethod(IS_ALT_KEY_INITIALIZED_PREFIX + i, altProps);
            }

            // Define protected isRequiredDataInitialized method.
            defineIsRequiredDataInitialized: {
                Map<String, StorableProperty<S>> requiredProperties =
                    new LinkedHashMap<String, StorableProperty<S>>();

                for (StorableProperty property : mAllProperties.values()) {
                    if (!property.isDerived() &&
                        !property.isPrimaryKeyMember() &&
                        !property.isJoin() &&
                        !property.isNullable()) {

                        requiredProperties.put(property.getName(), property);
                    }
                }

                addIsInitializedMethod
                    (IS_REQUIRED_DATA_INITIALIZED_METHOD_NAME, requiredProperties);
            }

            // Define optional protected isVersionInitialized method. The
            // versionOrdinal variable was set earlier, when properties were defined.
            if (versionOrdinal >= 0) {
                MethodInfo mi = mClassFile.addMethod
                    (Modifiers.PROTECTED, IS_VERSION_INITIALIZED_METHOD_NAME,
                     TypeDesc.BOOLEAN, null);
                CodeBuilder b = new CodeBuilder(mi);
                b.loadThis();
                b.loadField(PROPERTY_STATE_FIELD_NAME + (versionOrdinal >> 4), TypeDesc.INT);
                b.loadConstant(PROPERTY_STATE_MASK << ((versionOrdinal & 0xf) * 2));
                b.math(Opcode.IAND);
                // zero == false, not zero == true
                b.returnValue(TypeDesc.BOOLEAN);
            }
        }
    }

    /*
    private static Method lookupMethod(Class type, MethodInfo mi) {
        MethodDesc desc = mi.getMethodDescriptor();
        TypeDesc[] params = desc.getParameterTypes();
        Class[] args;

        if (params == null || params.length == 0) {
            args = null;
        } else {
            args = new Class[params.length];
            for (int i=0; i<args.length; i++) {
                args[i] = params[i].toClass();
            }
        }

        return lookupMethod(type, mi.getName(), args);
    }
    */

    private static Method lookupMethod(Class type, String name, Class... args) {
        try {
            return type.getMethod(name, args);
        } catch (NoSuchMethodException e) {
            Error error = new NoSuchMethodError();
            error.initCause(e);
            throw error;
        }
    }

    private void addPropertyBridges(StorableProperty<S> property) {
        Class[] covariantTypes = property.getCovariantTypes();
        if (covariantTypes == null || covariantTypes.length == 0) {
            return;
        }

        // Define copy bridges to allow covariant property types.

        for (Class type : covariantTypes) {
            TypeDesc desc = TypeDesc.forClass(type);

            if (property.getReadMethod() != null &&
                property.getReadMethod().getReturnType() != type)
            {
                MethodInfo mi = addMethodIfNotFinal
                    (Modifiers.PUBLIC.toBridge(true), property.getReadMethodName(), desc, null);

                if (mi != null) {
                    CodeBuilder b = new CodeBuilder(mi);
                    b.loadThis();
                    b.invoke(property.getReadMethod());
                    b.returnValue(desc);
                }
            }

            if (property.getWriteMethod() != null &&
                property.getWriteMethod().getParameterTypes()[0] != type)
            {
                // Not actually defined as a bridge method since parameter type differs.
                MethodInfo mi = addMethodIfNotFinal
                    (Modifiers.PUBLIC, property.getWriteMethodName(), null, new TypeDesc[] {desc});

                if (mi != null) {
                    CodeBuilder b = new CodeBuilder(mi);
                    b.loadThis();
                    b.loadLocal(b.getParameter(0));
                    b.checkCast(TypeDesc.forClass(property.getType()));
                    b.invoke(property.getWriteMethod());
                    b.returnVoid();
                }
            }
        }
    }

    /**
     * Generates a copy properties method with several options to control its
     * behavior. Although eight combinations can be defined, only four are
     * required by Storable interface. Uninitialized properties are never
     * copied.
     *
     * @param pkProperties when true, copy primary key properties
     * @param dataProperties when true, copy data properties
     * @param unequalOnly when true, only copy unequal properties
     * @param dirtyOnly when true, only copy dirty properties
     */
    private void addCopyPropertiesMethod
        (String methodName,
         boolean pkProperties,
         boolean versionProperty,
         boolean dataProperties,
         boolean unequalOnly,
         boolean dirtyOnly)
    {
        TypeDesc[] param = { TypeDesc.forClass(Storable.class) };
        TypeDesc storableTypeDesc = TypeDesc.forClass(mStorableType);

        MethodInfo mi= addMethodIfNotFinal
            (Modifiers.PUBLIC.toSynchronized(true), methodName, null, param);

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        LocalVariable target = CodeBuilderUtil.uneraseGenericParameter(b, storableTypeDesc, 0);

        LocalVariable stateBits = null;
        int mask = PROPERTY_STATE_MASK;

        for (StorableProperty property : mAllProperties.values()) {
            // Decide if property should be part of the copy.
            boolean shouldCopy =
                (!property.isDerived() || property.shouldCopyDerived()) &&
                !property.isJoin() &&
                (property.isPrimaryKeyMember() && pkProperties ||
                 property.isVersion() && versionProperty ||
                 !property.isPrimaryKeyMember() && dataProperties);

            if (shouldCopy) {
                int ordinal = property.getNumber();

                if (stateBits == null && !property.isDerived()) {
                    // Load state bits into local for quick retrieval.
                    stateBits = b.createLocalVariable(null, TypeDesc.INT);
                    String stateFieldName =
                        StorableGenerator.PROPERTY_STATE_FIELD_NAME + (ordinal >> 4);
                    b.loadThis();
                    b.loadField(stateFieldName, TypeDesc.INT);
                    b.storeLocal(stateBits);
                }

                Label skipCopy = b.createLabel();

                // Check if independent property is supported, and skip if not.
                if (property.isIndependent()) {
                    addSkipIndependent(b, target, property, skipCopy);
                }

                if (stateBits != null && !property.isDerived()) {
                    // Skip property if uninitialized.
                    b.loadLocal(stateBits);
                    b.loadConstant(mask);
                    b.math(Opcode.IAND);
                    b.ifZeroComparisonBranch(skipCopy, "==");

                    if (dirtyOnly) {
                        // Add code to find out if property has been dirty.
                        b.loadLocal(stateBits);
                        b.loadConstant(mask);
                        b.math(Opcode.IAND);
                        b.loadConstant(PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2));
                        b.ifComparisonBranch(skipCopy, "!=");
                    }
                }

                TypeDesc type = TypeDesc.forClass(property.getType());

                if (unequalOnly) {
                    // Add code to find out if they're equal.
                    loadThisProperty(b, property, type);  // [this.propValue
                    b.loadLocal(target);                  // [this.propValue, target
                    b.invoke(property.getReadMethod());   // [this.propValue, target.propValue
                    CodeBuilderUtil.addValuesEqualCall
                        (b, TypeDesc.forClass(property.getType()), true, skipCopy, true);
                }

                b.loadLocal(target);                  // [target
                loadThisProperty(b, property, type);  // [target, this.propValue
                storeProperty(b, property, type);

                skipCopy.setLocation();
            }

            if ((mask <<= 2) == 0) {
                mask = PROPERTY_STATE_MASK;
                stateBits = null;
            }
        }

        b.returnVoid();
    }

    private void addSkipIndependent(CodeBuilder b,
                                    LocalVariable target,
                                    StorableProperty property,
                                    Label skipCopy)
    {
        TypeDesc storableTypeDesc = TypeDesc.forClass(Storable.class);

        if (target != null) {
            b.loadLocal(target);
            b.loadConstant(property.getName());
            b.invokeInterface(storableTypeDesc,
                              "isPropertySupported",
                              TypeDesc.BOOLEAN,
                              new TypeDesc[] {TypeDesc.STRING});
            b.ifZeroComparisonBranch(skipCopy, "==");
        }

        b.loadThis();
        b.loadConstant(property.getName());
        b.invokeInterface(storableTypeDesc,
                          "isPropertySupported",
                          TypeDesc.BOOLEAN,
                          new TypeDesc[] {TypeDesc.STRING});
        b.ifZeroComparisonBranch(skipCopy, "==");
    }

    /**
     * Loads the property value of the current storable onto the stack. If the
     * property is derived the read method is used, otherwise it just loads the
     * value from the appropriate field.
     *
     * entry stack: [
     * exit stack: [value
     *
     * @param b - {@link CodeBuilder} to which to add the load code
     * @param property - property to load
     */
    private void loadThisProperty(CodeBuilder b, StorableProperty property) {
        loadThisProperty(b, property, TypeDesc.forClass(property.getType()));
    }

    /**
     * Loads the property value of the current storable onto the stack. If the
     * property is derived the read method is used, otherwise it just loads the
     * value from the appropriate field.
     *
     * entry stack: [
     * exit stack: [value
     *
     * @param b - {@link CodeBuilder} to which to add the load code
     * @param property - property to load
     * @param type - type of the property
     */
    private void loadThisProperty(CodeBuilder b, StorableProperty property, TypeDesc type) {
        b.loadThis();
        if (property.isDerived()) {
            b.invoke(property.getReadMethod());
        } else {
            b.loadField(property.getName(), type);
        }
    }

    /**
     * Puts the value on the stack into the specified storable.  If a write
     * method is defined, use it. Otherwise, just shove the value into the
     * appropriate field.
     *
     * entry stack: [storable, value
     * exit stack: [
     *
     * @param b - {@link CodeBuilder} to which to add the mutation code
     * @param property - property to mutate
     * @param type - type of the property
     */
    private void storeProperty(CodeBuilder b, StorableProperty property, TypeDesc type) {
        if (property.getWriteMethod() == null && !property.isDerived()) {
            b.storeField(property.getName(), type);
        } else {
            b.invoke(property.getWriteMethod());
        }
    }

    /**
     * Generates code that loads a property annotation to the stack.
     */
    private void loadPropertyAnnotation(CodeBuilder b,
                                        StorableProperty property,
                                        StorablePropertyAnnotation annotation) {
        /* Example
           UserInfo.class.getMethod("setFirstName", new Class[] {String.class})
               .getAnnotation(LengthConstraint.class)
        */

        String methodName = annotation.getAnnotatedMethod().getName();
        boolean isAccessor = !methodName.startsWith("set");

        b.loadConstant(TypeDesc.forClass(property.getEnclosingType()));
        b.loadConstant(methodName);
        if (isAccessor) {
            // Accessor method has no parameters.
            b.loadNull();
        } else {
            // Mutator method has one parameter.
            b.loadConstant(1);
            b.newObject(TypeDesc.forClass(Class[].class));
            b.dup();
            b.loadConstant(0);
            b.loadConstant(TypeDesc.forClass(property.getType()));
            b.storeToArray(TypeDesc.forClass(Class[].class));
        }
        b.invokeVirtual(Class.class.getName(), "getMethod",
                        TypeDesc.forClass(Method.class), new TypeDesc[] {
                            TypeDesc.STRING, TypeDesc.forClass(Class[].class)
                        });
        b.loadConstant(TypeDesc.forClass(annotation.getAnnotationType()));
        b.invokeVirtual(Method.class.getName(), "getAnnotation",
                        TypeDesc.forClass(Annotation.class), new TypeDesc[] {
                            TypeDesc.forClass(Class.class)
                        });
        b.checkCast(TypeDesc.forClass(annotation.getAnnotationType()));
    }

    /**
     * Generates code that loads a Storage instance on the stack, throwing a
     * FetchException if Storage request fails.
     *
     * @param type type of Storage to request
     */
    private void loadStorageForFetch(CodeBuilder b, TypeDesc type) {
        b.loadThis();
        b.loadField(SUPPORT_FIELD_NAME, mSupportType);
        TypeDesc storageType = TypeDesc.forClass(Storage.class);

        TypeDesc repositoryType = TypeDesc.forClass(Repository.class);
        b.invokeInterface
            (mSupportType, "getRootRepository", repositoryType, null);
        b.loadConstant(type);

        // This may throw a RepositoryException.
        Label tryStart = b.createLabel().setLocation();
        b.invokeInterface(repositoryType, STORAGE_FOR_METHOD_NAME, storageType,
                          new TypeDesc[]{TypeDesc.forClass(Class.class)});
        Label tryEnd = b.createLabel().setLocation();
        Label noException = b.createLabel();
        b.branch(noException);

        b.exceptionHandler(tryStart, tryEnd,
                           RepositoryException.class.getName());
        b.invokeVirtual
            (RepositoryException.class.getName(), "toFetchException",
             TypeDesc.forClass(FetchException.class), null);
        b.throwObject();

        noException.setLocation();
    }

    /**
     * For the given join property, marks all of its dependent internal join
     * element properties as dirty.
     */
    /*
    private void markInternalJoinElementsDirty(CodeBuilder b, StorableProperty joinProperty) {
        int count = mAllProperties.size();

        int ordinal = 0;
        int mask = 0;
        for (StorableProperty property : mAllProperties.values()) {
            if (property != joinProperty && !property.isDerived() && !property.isJoin()) {
                // Check to see if property is an internal member of joinProperty.
                for (int i=joinProperty.getJoinElementCount(); --i>=0; ) {
                    if (property == joinProperty.getInternalJoinElement(i)) {
                        mask |= PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2);
                    }
                }
            }
            ordinal++;
            if (((ordinal & 0xf) == 0 || ordinal >= count) && mask != 0) {
                String stateFieldName = PROPERTY_STATE_FIELD_NAME + ((ordinal - 1) >> 4);
                b.loadThis();
                b.loadThis();
                b.loadField(stateFieldName, TypeDesc.INT);
                b.loadConstant(mask);
                b.math(Opcode.IOR);
                b.storeField(stateFieldName, TypeDesc.INT);
                mask = 0;
            }
        }
    }
    */

    /**
     * Generates code to set all state properties to zero.
     */
    /*
    private void clearState(CodeBuilder b) {
        int ordinal = -1;
        int maxOrdinal = mAllProperties.size() - 1;
        boolean requireStateField = false;

        for (StorableProperty property : mAllProperties.values()) {
            ordinal++;

            if (!property.isDerived()) {
                requireStateField = true;

                if (ordinal == maxOrdinal || ((ordinal & 0xf) == 0xf)) {
                    if (requireStateField) {
                        String stateFieldName = PROPERTY_STATE_FIELD_NAME + (ordinal >> 4);

                        b.loadThis();
                        b.loadConstant(0);
                        b.storeField(stateFieldName, TypeDesc.INT);
                    }
                    requireStateField = false;
                }
            }
        }
    }
    */

    private void addMarkCleanMethod(String name) {
        MethodInfo mi =
            addMethodIfNotFinal(Modifiers.PUBLIC.toSynchronized(true), name, null, null);

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        final int count = mAllProperties.size();
        int ordinal = 0;
        int andMask = 0;
        int orMask = 0;
        boolean anyNonDerived = false;

        for (StorableProperty property : mAllProperties.values()) {
            if (!property.isDerived()) {
                anyNonDerived = true;
                if (property.isQuery()) {
                    // Don't erase cached query.
                    andMask |= PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2);
                } else if (!property.isJoin()) {
                    if (name == MARK_ALL_PROPERTIES_CLEAN) {
                        // Force clean state (1) always.
                        orMask |= PROPERTY_STATE_CLEAN << ((ordinal & 0xf) * 2);
                    } else if (name == MARK_PROPERTIES_CLEAN) {
                        // Mask will convert dirty (3) to clean (1). State 2, which
                        // is illegal, is converted to 0.
                        andMask |= PROPERTY_STATE_CLEAN << ((ordinal & 0xf) * 2);
                    }
                }
            }

            ordinal++;
            if ((ordinal & 0xf) == 0 || ordinal >= count) {
                if (anyNonDerived) {
                    String stateFieldName = PROPERTY_STATE_FIELD_NAME + ((ordinal - 1) >> 4);
                    b.loadThis();
                    if (andMask == 0) {
                        b.loadConstant(orMask);
                    } else {
                        b.loadThis();
                        b.loadField(stateFieldName, TypeDesc.INT);
                        b.loadConstant(andMask);
                        b.math(Opcode.IAND);
                        if (orMask != 0) {
                            b.loadConstant(orMask);
                            b.math(Opcode.IOR);
                        }
                    }
                    b.storeField(stateFieldName, TypeDesc.INT);
                }

                andMask = 0;
                orMask = 0;
                anyNonDerived = false;
            }
        }

        b.returnVoid();
    }

    private void addMarkDirtyMethod(String name) {
        MethodInfo mi =
            addMethodIfNotFinal(Modifiers.PUBLIC.toSynchronized(true), name, null, null);

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        final int count = mAllProperties.size();
        int ordinal = 0;
        int andMask = 0;
        int orMask = 0;
        boolean anyNonDerived = false;

        for (StorableProperty property : mAllProperties.values()) {
            if (!property.isDerived()) {
                anyNonDerived = true;
                if (property.isJoin()) {
                    // Erase cached join properties, but don't erase cached query.
                    if (!property.isQuery()) {
                        andMask |= PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2);
                    }
                } else if (name == MARK_ALL_PROPERTIES_DIRTY) {
                    // Force dirty state (3).
                    orMask |= PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2);
                }
            }

            ordinal++;
            if ((ordinal & 0xf) == 0 || ordinal >= count) {
                if (anyNonDerived) {
                    String stateFieldName = PROPERTY_STATE_FIELD_NAME + ((ordinal - 1) >> 4);
                    if (name == MARK_ALL_PROPERTIES_DIRTY) {
                        if (orMask != 0 || andMask != 0) {
                            b.loadThis(); // [this
                            b.loadThis(); // [this, this
                            b.loadField(stateFieldName, TypeDesc.INT); // [this, this.stateField
                            if (andMask != 0) {
                                b.loadConstant(~andMask);
                                b.math(Opcode.IAND);
                            }
                            if (orMask != 0) {
                                b.loadConstant(orMask);
                                b.math(Opcode.IOR);
                            }
                            b.storeField(stateFieldName, TypeDesc.INT);
                        }
                    } else {
                        // This is a great trick to convert all states of value 1
                        // (clean) into value 3 (dirty). States 0, 2, and 3 stay the
                        // same. Since joins cannot have state 1, they aren't affected.
                        // stateField |= ((stateField & 0x55555555) << 1);

                        b.loadThis(); // [this
                        b.loadThis(); // [this, this
                        b.loadField(stateFieldName, TypeDesc.INT); // [this, this.stateField
                        if (andMask != 0) {
                            b.loadConstant(~andMask);
                            b.math(Opcode.IAND);
                        }
                        b.dup(); // [this, this.stateField, this.stateField
                        b.loadConstant(0x55555555);
                        b.math(Opcode.IAND); // [this, this.stateField, this.stateField &0x55555555
                        b.loadConstant(1);
                        b.math(Opcode.ISHL); // [this, this.stateField, orMaskValue
                        b.math(Opcode.IOR);  // [this, newStateFieldValue
                        b.storeField(stateFieldName, TypeDesc.INT);
                    }
                }

                andMask = 0;
                orMask = 0;
                anyNonDerived = false;
            }
        }

        b.returnVoid();
    }

    /**
     * For the given ordinary key property, marks all of its dependent join
     * element properties as uninitialized, and marks given property as dirty.
     */
    private void markOrdinaryPropertyDirty
        (CodeBuilder b, StorableProperty ordinaryProperty)
    {
        int count = mAllProperties.size();

        int ordinal = 0;
        int andMask = 0xffffffff;
        int orMask = 0;
        boolean anyNonDerived = false;

        for (StorableProperty property : mAllProperties.values()) {
            if (!property.isDerived()) {
                anyNonDerived = true;
                if (property == ordinaryProperty) {
                    orMask |= PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2);
                } else if (property.isJoin()) {
                    // Check to see if ordinary is an internal member of join property.
                    for (int i=property.getJoinElementCount(); --i>=0; ) {
                        if (ordinaryProperty == property.getInternalJoinElement(i)) {
                            andMask &= ~(PROPERTY_STATE_DIRTY << ((ordinal & 0xf) * 2));
                        }
                    }
                }
            }

            ordinal++;
            if ((ordinal & 0xf) == 0 || ordinal >= count) {
                if (anyNonDerived && (andMask != 0xffffffff || orMask != 0)) {
                    String stateFieldName = PROPERTY_STATE_FIELD_NAME + ((ordinal - 1) >> 4);
                    b.loadThis();
                    b.loadThis();
                    b.loadField(stateFieldName, TypeDesc.INT);
                    if (andMask != 0xffffffff) {
                        b.loadConstant(andMask);
                        b.math(Opcode.IAND);
                    }
                    if (orMask != 0) {
                        b.loadConstant(orMask);
                        b.math(Opcode.IOR);
                    }
                    b.storeField(stateFieldName, TypeDesc.INT);
                }

                andMask = 0xffffffff;
                orMask = 0;
                anyNonDerived = false;
            }
        }
    }

    // Generates code that branches to the given label if any properties are dirty.
    private void branchIfDirty(CodeBuilder b, boolean includePk, Label label) {
        int count = mAllProperties.size();
        int ordinal = 0;
        int andMask = 0;
        boolean anyNonDerived = false;

        for (StorableProperty property : mAllProperties.values()) {
            if (!property.isDerived()) {
                anyNonDerived = true;
                if (!property.isJoin() && (!property.isPrimaryKeyMember() || includePk)) {
                    // Logical 'and' will convert state 1 (clean) to state 0, so
                    // that it will be ignored. State 3 (dirty) is what we're
                    // looking for, and it turns into 2. Essentially, we leave the
                    // high order bit on, since there is no state which has the
                    // high order bit on unless the low order bit is also on.
                    andMask |= 2 << ((ordinal & 0xf) * 2);
                }
            }

            ordinal++;
            if ((ordinal & 0xf) == 0 || ordinal >= count) {
                if (anyNonDerived) {
                    String stateFieldName = PROPERTY_STATE_FIELD_NAME + ((ordinal - 1) >> 4);
                    b.loadThis();
                    b.loadField(stateFieldName, TypeDesc.INT);
                    b.loadConstant(andMask);
                    b.math(Opcode.IAND);
                    // At least one property is dirty, so short circuit.
                    b.ifZeroComparisonBranch(label, "!=");
                }

                andMask = 0;
                anyNonDerived = false;
            }
        }
    }

    private void addIsInitializedMethod
        (String name, Map<String, ? extends StorableProperty<S>> properties)
    {
        // Don't check Automatic, Independent, or Version properties.
        {
            boolean cloned = false;
            for (StorableProperty<S> prop : properties.values()) {
                if (prop.isAutomatic() || prop.isIndependent() || prop.isVersion()) {
                    if (!cloned) {
                        properties = new LinkedHashMap<String, StorableProperty<S>>(properties);
                        cloned = true;
                    }
                    // This isn't concurrent modification since the loop is
                    // still operating on the original properties map.
                    properties.remove(prop.getName());
                }
            }
        }

        MethodInfo mi = mClassFile.addMethod(Modifiers.PROTECTED, name, TypeDesc.BOOLEAN, null);
        CodeBuilder b = new CodeBuilder(mi);

        if (properties.size() == 0) {
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);
            return;
        }

        if (properties.size() == 1) {
            int ordinal = properties.values().iterator().next().getNumber();
            b.loadThis();
            b.loadField(PROPERTY_STATE_FIELD_NAME + (ordinal >> 4), TypeDesc.INT);
            b.loadConstant(PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2));
            b.math(Opcode.IAND);
            // zero == false, not zero == true
            b.returnValue(TypeDesc.BOOLEAN);
            return;
        }

        // Multiple properties is a bit more tricky. The goal here is to
        // minimize the amount of work that needs to be done at runtime.

        int ordinal = 0;
        int mask = 0;
        boolean anyNonDerived = false;

        for (StorableProperty property : mAllProperties.values()) {
            if (!property.isDerived()) {
                anyNonDerived = true;
                if (properties.containsKey(property.getName())) {
                    mask |= PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2);
                }
            }

            ordinal++;
            if ((ordinal & 0xf) == 0 || ordinal >= mAllProperties.size()) {
                if (anyNonDerived && mask != 0) {
                    // This is a great trick to convert all states of value 1
                    // (clean) into value 3 (dirty). States 0, 2, and 3 stay the
                    // same. Since joins cannot have state 1, they aren't affected.
                    // stateField | ((stateField & 0x55555555) << 1);

                    b.loadThis();
                    b.loadField(PROPERTY_STATE_FIELD_NAME + ((ordinal - 1) >> 4), TypeDesc.INT);
                    b.dup(); // [this.stateField, this.stateField
                    b.loadConstant(0x55555555);
                    b.math(Opcode.IAND); // [this.stateField, this.stateField & 0x55555555
                    b.loadConstant(1);
                    b.math(Opcode.ISHL); // [this.stateField, orMaskValue
                    b.math(Opcode.IOR);  // [newStateFieldValue

                    // Flip all bits for property states. If final result is
                    // non-zero, then there were uninitialized properties.

                    b.loadConstant(mask);
                    b.math(Opcode.IXOR);
                    if (mask != 0xffffffff) {
                        b.loadConstant(mask);
                        b.math(Opcode.IAND);
                    }

                    Label cont = b.createLabel();
                    b.ifZeroComparisonBranch(cont, "==");
                    b.loadConstant(false);
                    b.returnValue(TypeDesc.BOOLEAN);
                    cont.setLocation();
                }

                mask = 0;
                anyNonDerived = false;
            }
        }

        b.loadConstant(true);
        b.returnValue(TypeDesc.BOOLEAN);
    }

    /**
     * Generates code that verifies that all primary keys are initialized.
     *
     * @param b builder that will invoke generated method
     * @param methodName name to give to generated method
     */
    private void requirePkInitialized(CodeBuilder b, String methodName) {
        // Add code to call method which we are about to define.
        b.loadThis();
        b.invokeVirtual(methodName, null, null);

        // Now define new method, discarding original builder object.
        b = new CodeBuilder(mClassFile.addMethod(Modifiers.PROTECTED, methodName, null, null));
        b.loadThis();
        b.invokeVirtual(IS_PK_INITIALIZED_METHOD_NAME, TypeDesc.BOOLEAN, null);
        Label pkInitialized = b.createLabel();
        b.ifZeroComparisonBranch(pkInitialized, "!=");
        CodeBuilderUtil.throwException
            (b, IllegalStateException.class, "Primary key not fully specified");
        pkInitialized.setLocation();
        b.returnVoid();
    }

    /**
     * Generates a private method which accepts a property name and returns
     * PROPERTY_STATE_UNINITIALIZED, PROPERTY_STATE_DIRTY, or
     * PROPERTY_STATE_CLEAN.
     */
    private void addPropertyStateExtractMethod() {
        MethodInfo mi = mClassFile.addMethod(Modifiers.PRIVATE, PROPERTY_STATE_EXTRACT_METHOD_NAME,
                                             TypeDesc.INT, new TypeDesc[] {TypeDesc.STRING});

        addPropertySwitch(new CodeBuilder(mi), SWITCH_FOR_STATE);
    }

    private void addPropertySwitch(CodeBuilder b, int switchFor) {
        // Generate big switch statement that operates on Strings. See also
        // org.cojen.util.BeanPropertyAccessor, which also generates this kind of
        // switch.

        // For switch case count, obtain a prime number, at least twice as
        // large as needed. This should minimize hash collisions. Since all the
        // hash keys are known up front, the capacity could be tweaked until
        // there are no collisions, but this technique is easier and
        // deterministic.

        int caseCount;
        {
            BigInteger capacity = BigInteger.valueOf(mAllProperties.size() * 2 + 1);
            while (!capacity.isProbablePrime(100)) {
                capacity = capacity.add(BigInteger.valueOf(2));
            }
            caseCount = capacity.intValue();
        }

        int[] cases = new int[caseCount];
        for (int i=0; i<caseCount; i++) {
            cases[i] = i;
        }

        Label[] switchLabels = new Label[caseCount];
        Label noMatch = b.createLabel();
        List<StorableProperty<?>>[] caseMatches = caseMatches(caseCount);

        for (int i=0; i<caseCount; i++) {
            List<?> matches = caseMatches[i];
            if (matches == null || matches.size() == 0) {
                switchLabels[i] = noMatch;
            } else {
                switchLabels[i] = b.createLabel();
            }
        }

        b.loadLocal(b.getParameter(0));
        b.invokeVirtual(String.class.getName(), "hashCode", TypeDesc.INT, null);
        b.loadConstant(0x7fffffff);
        b.math(Opcode.IAND);
        b.loadConstant(caseCount);
        b.math(Opcode.IREM);

        b.switchBranch(cases, switchLabels, noMatch);

        // Params to invoke String.equals.
        TypeDesc[] params = {TypeDesc.OBJECT};

        Label derivedMatch = null;
        Label joinMatch = null;
        Label unreadable = null;
        Label unwritable = null;
        Label readException = null;
        Label writeException = null;

        for (int i=0; i<caseCount; i++) {
            List<StorableProperty<?>> matches = caseMatches[i];
            if (matches == null || matches.size() == 0) {
                continue;
            }

            switchLabels[i].setLocation();

            int matchCount = matches.size();
            for (int j=0; j<matchCount; j++) {
                StorableProperty<?> prop = matches.get(j);

                // Test against name to find exact match.

                b.loadConstant(prop.getName());
                b.loadLocal(b.getParameter(0));
                b.invokeVirtual(String.class.getName(), "equals", TypeDesc.BOOLEAN, params);

                Label notEqual;

                if (j == matchCount - 1) {
                    notEqual = null;
                    b.ifZeroComparisonBranch(noMatch, "==");
                } else {
                    notEqual = b.createLabel();
                    b.ifZeroComparisonBranch(notEqual, "==");
                }

                if (switchFor == SWITCH_FOR_STATE) {
                    if (prop.isDerived()) {
                        if (derivedMatch == null) {
                            derivedMatch = b.createLabel();
                        }
                        b.branch(derivedMatch);
                    } else if (prop.isJoin()) {
                        if (joinMatch == null) {
                            joinMatch = b.createLabel();
                        }
                        b.branch(joinMatch);
                    } else {
                        int ordinal = prop.getNumber();
                        b.loadThis();
                        b.loadField(PROPERTY_STATE_FIELD_NAME + (ordinal >> 4), TypeDesc.INT);
                        int shift = (ordinal & 0xf) * 2;
                        if (shift != 0) {
                            b.loadConstant(shift);
                            b.math(Opcode.ISHR);
                        }
                        b.loadConstant(PROPERTY_STATE_MASK);
                        b.math(Opcode.IAND);
                        b.returnValue(TypeDesc.INT);
                    }
                } else if (switchFor == SWITCH_FOR_GET) {
                    if (prop.getReadMethod() == null) {
                        if (unreadable == null) {
                            unreadable = b.createLabel();
                        }
                        b.branch(unreadable);
                    } else if (throwsCheckedException(prop.getReadMethod())) {
                        if (readException == null) {
                            readException = b.createLabel();
                        }
                        b.branch(readException);
                    } else {
                        b.loadThis();
                        b.invoke(prop.getReadMethod());
                        TypeDesc type = TypeDesc.forClass(prop.getType());
                        b.convert(type, type.toObjectType());
                        b.returnValue(TypeDesc.OBJECT);
                    }
                } else if (switchFor == SWITCH_FOR_SET) {
                    if (prop.getWriteMethod() == null) {
                        if (unwritable == null) {
                            unwritable = b.createLabel();
                        }
                        b.branch(unwritable);
                    } else if (throwsCheckedException(prop.getWriteMethod())) {
                        if (writeException == null) {
                            writeException = b.createLabel();
                        }
                        b.branch(writeException);
                    } else {
                        b.loadThis();
                        b.loadLocal(b.getParameter(1));
                        TypeDesc type = TypeDesc.forClass(prop.getType());
                        b.checkCast(type.toObjectType());
                        b.convert(type.toObjectType(), type);
                        b.invoke(prop.getWriteMethod());
                        b.returnVoid();
                    }
                }

                if (notEqual != null) {
                    notEqual.setLocation();
                }
            }
        }

        noMatch.setLocation();
        throwIllegalArgException(b, "Unknown property: ", b.getParameter(0));

        if (derivedMatch != null) {
            derivedMatch.setLocation();
            throwIllegalArgException
                (b, "Cannot get state for derived property: ", b.getParameter(0));
        }

        if (joinMatch != null) {
            joinMatch.setLocation();
            throwIllegalArgException(b, "Cannot get state for join property: ", b.getParameter(0));
        }

        if (unreadable != null) {
            unreadable.setLocation();
            throwIllegalArgException(b, "No accessor method for property: ", b.getParameter(0));
        }

        if (unwritable != null) {
            unwritable.setLocation();
            throwIllegalArgException(b, "No mutator method for property: ", b.getParameter(0));
        }

        if (readException != null) {
            readException.setLocation();
            throwIllegalArgException(b, "Accessor method declares throwing a checked exception: ",
                                     b.getParameter(0));
        }

        if (writeException != null) {
            writeException.setLocation();
            throwIllegalArgException(b, "Mutator method declares throwing a checked exception: ",
                                     b.getParameter(0));
        }
    }

    private static boolean throwsCheckedException(Method method) {
        Class<?>[] exceptionTypes = method.getExceptionTypes();
        if (exceptionTypes == null) {
            return false;
        }

        for (Class<?> exceptionType : exceptionTypes) {
            if (RuntimeException.class.isAssignableFrom(exceptionType)) {
                continue;
            }
            if (Error.class.isAssignableFrom(exceptionType)) {
                continue;
            }
            return true;
        }

        return false;
    }

    private static void throwIllegalArgException(CodeBuilder b, String message,
                                                 LocalVariable concatStr)
    {
        TypeDesc exceptionType = TypeDesc.forClass(IllegalArgumentException.class);
        TypeDesc[] params = {TypeDesc.STRING};

        b.newObject(exceptionType);
        b.dup();
        b.loadConstant(message);
        b.loadLocal(concatStr);
        b.invokeVirtual(TypeDesc.STRING, "concat", TypeDesc.STRING, params);
        b.invokeConstructor(exceptionType, params);
        b.throwObject();
    }

    /**
     * Returns the properties that match on a given case. The array length is
     * the same as the case count. Each list represents the matches. The lists
     * themselves may be null if no matches for that case.
     */
    private List<StorableProperty<?>>[] caseMatches(int caseCount) {
        List<StorableProperty<?>>[] cases = new List[caseCount];

        for (StorableProperty<?> prop : mAllProperties.values()) {
            int hashCode = prop.getName().hashCode();
            int caseValue = (hashCode & 0x7fffffff) % caseCount;
            List matches = cases[caseValue];
            if (matches == null) {
                matches = cases[caseValue] = new ArrayList<StorableProperty<?>>();
            }
            matches.add(prop);
        }

        return cases;
    }

    /**
     * Generates public method which accepts a property name and returns a
     * boolean true, if the given state matches the property's actual state.
     *
     * @param name name of method
     * @param state property state to check
     */
    private void addPropertyStateCheckMethod(String name, int state) {
        MethodInfo mi = addMethodIfNotFinal(Modifiers.PUBLIC, name,
                                            TypeDesc.BOOLEAN, new TypeDesc[] {TypeDesc.STRING});

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        // Call private method to extract state and compare.
        b.loadThis();
        b.loadLocal(b.getParameter(0));
        b.invokePrivate(PROPERTY_STATE_EXTRACT_METHOD_NAME,
                        TypeDesc.INT, new TypeDesc[] {TypeDesc.STRING});
        Label isFalse = b.createLabel();
        if (state == 0) {
            b.ifZeroComparisonBranch(isFalse, "!=");
        } else {
            b.loadConstant(state);
            b.ifComparisonBranch(isFalse, "!=");
        }
        b.loadConstant(true);
        b.returnValue(TypeDesc.BOOLEAN);
        isFalse.setLocation();
        b.loadConstant(false);
        b.returnValue(TypeDesc.BOOLEAN);
    }

    private void addGetPropertyValueMethod() {
        MethodInfo mi = addMethodIfNotFinal(Modifiers.PUBLIC, GET_PROPERTY_VALUE,
                                            TypeDesc.OBJECT, new TypeDesc[] {TypeDesc.STRING});

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        addPropertySwitch(b, SWITCH_FOR_GET);
    }

    private void addSetPropertyValueMethod() {
        MethodInfo mi = addMethodIfNotFinal(Modifiers.PUBLIC, SET_PROPERTY_VALUE, null,
                                            new TypeDesc[] {TypeDesc.STRING, TypeDesc.OBJECT});

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        addPropertySwitch(b, SWITCH_FOR_SET);
    }

    private void addPropertyMapMethod() {
        TypeDesc mapType = TypeDesc.forClass(Map.class);

        MethodInfo mi = addMethodIfNotFinal(Modifiers.PUBLIC, PROPERTY_MAP, mapType, null);

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        TypeDesc propertyMapType = TypeDesc.forClass(StorablePropertyMap.class);

        b.loadConstant(TypeDesc.forClass(mStorableType));
        b.loadThis();
        b.invokeStatic(propertyMapType, "createMap", propertyMapType,
                       new TypeDesc[] {TypeDesc.forClass(Class.class),
                                       TypeDesc.forClass(Storable.class)});
        b.returnValue(mapType);
    }

    private void addWriteToMethod() {
        TypeDesc streamType = TypeDesc.forClass(OutputStream.class);

        MethodInfo mi = addMethodIfNotFinal(Modifiers.PUBLIC.toSynchronized(true), WRITE_TO, null,
                                            new TypeDesc[] {streamType});

        if (mi == null) {
            return;
        }

        GenericEncodingStrategy<S> encoder = new GenericEncodingStrategy<S>(mStorableType, null);

        CodeBuilder b = new CodeBuilder(mi);

        LocalVariable encodedVar;
        try {
            encodedVar = encoder.buildSerialEncoding(b, null);
        } catch (SupportException e) {
            // Wipe out any code generated so far.
            b = new CodeBuilder(mi);
            CodeBuilderUtil.throwException(b, SupportException.class, e.getMessage());
            return;
        }

        b.loadLocal(encodedVar);
        b.arrayLength();
        b.loadLocal(b.getParameter(0));
        b.invokeStatic(TypeDesc.forClass(DataEncoder.class), "writeLength", TypeDesc.INT,
                       new TypeDesc[] {TypeDesc.INT, streamType});
        b.pop();

        b.loadLocal(b.getParameter(0));
        b.loadLocal(encodedVar);
        b.invokeVirtual(streamType, "write", null, new TypeDesc[] {encodedVar.getType()});
        b.returnVoid();
    }

    private void addReadFromMethod() {
        TypeDesc streamType = TypeDesc.forClass(InputStream.class);

        MethodInfo mi = addMethodIfNotFinal(Modifiers.PUBLIC.toSynchronized(true), READ_FROM, null,
                                            new TypeDesc[] {streamType});

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        TypeDesc dataDecoderType = TypeDesc.forClass(DataDecoder.class);

        b.loadLocal(b.getParameter(0));
        b.invokeStatic(dataDecoderType, "readLength", TypeDesc.INT, new TypeDesc[] {streamType});

        LocalVariable encodedVar = b.createLocalVariable(null, TypeDesc.forClass(byte[].class));
        b.newObject(encodedVar.getType());
        b.storeLocal(encodedVar);

        b.loadLocal(b.getParameter(0));
        b.loadLocal(encodedVar);
        b.invokeStatic(dataDecoderType, "readFully", null,
                       new TypeDesc[] {streamType, encodedVar.getType()});

        GenericEncodingStrategy<S> encoder = new GenericEncodingStrategy<S>(mStorableType, null);

        try {
            encoder.buildSerialDecoding(b, null, encodedVar);
        } catch (SupportException e) {
            // Wipe out any code generated so far.
            b = new CodeBuilder(mi);
            CodeBuilderUtil.throwException(b, SupportException.class, e.getMessage());
            return;
        }

        b.returnVoid();
    }

    /**
     * Defines a hashCode method.
     */
    private void addHashCodeMethod() {
        Modifiers modifiers = Modifiers.PUBLIC.toSynchronized(true);
        MethodInfo mi = addMethodIfNotFinal(modifiers, "hashCode", TypeDesc.INT, null);

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        boolean mixIn = false;
        for (StorableProperty property : mAllProperties.values()) {
            if (property.isDerived() || property.isJoin()) {
                continue;
            }
            addHashCodeCall(b, property.getName(),
                            TypeDesc.forClass(property.getType()), true, mixIn);
            mixIn = true;
        }

        b.returnValue(TypeDesc.INT);
    }

    private void addHashCodeCall(CodeBuilder b, String fieldName,
                                 TypeDesc fieldType, boolean testForNull,
                                 boolean mixIn)
    {
        if (mixIn) {
            // Multiply current hashcode by 31 before adding more to it.
            b.loadConstant(5);
            b.math(Opcode.ISHL);
            b.loadConstant(1);
            b.math(Opcode.ISUB);
        }

        b.loadThis();
        b.loadField(fieldName, fieldType);

        switch (fieldType.getTypeCode()) {
        case TypeDesc.FLOAT_CODE:
            b.invokeStatic(TypeDesc.FLOAT.toObjectType(), "floatToIntBits",
                           TypeDesc.INT, new TypeDesc[]{TypeDesc.FLOAT});
            // Fall through
        case TypeDesc.INT_CODE:
        case TypeDesc.CHAR_CODE:
        case TypeDesc.SHORT_CODE:
        case TypeDesc.BYTE_CODE:
        case TypeDesc.BOOLEAN_CODE:
            if (mixIn) {
                b.math(Opcode.IADD);
            }
            break;

        case TypeDesc.DOUBLE_CODE:
            b.invokeStatic(TypeDesc.DOUBLE.toObjectType(), "doubleToLongBits",
                           TypeDesc.LONG, new TypeDesc[]{TypeDesc.DOUBLE});
            // Fall through
        case TypeDesc.LONG_CODE:
            b.dup2();
            b.loadConstant(32);
            b.math(Opcode.LUSHR);
            b.math(Opcode.LXOR);
            b.convert(TypeDesc.LONG, TypeDesc.INT);
            if (mixIn) {
                b.math(Opcode.IADD);
            }
            break;

        case TypeDesc.OBJECT_CODE:
        default:
            LocalVariable value = null;
            if (testForNull) {
                value = b.createLocalVariable(null, fieldType);
                b.storeLocal(value);
                b.loadLocal(value);
            }
            if (mixIn) {
                Label isNull = b.createLabel();
                if (testForNull) {
                    b.ifNullBranch(isNull, true);
                    b.loadLocal(value);
                }
                addHashCodeCallTo(b, fieldType);
                b.math(Opcode.IADD);
                if (testForNull) {
                    isNull.setLocation();
                }
            } else {
                Label cont = b.createLabel();
                if (testForNull) {
                    Label notNull = b.createLabel();
                    b.ifNullBranch(notNull, false);
                    b.loadConstant(0);
                    b.branch(cont);
                    notNull.setLocation();
                    b.loadLocal(value);
                }
                addHashCodeCallTo(b, fieldType);
                if (testForNull) {
                    cont.setLocation();
                }
            }
            break;
        }
    }

    private void addHashCodeCallTo(CodeBuilder b, TypeDesc fieldType) {
        if (fieldType.isArray()) {
            if (!fieldType.getComponentType().isPrimitive()) {
                b.invokeStatic("java.util.Arrays", "deepHashCode",
                               TypeDesc.INT, new TypeDesc[] {TypeDesc.forClass(Object[].class)});
            } else {
                b.invokeStatic("java.util.Arrays", "hashCode",
                               TypeDesc.INT, new TypeDesc[] {fieldType});
            }
        } else {
            b.invokeVirtual(TypeDesc.OBJECT, "hashCode", TypeDesc.INT, null);
        }
    }

    /**
     * Defines an equals method.
     *
     * @param equalityType Type of equality to define - {@link EQUAL_KEYS} for "equalKeys",
     * {@link EQUAL_PROPERTIES} for "equalProperties", and {@link EQUAL_FULL} for "equals"
     */
    private void addEqualsMethod(int equalityType) {
        TypeDesc[] objectParam = {TypeDesc.OBJECT};

        String equalsMethodName;
        switch (equalityType) {
        default:
            throw new IllegalArgumentException();
        case EQUAL_KEYS:
            equalsMethodName = EQUAL_PRIMARY_KEYS_METHOD_NAME;
            break;
        case EQUAL_PROPERTIES:
            equalsMethodName = EQUAL_PROPERTIES_METHOD_NAME;
            break;
        case EQUAL_FULL:
            equalsMethodName = EQUALS_METHOD_NAME;
        }

        Modifiers modifiers = Modifiers.PUBLIC.toSynchronized(true);
        MethodInfo mi = addMethodIfNotFinal
            (modifiers, equalsMethodName, TypeDesc.BOOLEAN, objectParam);

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);

        // if (this == target) return true;
        b.loadThis();
        b.loadLocal(b.getParameter(0));
        Label notEqual = b.createLabel();
        b.ifEqualBranch(notEqual, false);
        b.loadConstant(true);
        b.returnValue(TypeDesc.BOOLEAN);
        notEqual.setLocation();

        // if (! target instanceof this) return false;
        TypeDesc userStorableTypeDesc = TypeDesc.forClass(mStorableType);
        b.loadLocal(b.getParameter(0));
        b.instanceOf(userStorableTypeDesc);
        Label fail = b.createLabel();
        b.ifZeroComparisonBranch(fail, "==");

        // this.class other = (this.class)target;
        LocalVariable other = b.createLocalVariable(null, userStorableTypeDesc);
        b.loadLocal(b.getParameter(0));
        b.checkCast(userStorableTypeDesc);
        b.storeLocal(other);

        for (StorableProperty property : mAllProperties.values()) {
            if (property.isDerived() || property.isJoin()) {
                continue;
            }
            // If we're only comparing keys, and this isn't a key, skip it
            if ((equalityType == EQUAL_KEYS) && !property.isPrimaryKeyMember()) {
                continue;
            }

            // Check if independent property is supported, and skip if not.
            Label skipCheck = b.createLabel();
            if (equalityType != EQUAL_KEYS && property.isIndependent()) {
                addSkipIndependent(b, other, property, skipCheck);
            }

            TypeDesc fieldType = TypeDesc.forClass(property.getType());
            loadThisProperty(b, property);

            b.loadLocal(other);
            b.invoke(property.getReadMethod());
            CodeBuilderUtil.addValuesEqualCall(b, fieldType, true, fail, false);

            skipCheck.setLocation();
        }

        b.loadConstant(true);
        b.returnValue(TypeDesc.BOOLEAN);

        fail.setLocation();
        b.loadConstant(false);
        b.returnValue(TypeDesc.BOOLEAN);
    }

    /**
     * Defines a toString method, which assumes that the ClassFile is targeting
     * version 1.5 of Java.
     *
     * @param keyOnly when true, generate a toStringKeyOnly method instead
     */
    private void addToStringMethod(boolean keyOnly) {
        TypeDesc stringBuilder = TypeDesc.forClass(StringBuilder.class);

        Modifiers modifiers = Modifiers.PUBLIC.toSynchronized(true);
        MethodInfo mi = addMethodIfNotFinal(modifiers,
                                            keyOnly ?
                                            TO_STRING_KEY_ONLY_METHOD_NAME :
                                            TO_STRING_METHOD_NAME,
                                            TypeDesc.STRING, null);

        if (mi == null) {
            return;
        }

        CodeBuilder b = new CodeBuilder(mi);
        b.newObject(stringBuilder);
        b.dup();
        b.invokeConstructor(stringBuilder, null);
        b.loadConstant(mStorableType.getName());
        invokeAppend(b, TypeDesc.STRING);

        String detail;
        if (keyOnly) {
            detail = " (key only) {";
        } else {
            detail = " {";
        }

        b.loadConstant(detail);
        invokeAppend(b, TypeDesc.STRING);

        // First pass, just print primary keys.

        LocalVariable commaCountVar = b.createLocalVariable(null, TypeDesc.INT);
        b.loadConstant(-1);
        b.storeLocal(commaCountVar);

        for (StorableProperty property : mInfo.getPrimaryKeyProperties().values()) {
            addPropertyAppendCall(b, property, commaCountVar);
        }

        // Second pass, print non-primary keys.

        if (!keyOnly) {
            for (StorableProperty property : mAllProperties.values()) {
                // Don't print any derived or join properties since they may throw an exception.
                if (!property.isPrimaryKeyMember() &&
                    (!property.isDerived()) && (!property.isJoin()))
                {
                    addPropertyAppendCall(b, property, commaCountVar);
                }
            }
        }

        b.loadConstant('}');
        invokeAppend(b, TypeDesc.CHAR);

        // For key string, also show all the alternate keys. This makes the
        // FetchNoneException message more helpful.
        if (keyOnly) {
            int altKeyCount = mInfo.getAlternateKeyCount();
            for (int i=0; i<altKeyCount; i++) {
                b.loadConstant(-1);
                b.storeLocal(commaCountVar);

                b.loadConstant(", {");
                invokeAppend(b, TypeDesc.STRING);

                StorableKey<S> key = mInfo.getAlternateKey(i);

                for (OrderedProperty<S> op : key.getProperties()) {
                    StorableProperty<S> property = op.getChainedProperty().getPrimeProperty();
                    addPropertyAppendCall(b, property, commaCountVar);
                }

                b.loadConstant('}');
                invokeAppend(b, TypeDesc.CHAR);
            }
        }

        b.invokeVirtual(stringBuilder, TO_STRING_METHOD_NAME, TypeDesc.STRING, null);
        b.returnValue(TypeDesc.STRING);
    }

    private void invokeAppend(CodeBuilder b, TypeDesc type) {
        TypeDesc stringBuilder = TypeDesc.forClass(StringBuilder.class);
        b.invokeVirtual(stringBuilder, "append", stringBuilder, new TypeDesc[] {type});
    }

    private void addPropertyAppendCall(CodeBuilder b,
                                       StorableProperty property,
                                       LocalVariable commaCountVar)
    {
        Label skipPrint = b.createLabel();

        // Check if independent property is supported, and skip if not.
        if (property.isIndependent()) {
            addSkipIndependent(b, null, property, skipPrint);
        }

        int ordinal = property.getNumber();

        // Check if property is initialized, and skip if not.
        b.loadThis();
        b.loadField(PROPERTY_STATE_FIELD_NAME + (ordinal >> 4), TypeDesc.INT);
        b.loadConstant(PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2));
        b.math(Opcode.IAND);
        b.ifZeroComparisonBranch(skipPrint, "==");

        b.integerIncrement(commaCountVar, 1);
        b.loadLocal(commaCountVar);
        Label noComma = b.createLabel();
        b.ifZeroComparisonBranch(noComma, "==");
        b.loadConstant(", ");
        invokeAppend(b, TypeDesc.STRING);
        noComma.setLocation();

        addPropertyAppendCall(b, property);

        skipPrint.setLocation();
    }

    private void addPropertyAppendCall(CodeBuilder b, StorableProperty property) {
        b.loadConstant(property.getName());
        invokeAppend(b, TypeDesc.STRING);
        b.loadConstant('=');
        invokeAppend(b, TypeDesc.CHAR);
        loadThisProperty(b, property);
        TypeDesc type = TypeDesc.forClass(property.getType());
        if (type.isPrimitive()) {
            if (type == TypeDesc.BYTE || type == TypeDesc.SHORT) {
                type = TypeDesc.INT;
            }
        } else {
            if (type != TypeDesc.STRING) {
                if (type.isArray()) {
                    if (!type.getComponentType().isPrimitive()) {
                        b.invokeStatic("java.util.Arrays", "deepToString",
                                       TypeDesc.STRING,
                                       new TypeDesc[] {TypeDesc.OBJECT.toArrayType()});
                    } else {
                        b.invokeStatic("java.util.Arrays", TO_STRING_METHOD_NAME,
                                       TypeDesc.STRING, new TypeDesc[] {type});
                    }
                }
                type = TypeDesc.OBJECT;
            }
        }
        invokeAppend(b, type);
    }

    /**
     * Generates code to get a trigger, forcing a transaction if trigger is not
     * null. Also, if there is a trigger, the "before" method is called.
     *
     * @param opType type of operation, Insert, Update, or Delete
     * @param forTryVar optional boolean variable for selecting whether to call
     * "before" or "beforeTry" method
     * @param forTry used if forTryVar is null
     * @param triggerVar required variable of type Trigger for storing trigger
     * @param txnVar required variable of type Transaction for storing transaction
     * @param stateVar variable of type Object for storing state
     * @return try start label for transaction
     */
    private Label addGetTriggerAndEnterTxn(CodeBuilder b,
                                           String opType,
                                           LocalVariable forTryVar,
                                           boolean forTry,
                                           LocalVariable triggerVar,
                                           LocalVariable txnVar,
                                           LocalVariable stateVar)
    {
        // trigger = support$.getXxxTrigger();
        b.loadThis();
        b.loadField(SUPPORT_FIELD_NAME, mSupportType);
        Method m = lookupMethod(mSupportType.toClass(), "get" + opType + "Trigger");
        b.invoke(m);
        b.storeLocal(triggerVar);
        // state = null;
        b.loadNull();
        b.storeLocal(stateVar);

        // if (trigger == null) {
        //     txn = null;
        // } else {
        //     txn = support.getRootRepository().enterTransaction();
        //   tryStart:
        //     if (forTry) {
        //         state = trigger.beforeTryXxx(this);
        //     } else {
        //         state = trigger.beforeXxx(this);
        //     }
        // }
        b.loadLocal(triggerVar);
        Label hasTrigger = b.createLabel();
        b.ifNullBranch(hasTrigger, false);

        // txn = null
        b.loadNull();
        b.storeLocal(txnVar);
        Label cont = b.createLabel();
        b.branch(cont);

        hasTrigger.setLocation();

        // txn = support.getRootRepository().enterTransaction();
        TypeDesc repositoryType = TypeDesc.forClass(Repository.class);
        TypeDesc transactionType = TypeDesc.forClass(Transaction.class);
        b.loadThis();
        b.loadField(SUPPORT_FIELD_NAME, mSupportType);
        b.invokeInterface(mSupportType, "getRootRepository", repositoryType, null);
        b.invokeInterface(repositoryType, ENTER_TRANSACTION_METHOD_NAME, transactionType, null);
        b.storeLocal(txnVar);

        Label tryStart = b.createLabel().setLocation();

        // if (forTry) {
        //     state = trigger.beforeTryXxx(this);
        // } else {
        //     state = trigger.beforeXxx(this);
        // }
        b.loadLocal(triggerVar);
        b.loadThis();

        if (forTryVar == null) {
            if (forTry) {
                b.invokeVirtual(triggerVar.getType(), "beforeTry" + opType,
                                TypeDesc.OBJECT, new TypeDesc[] {TypeDesc.OBJECT});
            } else {
                b.invokeVirtual(triggerVar.getType(), "before" + opType,
                                TypeDesc.OBJECT, new TypeDesc[] {TypeDesc.OBJECT});
            }
            b.storeLocal(stateVar);
        } else {
            b.loadLocal(forTryVar);
            Label isForTry = b.createLabel();

            b.ifZeroComparisonBranch(isForTry, "!=");
            b.invokeVirtual(triggerVar.getType(), "before" + opType,
                            TypeDesc.OBJECT, new TypeDesc[] {TypeDesc.OBJECT});
            b.storeLocal(stateVar);
            b.branch(cont);

            isForTry.setLocation();
            b.invokeVirtual(triggerVar.getType(), "beforeTry" + opType,
                            TypeDesc.OBJECT, new TypeDesc[] {TypeDesc.OBJECT});
            b.storeLocal(stateVar);
        }

        cont.setLocation();

        return tryStart;
    }

    /**
     * Generates code to call a trigger after the persistence operation has
     * been invoked.
     *
     * @param opType type of operation, Insert, Update, or Delete
     * @param forTryVar optional boolean variable for selecting whether to call
     * "after" or "afterTry" method
     * @param forTry used if forTryVar is null
     * @param triggerVar required variable of type Trigger for retrieving trigger
     * @param txnVar required variable of type Transaction for storing transaction
     * @param stateVar required variable of type Object for retrieving state
     */
    private void addTriggerAfterAndExitTxn(CodeBuilder b,
                                           String opType,
                                           LocalVariable forTryVar,
                                           boolean forTry,
                                           LocalVariable triggerVar,
                                           LocalVariable txnVar,
                                           LocalVariable stateVar)
    {
        // if (trigger != null) {
        b.loadLocal(triggerVar);
        Label cont = b.createLabel();
        b.ifNullBranch(cont, true);

        // if (forTry) {
        //     trigger.afterTryXxx(this, state);
        // } else {
        //     trigger.afterXxx(this, state);
        // }
        b.loadLocal(triggerVar);
        b.loadThis();
        b.loadLocal(stateVar);

        if (forTryVar == null) {
            if (forTry) {
                b.invokeVirtual(TypeDesc.forClass(Trigger.class), "afterTry" + opType, null,
                                new TypeDesc[] {TypeDesc.OBJECT, TypeDesc.OBJECT});
            } else {
                b.invokeVirtual(TypeDesc.forClass(Trigger.class), "after" + opType, null,
                                new TypeDesc[] {TypeDesc.OBJECT, TypeDesc.OBJECT});
            }
        } else {
            b.loadLocal(forTryVar);
            Label isForTry = b.createLabel();

            b.ifZeroComparisonBranch(isForTry, "!=");
            b.invokeVirtual(TypeDesc.forClass(Trigger.class), "after" + opType, null,
                            new TypeDesc[] {TypeDesc.OBJECT, TypeDesc.OBJECT});
            Label commitAndExit = b.createLabel();
            b.branch(commitAndExit);

            isForTry.setLocation();
            b.invokeVirtual(TypeDesc.forClass(Trigger.class), "afterTry" + opType, null,
                            new TypeDesc[] {TypeDesc.OBJECT, TypeDesc.OBJECT});
            commitAndExit.setLocation();
        }

        //     txn.commit();
        //     txn.exit();
        TypeDesc transactionType = TypeDesc.forClass(Transaction.class);
        b.loadLocal(txnVar);
        b.invokeInterface(transactionType, COMMIT_METHOD_NAME, null, null);
        b.loadLocal(txnVar);
        b.invokeInterface(transactionType, EXIT_METHOD_NAME, null, null);

        cont.setLocation();
    }

    /**
     * Generates code to call a trigger after the persistence operation has
     * failed.
     *
     * @param opType type of operation, Insert, Update, or Delete
     * @param triggerVar required variable of type Trigger for retrieving trigger
     * @param txnVar required variable of type Transaction for storing transaction
     * @param stateVar required variable of type Object for retrieving state
     */
    private void addTriggerFailedAndExitTxn(CodeBuilder b,
                                            String opType,
                                            LocalVariable triggerVar,
                                            LocalVariable txnVar,
                                            LocalVariable stateVar)
    {
        TypeDesc transactionType = TypeDesc.forClass(Transaction.class);

        // if (trigger != null) {
        b.loadLocal(triggerVar);
        Label isNull = b.createLabel();
        b.ifNullBranch(isNull, true);

        //     try {
        //         trigger.failedXxx(this, state);
        //     } catch (Throwable e) {
        //         uncaught(e);
        //     }
        Label tryStart = b.createLabel().setLocation();
        b.loadLocal(triggerVar);
        b.loadThis();
        b.loadLocal(stateVar);
        b.invokeVirtual(TypeDesc.forClass(Trigger.class), "failed" + opType, null,
                        new TypeDesc[] {TypeDesc.OBJECT, TypeDesc.OBJECT});
        Label tryEnd = b.createLabel().setLocation();
        Label cont = b.createLabel();
        b.branch(cont);
        b.exceptionHandler(tryStart, tryEnd, Throwable.class.getName());
        b.invokeStatic(UNCAUGHT_METHOD_NAME, null,
                       new TypeDesc[] {TypeDesc.forClass(Throwable.class)});
        cont.setLocation();

        //     txn.exit();
        b.loadLocal(txnVar);
        b.invokeInterface(transactionType, EXIT_METHOD_NAME, null, null);

        isNull.setLocation();
    }

    /**
     * Generates exception handler code to call a trigger after the persistence
     * operation has failed.
     *
     * @param opType type of operation, Insert, Update, or Delete
     * @param forTryVar optional boolean variable for selecting whether to
     * throw or catch Trigger.Abort.
     * @param forTry used if forTryVar is null
     * @param triggerVar required variable of type Trigger for retrieving trigger
     * @param txnVar required variable of type Transaction for storing transaction
     * @param stateVar required variable of type Object for retrieving state
     * @param tryStart start of exception handler around transaction
     */
    private void addTriggerFailedAndExitTxn(CodeBuilder b,
                                            String opType,
                                            LocalVariable forTryVar,
                                            boolean forTry,
                                            LocalVariable triggerVar,
                                            LocalVariable txnVar,
                                            LocalVariable stateVar,
                                            Label tryStart)
    {
        if (tryStart == null) {
            addTriggerFailedAndExitTxn(b, opType, triggerVar, txnVar, stateVar);
            return;
        }

        // } catch (... e) {
        //     if (trigger != null) {
        //         try {
        //             trigger.failedXxx(this, state);
        //         } catch (Throwable e) {
        //             uncaught(e);
        //         }
        //     }
        //     txn.exit();
        //     if (e instanceof Trigger.Abort) {
        //         if (forTryVar) {
        //             return false;
        //         } else {
        //             // Try to add some trace for context
        //             throw ((Trigger.Abort) e).withStackTrace();
        //         }
        //     }
        //     if (e instanceof RepositoryException) {
        //         throw ((RepositoryException) e).toPersistException();
        //     }
        //     throw e;
        // }

        Label tryEnd = b.createLabel().setLocation();
        b.exceptionHandler(tryStart, tryEnd, null);
        LocalVariable exceptionVar = b.createLocalVariable(null, TypeDesc.OBJECT);
        b.storeLocal(exceptionVar);

        addTriggerFailedAndExitTxn(b, opType, triggerVar, txnVar, stateVar);

        b.loadLocal(exceptionVar);
        TypeDesc abortException = TypeDesc.forClass(Trigger.Abort.class);
        b.instanceOf(abortException);
        Label nextCheck = b.createLabel();
        b.ifZeroComparisonBranch(nextCheck, "==");
        if (forTryVar == null) {
            if (forTry) {
                b.loadConstant(false);
                b.returnValue(TypeDesc.BOOLEAN);
            } else {
                b.loadLocal(exceptionVar);
                b.checkCast(abortException);
                b.invokeVirtual(abortException, "withStackTrace", abortException, null);
                b.throwObject();
            }
        } else {
            b.loadLocal(forTryVar);
            Label isForTry = b.createLabel();
            b.ifZeroComparisonBranch(isForTry, "!=");
            b.loadLocal(exceptionVar);
            b.checkCast(abortException);
            b.invokeVirtual(abortException, "withStackTrace", abortException, null);
            b.throwObject();
            isForTry.setLocation();
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);
        }

        nextCheck.setLocation();
        b.loadLocal(exceptionVar);
        TypeDesc repException = TypeDesc.forClass(RepositoryException.class);
        b.instanceOf(repException);
        Label throwAny = b.createLabel();
        b.ifZeroComparisonBranch(throwAny, "==");
        b.loadLocal(exceptionVar);
        b.checkCast(repException);
        b.invokeVirtual(repException, "toPersistException",
                        TypeDesc.forClass(PersistException.class), null);
        b.throwObject();

        throwAny.setLocation();
        b.loadLocal(exceptionVar);
        b.throwObject();
    }

    /**
     * Generates method which passes exception to uncaught exception handler.
     */
    private void defineUncaughtExceptionHandler() {
        MethodInfo mi = mClassFile.addMethod
            (Modifiers.PRIVATE.toStatic(true), UNCAUGHT_METHOD_NAME, null,
             new TypeDesc[] {TypeDesc.forClass(Throwable.class)});
        CodeBuilder b = new CodeBuilder(mi);

        // Thread t = Thread.currentThread();
        // t.getUncaughtExceptionHandler().uncaughtException(t, e);
        TypeDesc threadType = TypeDesc.forClass(Thread.class);
        b.invokeStatic(Thread.class.getName(), "currentThread", threadType, null);
        LocalVariable threadVar = b.createLocalVariable(null, threadType);
        b.storeLocal(threadVar);
        b.loadLocal(threadVar);
        TypeDesc handlerType = TypeDesc.forClass(Thread.UncaughtExceptionHandler.class);
        b.invokeVirtual(threadType, "getUncaughtExceptionHandler", handlerType, null);
        b.loadLocal(threadVar);
        b.loadLocal(b.getParameter(0));
        b.invokeInterface(handlerType, "uncaughtException", null,
                          new TypeDesc[] {threadType, TypeDesc.forClass(Throwable.class)});
        b.returnVoid();
    }

    /**
     * @return MethodInfo for completing definition or null if superclass
     * already implements method as final.
     */
    private MethodInfo addMethodIfNotFinal(Modifiers modifiers, String name,
                                           TypeDesc retType, TypeDesc[] params)
    {
        if (CodeBuilderUtil.isPublicMethodFinal(mStorableType, name, retType, params)) {
            return null;
        }

        return mClassFile.addMethod(modifiers, name, retType, params);
    }
}
