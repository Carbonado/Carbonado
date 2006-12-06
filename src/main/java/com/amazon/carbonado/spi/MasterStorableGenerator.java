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

package com.amazon.carbonado.spi;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.Opcode;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.KeyFactory;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.ConstraintException;
import com.amazon.carbonado.OptimisticLockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import static com.amazon.carbonado.spi.CommonMethodNames.*;

/**
 * Generates and caches abstract implementations of {@link Storable} types
 * suitable for use by master repositories. The generated classes extend those
 * generated by {@link StorableGenerator}. Subclasses need not worry about
 * transactions since this class takes care of that.
 *
 * @author Brian S O'Neill
 */
public final class MasterStorableGenerator<S extends Storable> {
    // Note: All generated fields/methods have a "$" character in them to
    // prevent name collisions with any inherited fields/methods. User storable
    // properties are defined as fields which exactly match the property
    // name. We don't want collisions with those either. Legal bean properties
    // cannot have "$" in them, so there's nothing to worry about.

    /** Name of protected abstract method in generated storable */
    public static final String
        DO_TRY_LOAD_MASTER_METHOD_NAME   = StorableGenerator.DO_TRY_LOAD_METHOD_NAME,
        DO_TRY_INSERT_MASTER_METHOD_NAME = "doTryInsert$master",
        DO_TRY_UPDATE_MASTER_METHOD_NAME = "doTryUpdate$master",
        DO_TRY_DELETE_MASTER_METHOD_NAME = "doTryDelete$master";

    private static final String APPEND_UNINIT_PROPERTY = "appendUninitializedPropertyName$";

    private static final String INSERT_OP = "Insert";
    private static final String UPDATE_OP = "Update";
    private static final String DELETE_OP = "Delete";

    // Cache of generated abstract classes.
    private static Map<Object, Class<? extends Storable>> cCache = new SoftValuedHashMap();

    /**
     * Returns an abstract implementation of the given Storable type, which
     * is fully thread-safe. The Storable type itself may be an interface or
     * a class. If it is a class, then it must not be final, and it must have a
     * public, no-arg constructor. The constructor for the returned abstract
     * class looks like this:
     *
     * <pre>
     * public &lt;init&gt;(MasterSupport);
     * </pre>
     *
     * Subclasses must implement the following abstract protected methods,
     * whose exact names are defined by constants in this class:
     *
     * <pre>
     * // Load the object by examining the primary key.
     * protected abstract boolean doTryLoad() throws FetchException;
     *
     * // Insert the object into the storage layer.
     * protected abstract boolean doTryInsert_master() throws PersistException;
     *
     * // Update the object in the storage.
     * protected abstract boolean doTryUpdate_master() throws PersistException;
     *
     * // Delete the object from the storage layer by the primary key.
     * protected abstract boolean doTryDelete_master() throws PersistException;
     * </pre>
     *
     * Subclasses can access the MasterSupport instance via the protected field
     * named by {@link StorableGenerator#SUPPORT_FIELD_NAME SUPPORT_FIELD_NAME}.
     *
     * @throws com.amazon.carbonado.MalformedTypeException if Storable type is not well-formed
     * @throws IllegalArgumentException if type is null
     * @see MasterSupport
     */
    public static <S extends Storable> Class<? extends S>
        getAbstractClass(Class<S> type, EnumSet<MasterFeature> features)
        throws SupportException, IllegalArgumentException
    {
        StorableInfo<S> info = StorableIntrospector.examine(type);

        anySequences:
        if (features.contains(MasterFeature.INSERT_SEQUENCES)) {
            for (StorableProperty<S> property : info.getAllProperties().values()) {
                if (property.getSequenceName() != null) {
                    break anySequences;
                }
            }
            features.remove(MasterFeature.INSERT_SEQUENCES);
        }

        if (info.getVersionProperty() == null) {
            features.remove(MasterFeature.VERSIONING);
        }

        if (features.contains(MasterFeature.VERSIONING)) {
            // Implied feature.
            features.add(MasterFeature.UPDATE_FULL);
        }

        if (alwaysHasTxn(INSERT_OP, features)) {
            // Implied feature.
            features.add(MasterFeature.INSERT_TXN);
        }
        if (alwaysHasTxn(UPDATE_OP, features)) {
            // Implied feature.
            features.add(MasterFeature.UPDATE_TXN);
        }
        if (alwaysHasTxn(DELETE_OP, features)) {
            // Implied feature.
            features.add(MasterFeature.DELETE_TXN);
        }

        if (requiresTxnForUpdate(INSERT_OP, features)) {
            // Implied feature.
            features.add(MasterFeature.INSERT_TXN_FOR_UPDATE);
        }
        if (requiresTxnForUpdate(UPDATE_OP, features)) {
            // Implied feature.
            features.add(MasterFeature.UPDATE_TXN_FOR_UPDATE);
        }
        if (requiresTxnForUpdate(DELETE_OP, features)) {
            // Implied feature.
            features.add(MasterFeature.DELETE_TXN_FOR_UPDATE);
        }

        Object key = KeyFactory.createKey(new Object[] {type, features});

        synchronized (cCache) {
            Class<? extends S> abstractClass = (Class<? extends S>) cCache.get(key);
            if (abstractClass != null) {
                return abstractClass;
            }
            abstractClass =
                new MasterStorableGenerator<S>(type, features).generateAndInjectClass();
            cCache.put(key, abstractClass);
            return abstractClass;
        }
    }

    private final EnumSet<MasterFeature> mFeatures;
    private final StorableInfo<S> mInfo;
    private final Map<String, ? extends StorableProperty<S>> mAllProperties;

    private final ClassInjector mClassInjector;
    private final ClassFile mClassFile;

    private MasterStorableGenerator(Class<S> storableType, EnumSet<MasterFeature> features) {
        mFeatures = features;
        mInfo = StorableIntrospector.examine(storableType);
        mAllProperties = mInfo.getAllProperties();

        final Class<? extends S> abstractClass = StorableGenerator.getAbstractClass(storableType);

        mClassInjector = ClassInjector.create
            (storableType.getName(), abstractClass.getClassLoader());

        mClassFile = new ClassFile(mClassInjector.getClassName(), abstractClass);
        mClassFile.setModifiers(mClassFile.getModifiers().toAbstract(true));
        mClassFile.markSynthetic();
        mClassFile.setSourceFile(MasterStorableGenerator.class.getName());
        mClassFile.setTarget("1.5");
    }

    private Class<? extends S> generateAndInjectClass() throws SupportException {
        generateClass();
        Class abstractClass = mClassInjector.defineClass(mClassFile);
        return (Class<? extends S>) abstractClass;
    }

    private void generateClass() throws SupportException {
        // Declare some types.
        final TypeDesc storableType = TypeDesc.forClass(Storable.class);
        final TypeDesc triggerSupportType = TypeDesc.forClass(TriggerSupport.class);
        final TypeDesc masterSupportType = TypeDesc.forClass(MasterSupport.class);
        final TypeDesc transactionType = TypeDesc.forClass(Transaction.class);
        final TypeDesc optimisticLockType = TypeDesc.forClass(OptimisticLockException.class);
        final TypeDesc persistExceptionType = TypeDesc.forClass(PersistException.class);

        // Add constructor that accepts a MasterSupport.
        {
            TypeDesc[] params = {masterSupportType};
            MethodInfo mi = mClassFile.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeSuperConstructor(new TypeDesc[] {triggerSupportType});

            b.returnVoid();
        }

        // Declare protected abstract methods.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toAbstract(true),
                 DO_TRY_INSERT_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(persistExceptionType);

            mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toAbstract(true),
                 DO_TRY_UPDATE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(persistExceptionType);

            mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toAbstract(true),
                 DO_TRY_DELETE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(persistExceptionType);
        }

        // Add required protected doTryInsert method.
        {
            // If sequence support requested, implement special insert hook to
            // call sequences for properties which are UNINITIALIZED. User may
            // provide explicit values for properties with sequences.

            if (mFeatures.contains(MasterFeature.INSERT_SEQUENCES)) {
                MethodInfo mi = mClassFile.addMethod
                    (Modifiers.PROTECTED,
                     StorableGenerator.CHECK_PK_FOR_INSERT_METHOD_NAME,
                     null, null);
                CodeBuilder b = new CodeBuilder(mi);

                int ordinal = 0;
                for (StorableProperty<S> property : mAllProperties.values()) {
                    if (property.getSequenceName() != null) {
                        // Check the state of this property, to see if it is
                        // uninitialized. Uninitialized state has value zero.

                        String stateFieldName =
                            StorableGenerator.PROPERTY_STATE_FIELD_NAME + (ordinal >> 4);

                        b.loadThis();
                        b.loadField(stateFieldName, TypeDesc.INT);
                        int shift = (ordinal & 0xf) * 2;
                        b.loadConstant(StorableGenerator.PROPERTY_STATE_MASK << shift);
                        b.math(Opcode.IAND);

                        Label isInitialized = b.createLabel();
                        b.ifZeroComparisonBranch(isInitialized, "!=");

                        // Load this in preparation for storing value to property.
                        b.loadThis();

                        // Call MasterSupport.getSequenceValueProducer(String).
                        TypeDesc seqValueProdType = TypeDesc.forClass(SequenceValueProducer.class);
                        b.loadThis();
                        b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, triggerSupportType);
                        b.checkCast(masterSupportType);
                        b.loadConstant(property.getSequenceName());
                        b.invokeInterface
                            (masterSupportType, "getSequenceValueProducer",
                             seqValueProdType, new TypeDesc[] {TypeDesc.STRING});

                        // Find appropriate method to call for getting next sequence value.
                        TypeDesc propertyType = TypeDesc.forClass(property.getType());
                        TypeDesc propertyObjType = propertyType.toObjectType();
                        Method method;

                        try {
                            if (propertyObjType == TypeDesc.LONG.toObjectType()) {
                                method = SequenceValueProducer.class
                                    .getMethod("nextLongValue", (Class[]) null);
                            } else if (propertyObjType == TypeDesc.INT.toObjectType()) {
                                method = SequenceValueProducer.class
                                    .getMethod("nextIntValue", (Class[]) null);
                            } else if (propertyObjType == TypeDesc.STRING) {
                                method = SequenceValueProducer.class
                                    .getMethod("nextDecimalValue", (Class[]) null);
                            } else {
                                throw new SupportException
                                    ("Unable to support sequence of type \"" +
                                     property.getType().getName() + "\" for property: " +
                                     property.getName());
                            }
                        } catch (NoSuchMethodException e) {
                            Error err = new NoSuchMethodError();
                            err.initCause(e);
                            throw err;
                        }

                        b.invoke(method);
                        b.convert(TypeDesc.forClass(method.getReturnType()), propertyType);

                        // Store property
                        b.storeField(property.getName(), propertyType);

                        // Set state to dirty.
                        b.loadThis();
                        b.loadThis();
                        b.loadField(stateFieldName, TypeDesc.INT);
                        b.loadConstant(StorableGenerator.PROPERTY_STATE_DIRTY << shift);
                        b.math(Opcode.IOR);
                        b.storeField(stateFieldName, TypeDesc.INT);

                        isInitialized.setLocation();
                    }

                    ordinal++;
                }

                // We've tried our best to fill in missing values, now run the
                // original check method.
                b.loadThis();
                b.invokeSuper(mClassFile.getSuperClassName(),
                              StorableGenerator.CHECK_PK_FOR_INSERT_METHOD_NAME,
                              null, null);
                b.returnVoid();
            }

            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toFinal(true),
                 StorableGenerator.DO_TRY_INSERT_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(persistExceptionType);
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable txnVar = b.createLocalVariable(null, transactionType);

            Label tryStart = addEnterTransaction(b, INSERT_OP, txnVar);

            if (mFeatures.contains(MasterFeature.VERSIONING)) {
                // Only set if uninitialized.
                b.loadThis();
                b.invokeVirtual(StorableGenerator.IS_VERSION_INITIALIZED_METHOD_NAME,
                                TypeDesc.BOOLEAN, null);
                Label isInitialized = b.createLabel();
                b.ifZeroComparisonBranch(isInitialized, "!=");
                addAdjustVersionProperty(b, null, 1);
                isInitialized.setLocation();
            }

            if (mFeatures.contains(MasterFeature.INSERT_CHECK_REQUIRED)) {
                // Ensure that required properties have been set.
                b.loadThis();
                b.invokeVirtual(StorableGenerator.IS_REQUIRED_DATA_INITIALIZED_METHOD_NAME,
                                TypeDesc.BOOLEAN, null);
                Label isInitialized = b.createLabel();
                b.ifZeroComparisonBranch(isInitialized, "!=");

                // Throw a ConstraintException.
                TypeDesc exType = TypeDesc.forClass(ConstraintException.class);
                b.newObject(exType);
                b.dup();

                // Append all the uninitialized property names to the exception message.

                LocalVariable countVar = b.createLocalVariable(null, TypeDesc.INT);
                b.loadConstant(0);
                b.storeLocal(countVar);

                TypeDesc sbType = TypeDesc.forClass(StringBuilder.class);
                b.newObject(sbType);
                b.dup();
                b.loadConstant("Not all required properties have been set: ");
                TypeDesc[] stringParam = {TypeDesc.STRING};
                b.invokeConstructor(sbType, stringParam);
                LocalVariable sbVar = b.createLocalVariable(null, sbType);
                b.storeLocal(sbVar);

                int ordinal = -1;

                HashSet<Integer> stateAppendMethods = new HashSet<Integer>();

                // Parameters are: StringBuilder, count, mask, property name
                TypeDesc[] appendParams = {sbType, TypeDesc.INT, TypeDesc.INT, TypeDesc.STRING};

                for (StorableProperty<S> property : mAllProperties.values()) {
                    ordinal++;

                    if (property.isJoin() || property.isPrimaryKeyMember()
                        || property.isNullable())
                    {
                        continue;
                    }

                    int stateField = ordinal >> 4;

                    String stateAppendMethodName = APPEND_UNINIT_PROPERTY + stateField;

                    if (!stateAppendMethods.contains(stateField)) {
                        stateAppendMethods.add(stateField);

                        MethodInfo mi2 = mClassFile.addMethod
                            (Modifiers.PRIVATE, stateAppendMethodName, TypeDesc.INT, appendParams);

                        CodeBuilder b2 = new CodeBuilder(mi2);

                        // Load the StringBuilder parameter.
                        b2.loadLocal(b2.getParameter(0));

                        String stateFieldName =
                            StorableGenerator.PROPERTY_STATE_FIELD_NAME + (ordinal >> 4);

                        b2.loadThis();
                        b2.loadField(stateFieldName, TypeDesc.INT);
                        // Load the mask parameter.
                        b2.loadLocal(b2.getParameter(2));
                        b2.math(Opcode.IAND);
                        
                        Label propIsInitialized = b2.createLabel();
                        b2.ifZeroComparisonBranch(propIsInitialized, "!=");

                        // Load the count parameter.
                        b2.loadLocal(b2.getParameter(1));
                        Label noComma = b2.createLabel();
                        b2.ifZeroComparisonBranch(noComma, "==");
                        b2.loadConstant(", ");
                        b2.invokeVirtual(sbType, "append", sbType, stringParam);
                        noComma.setLocation();

                        // Load the property name parameter.
                        b2.loadLocal(b2.getParameter(3));
                        b2.invokeVirtual(sbType, "append", sbType, stringParam);

                        // Increment the count parameter.
                        b2.integerIncrement(b2.getParameter(1), 1);

                        propIsInitialized.setLocation();

                        // Return the possibly updated count.
                        b2.loadLocal(b2.getParameter(1));
                        b2.returnValue(TypeDesc.INT);
                    }

                    b.loadThis();
                    // Parameters are: StringBuilder, count, mask, property name
                    b.loadLocal(sbVar);
                    b.loadLocal(countVar);
                    b.loadConstant(StorableGenerator.PROPERTY_STATE_MASK << ((ordinal & 0xf) * 2));
                    b.loadConstant(property.getName());
                    b.invokePrivate(stateAppendMethodName, TypeDesc.INT, appendParams);
                    b.storeLocal(countVar);
                }

                b.loadLocal(sbVar);
                b.invokeVirtual(sbType, "toString", TypeDesc.STRING, null);
                b.invokeConstructor(exType, new TypeDesc[] {TypeDesc.STRING});
                b.throwObject();

                isInitialized.setLocation();
            }

            b.loadThis();
            b.invokeVirtual(DO_TRY_INSERT_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);

            if (tryStart == null) {
                b.returnValue(TypeDesc.BOOLEAN);
            } else {
                Label failed = b.createLabel();
                b.ifZeroComparisonBranch(failed, "==");

                addCommitAndExitTransaction(b, INSERT_OP, txnVar);
                b.loadConstant(true);
                b.returnValue(TypeDesc.BOOLEAN);

                failed.setLocation();
                addExitTransaction(b, INSERT_OP, txnVar);
                b.loadConstant(false);
                b.returnValue(TypeDesc.BOOLEAN);

                addExitTransaction(b, INSERT_OP, txnVar, tryStart);
            }
        }

        // Add required protected doTryUpdate method.
        addDoTryUpdate: {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toFinal(true),
                 StorableGenerator.DO_TRY_UPDATE_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(persistExceptionType);
            CodeBuilder b = new CodeBuilder(mi);

            if ((!mFeatures.contains(MasterFeature.VERSIONING)) &&
                (!mFeatures.contains(MasterFeature.UPDATE_FULL)))
            {
                // Nothing special needs to be done, so just delegate and return.
                b.loadThis();
                b.invokeVirtual(DO_TRY_UPDATE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
                b.returnValue(TypeDesc.BOOLEAN);
                break addDoTryUpdate;
            }

            LocalVariable txnVar = b.createLocalVariable(null, transactionType);
            LocalVariable savedVar = null;

            Label tryStart = addEnterTransaction(b, UPDATE_OP, txnVar);

            Label failed = b.createLabel();

            if (mFeatures.contains(MasterFeature.UPDATE_FULL)) {
                // Storable saved = copy();
                b.loadThis();
                b.invokeVirtual(COPY_METHOD_NAME, storableType, null);
                b.checkCast(mClassFile.getType());
                savedVar = b.createLocalVariable(null, mClassFile.getType());
                b.storeLocal(savedVar);

                // if (!saved.tryLoad()) {
                //     goto failed;
                // }
                b.loadLocal(savedVar);
                b.invokeInterface(storableType, TRY_LOAD_METHOD_NAME, TypeDesc.BOOLEAN, null);
                b.ifZeroComparisonBranch(failed, "==");

                // if (version support enabled) {
                //     if (this.getVersionNumber() != saved.getVersionNumber()) {
                //         throw new OptimisticLockException
                //             (this.getVersionNumber(), saved.getVersionNumber(), this);
                //     }
                // }
                if (mFeatures.contains(MasterFeature.VERSIONING)) {
                    TypeDesc versionType = TypeDesc.forClass(mInfo.getVersionProperty().getType());
                    b.loadThis();
                    b.invoke(mInfo.getVersionProperty().getReadMethod());
                    b.loadLocal(savedVar);
                    b.invoke(mInfo.getVersionProperty().getReadMethod());
                    Label sameVersion = b.createLabel();
                    CodeBuilderUtil.addValuesEqualCall(b, versionType, true, sameVersion, true);
                    b.newObject(optimisticLockType);
                    b.dup();
                    b.loadThis();
                    b.invoke(mInfo.getVersionProperty().getReadMethod());
                    b.convert(versionType, TypeDesc.OBJECT);
                    b.loadLocal(savedVar);
                    b.invoke(mInfo.getVersionProperty().getReadMethod());
                    b.convert(versionType, TypeDesc.OBJECT);
                    b.loadThis();
                    b.invokeConstructor
                        (optimisticLockType,
                         new TypeDesc[] {TypeDesc.OBJECT, TypeDesc.OBJECT, storableType});
                    b.throwObject();
                    sameVersion.setLocation();
                }

                // this.copyDirtyProperties(saved);
                // if (version support enabled) {
                //     saved.setVersionNumber(saved.getVersionNumber() + 1);
                // }
                b.loadThis();
                b.loadLocal(savedVar);
                b.invokeVirtual(COPY_DIRTY_PROPERTIES, null, new TypeDesc[] {storableType});
                if (mFeatures.contains(MasterFeature.VERSIONING)) {
                    addAdjustVersionProperty(b, savedVar, -1);
                }

                // if (!saved.doTryUpdateMaster()) {
                //     goto failed;
                // }
                b.loadLocal(savedVar);
                b.invokeVirtual(DO_TRY_UPDATE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
                b.ifZeroComparisonBranch(failed, "==");

                // saved.copyUnequalProperties(this);
                b.loadLocal(savedVar);
                b.loadThis();
                b.invokeInterface
                    (storableType, COPY_UNEQUAL_PROPERTIES, null, new TypeDesc[] {storableType});
            } else {
                // if (!this.doTryUpdateMaster()) {
                //     goto failed;
                // }
                b.loadThis();
                b.invokeVirtual(DO_TRY_UPDATE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);
                b.ifZeroComparisonBranch(failed, "==");
            }

            // txn.commit();
            // txn.exit();
            // return true;
            addCommitAndExitTransaction(b, UPDATE_OP, txnVar);
            b.loadConstant(true);
            b.returnValue(TypeDesc.BOOLEAN);

            // failed:
            // txn.exit();
            failed.setLocation();
            addExitTransaction(b, UPDATE_OP, txnVar);
            // return false;
            b.loadConstant(false);
            b.returnValue(TypeDesc.BOOLEAN);

            addExitTransaction(b, UPDATE_OP, txnVar, tryStart);
        }

        // Add required protected doTryDelete method.
        {
            MethodInfo mi = mClassFile.addMethod
                (Modifiers.PROTECTED.toFinal(true),
                 StorableGenerator.DO_TRY_DELETE_METHOD_NAME, TypeDesc.BOOLEAN, null);
            mi.addException(persistExceptionType);
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable txnVar = b.createLocalVariable(null, transactionType);

            Label tryStart = addEnterTransaction(b, DELETE_OP, txnVar);

            b.loadThis();
            b.invokeVirtual(DO_TRY_DELETE_MASTER_METHOD_NAME, TypeDesc.BOOLEAN, null);

            if (tryStart == null) {
                b.returnValue(TypeDesc.BOOLEAN);
            } else {
                Label failed = b.createLabel();
                b.ifZeroComparisonBranch(failed, "==");
                addCommitAndExitTransaction(b, DELETE_OP, txnVar);
                b.loadConstant(true);
                b.returnValue(TypeDesc.BOOLEAN);

                failed.setLocation();
                addExitTransaction(b, DELETE_OP, txnVar);
                b.loadConstant(false);
                b.returnValue(TypeDesc.BOOLEAN);

                addExitTransaction(b, DELETE_OP, txnVar, tryStart);
            }
        }
    }

    /**
     * Generates code to enter a transaction, if required.
     *
     * @param opType type of operation, Insert, Update, or Delete
     * @param txnVar required variable of type Transaction for storing transaction
     * @return optional try start label for transaction
     */
    private Label addEnterTransaction(CodeBuilder b, String opType, LocalVariable txnVar) {
        if (!alwaysHasTxn(opType)) {
            return null;
        }

        // txn = masterSupport.getRootRepository().enterTransaction();

        TypeDesc repositoryType = TypeDesc.forClass(Repository.class);
        TypeDesc transactionType = TypeDesc.forClass(Transaction.class);
        TypeDesc triggerSupportType = TypeDesc.forClass(TriggerSupport.class);
        TypeDesc masterSupportType = TypeDesc.forClass(MasterSupport.class);

        b.loadThis();
        b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, triggerSupportType);
        b.invokeInterface(masterSupportType, "getRootRepository",
                          repositoryType, null);
        b.invokeInterface(repositoryType, ENTER_TRANSACTION_METHOD_NAME,
                          transactionType, null);
        b.storeLocal(txnVar);
        if (requiresTxnForUpdate(opType)) {
            // txn.setForUpdate(true);
            b.loadLocal(txnVar);
            b.loadConstant(true);
            b.invokeInterface(transactionType, SET_FOR_UPDATE_METHOD_NAME, null,
                              new TypeDesc[] {TypeDesc.BOOLEAN});
        }

        return b.createLabel().setLocation();
    }

    private boolean alwaysHasTxn(String opType) {
        return alwaysHasTxn(opType, mFeatures);
    }

    private static boolean alwaysHasTxn(String opType, EnumSet<MasterFeature> features) {
        if (opType == UPDATE_OP) {
            return
                features.contains(MasterFeature.UPDATE_TXN) ||
                features.contains(MasterFeature.UPDATE_TXN_FOR_UPDATE) ||
                features.contains(MasterFeature.VERSIONING) ||
                features.contains(MasterFeature.UPDATE_FULL);
        } else if (opType == INSERT_OP) {
            return
                features.contains(MasterFeature.INSERT_TXN) ||
                features.contains(MasterFeature.INSERT_TXN_FOR_UPDATE);
        } else if (opType == DELETE_OP) {
            return
                features.contains(MasterFeature.DELETE_TXN) ||
                features.contains(MasterFeature.DELETE_TXN_FOR_UPDATE);
        }
        return false;
    }

    private boolean requiresTxnForUpdate(String opType) {
        return requiresTxnForUpdate(opType, mFeatures);
    }

    private static boolean requiresTxnForUpdate(String opType, EnumSet<MasterFeature> features) {
        if (opType == UPDATE_OP) {
            return
                features.contains(MasterFeature.UPDATE_TXN_FOR_UPDATE) ||
                features.contains(MasterFeature.VERSIONING) ||
                features.contains(MasterFeature.UPDATE_FULL);
        } else if (opType == INSERT_OP) {
            return features.contains(MasterFeature.INSERT_TXN_FOR_UPDATE);
        } else if (opType == DELETE_OP) {
            return features.contains(MasterFeature.DELETE_TXN_FOR_UPDATE);
        }
        return false;
    }

    private void addCommitAndExitTransaction(CodeBuilder b, String opType, LocalVariable txnVar) {
        if (!alwaysHasTxn(opType)) {
            return;
        }

        TypeDesc transactionType = TypeDesc.forClass(Transaction.class);

        // txn.commit();
        // txn.exit();
        b.loadLocal(txnVar);
        b.invokeInterface(transactionType, COMMIT_METHOD_NAME, null, null);
        b.loadLocal(txnVar);
        b.invokeInterface(transactionType, EXIT_METHOD_NAME, null, null);
    }

    /**
     *
     * @param opType type of operation, Insert, Update, or Delete
     */
    private void addExitTransaction(CodeBuilder b, String opType, LocalVariable txnVar) {
        if (!alwaysHasTxn(opType)) {
            return;
        }

        TypeDesc transactionType = TypeDesc.forClass(Transaction.class);

        // txn.exit();
        b.loadLocal(txnVar);
        b.invokeInterface(transactionType, EXIT_METHOD_NAME, null, null);
    }

    /**
     *
     * @param opType type of operation, Insert, Update, or Delete
     */
    private void addExitTransaction(CodeBuilder b, String opType, LocalVariable txnVar,
                                    Label tryStart)
    {
        if (tryStart == null) {
            addExitTransaction(b, opType, txnVar);
            return;
        }

        // } catch (... e) {
        //     txn.exit();
        //     throw e;
        // }

        Label tryEnd = b.createLabel().setLocation();
        b.exceptionHandler(tryStart, tryEnd, null);
        addExitTransaction(b, opType, txnVar);
        b.throwObject();
    }

    /*
     * Generates code to adjust the version property. If value parameter is negative, then
     * version is incremented as follows:
     *
     * storable.setVersionNumber(storable.getVersionNumber() + 1);
     *
     * Otherwise, the version is set:
     *
     * storable.setVersionNumber(value);
     *
     * @param storableVar references storable instance, or null if this
     * @param value if negative, increment version, else, set version to this value
     */
    private void addAdjustVersionProperty(CodeBuilder b,
                                          LocalVariable storableVar,
                                          int value)
        throws SupportException
    {
        StorableProperty<?> versionProperty = mInfo.getVersionProperty();

        TypeDesc versionType = TypeDesc.forClass(versionProperty.getType());
        TypeDesc versionPrimitiveType = versionType.toPrimitiveType();
        supportCheck: {
            if (versionPrimitiveType != null) {
                switch (versionPrimitiveType.getTypeCode()) {
                case TypeDesc.INT_CODE:
                case TypeDesc.LONG_CODE:
                    break supportCheck;
                }
            }
            throw new SupportException
                ("Unsupported version type: " + versionType.getFullName());
        }

        if (storableVar == null) {
            b.loadThis();
        } else {
            b.loadLocal(storableVar);
        }

        if (value >= 0) {
            if (versionPrimitiveType == TypeDesc.LONG) {
                b.loadConstant((long) value);
            } else {
                b.loadConstant(value);
            }
        } else {
            b.dup();
            b.invoke(versionProperty.getReadMethod());
            Label setVersion = b.createLabel();
            if (!versionType.isPrimitive()) {
                b.dup();
                Label versionNotNull = b.createLabel();
                b.ifNullBranch(versionNotNull, false);
                b.pop();
                if (versionPrimitiveType == TypeDesc.LONG) {
                    b.loadConstant(1L);
                } else {
                    b.loadConstant(1);
                }
                b.branch(setVersion);
                versionNotNull.setLocation();
                b.convert(versionType, versionPrimitiveType);
            }
            if (versionPrimitiveType == TypeDesc.LONG) {
                b.loadConstant(1L);
                b.math(Opcode.LADD);
            } else {
                b.loadConstant(1);
                b.math(Opcode.IADD);
            }
            setVersion.setLocation();
        }

        b.convert(versionPrimitiveType, versionType);
        b.invoke(versionProperty.getWriteMethod());
    }
}
