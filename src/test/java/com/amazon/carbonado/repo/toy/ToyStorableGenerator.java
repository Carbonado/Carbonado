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

package com.amazon.carbonado.repo.toy;

import java.util.EnumSet;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;

import org.cojen.util.ClassInjector;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.spi.MasterFeature;
import com.amazon.carbonado.spi.MasterStorableGenerator;
import com.amazon.carbonado.spi.MasterSupport;
import com.amazon.carbonado.spi.StorableGenerator;
import com.amazon.carbonado.spi.TriggerSupport;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ToyStorableGenerator<S extends Storable> {
    private static final Map<Class, Class> cCache;

    static {
        cCache = new SoftValuedHashMap();
    }

    /**
     * Generated class has a constructor that accepts a ToyStorage instance.
     */
    public static <S extends Storable> Class<? extends S> getGeneratedClass(Class<S> type)
        throws SupportException
    {
        synchronized (cCache) {
            Class<? extends S> generatedClass = (Class<? extends S>) cCache.get(type);
            if (generatedClass != null) {
                return generatedClass;
            }
            generatedClass = new ToyStorableGenerator<S>(type).generateAndInjectClass();
            cCache.put(type, generatedClass);
            return generatedClass;
        }
    }

    private final Class<S> mStorableType;

    private final ClassInjector mClassInjector;
    private final ClassFile mClassFile;

    private ToyStorableGenerator(Class<S> type) throws SupportException {
        mStorableType = type;

        EnumSet<MasterFeature> features = EnumSet
            .of(MasterFeature.VERSIONING, MasterFeature.INSERT_SEQUENCES);

        final Class<? extends S> abstractClass =
            MasterStorableGenerator.getAbstractClass(mStorableType, features);

        mClassInjector = ClassInjector.create(mStorableType.getName(),
                                              abstractClass.getClassLoader());

        mClassFile = new ClassFile(mClassInjector.getClassName(), abstractClass);
        mClassFile.markSynthetic();
        mClassFile.setSourceFile(ToyStorableGenerator.class.getName());
        mClassFile.setTarget("1.5");
    }

    private Class<? extends S> generateAndInjectClass() {
        TypeDesc masterSupportType = TypeDesc.forClass(MasterSupport.class);
        TypeDesc toyStorageType = TypeDesc.forClass(ToyStorage.class);

        // Add constructor that accepts a ToyStorage.
        {
            TypeDesc[] params = {toyStorageType};
            MethodInfo mi = mClassFile.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);
            b.loadThis();
            b.loadLocal(b.getParameter(0));
            b.invokeSuperConstructor(new TypeDesc[] {masterSupportType});
            b.returnVoid();
        }

        // Implement abstract methods which all delegate to ToyStorage instance.

        generateDelegatedMethod
            (MasterStorableGenerator.DO_TRY_LOAD_MASTER_METHOD_NAME, "doTryLoad");
        generateDelegatedMethod
            (MasterStorableGenerator.DO_TRY_INSERT_MASTER_METHOD_NAME, "doTryInsert");
        generateDelegatedMethod
            (MasterStorableGenerator.DO_TRY_UPDATE_MASTER_METHOD_NAME, "doTryUpdate");
        generateDelegatedMethod
            (MasterStorableGenerator.DO_TRY_DELETE_MASTER_METHOD_NAME, "doTryDelete");

        Class<? extends S> generatedClass = mClassInjector.defineClass(mClassFile);

        return generatedClass;
    }

    private void generateDelegatedMethod(String masterMethodName, String supportMethodName) {
        TypeDesc triggerSupportType = TypeDesc.forClass(TriggerSupport.class);
        TypeDesc toyStorageType = TypeDesc.forClass(ToyStorage.class);

        TypeDesc[] storableParam = {TypeDesc.forClass(Storable.class)};

        MethodInfo mi = mClassFile.addMethod
            (Modifiers.PROTECTED, masterMethodName, TypeDesc.BOOLEAN, null);
        CodeBuilder b = new CodeBuilder(mi);

        b.loadThis();
        b.loadField(StorableGenerator.SUPPORT_FIELD_NAME, triggerSupportType);
        b.checkCast(toyStorageType);
        b.loadThis();
        b.invokeVirtual(toyStorageType, supportMethodName, TypeDesc.BOOLEAN, storableParam);
        b.returnValue(TypeDesc.BOOLEAN);
    }
}
