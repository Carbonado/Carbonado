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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

import java.lang.reflect.UndeclaredThrowableException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;
import org.cojen.util.ClassInjector;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import static com.amazon.carbonado.gen.CommonMethodNames.*;

import com.amazon.carbonado.raw.GenericEncodingStrategy;

/**
 * Support for general-purpose serialization of storables.
 * <p>
 * TODO: This class is unable to determine state of properties, and so they are
 * lost during serialization. Upon deserialization, all properties are assumed
 * dirty. To fix this, serialization might need to be supported directly by
 * Storables. When that happens, this class will be deprecated.
 *
 * @author Brian S O'Neill
 */
public abstract class StorableSerializer<S extends Storable> {
    private static final String ENCODE_METHOD_NAME = "encode";
    private static final String DECODE_METHOD_NAME = "decode";
    private static final String WRITE_METHOD_NAME = "write";
    private static final String READ_METHOD_NAME = "read";

    @SuppressWarnings("unchecked")
    private static Map<Class, Reference<StorableSerializer<?>>> cCache = new WeakIdentityMap();

    /**
     * @param type type of storable to serialize
     */
    @SuppressWarnings("unchecked")
    public static <S extends Storable> StorableSerializer<S> forType(Class<S> type)
        throws SupportException
    {
        synchronized (cCache) {
            StorableSerializer<S> serializer;
            Reference<StorableSerializer<?>> ref = cCache.get(type);
            if (ref != null) {
                serializer = (StorableSerializer<S>) ref.get();
                if (serializer != null) {
                    return serializer;
                }
            }
            serializer = generateSerializer(type);
            cCache.put(type, new SoftReference<StorableSerializer<?>>(serializer));
            return serializer;
        }
    }

    @SuppressWarnings("unchecked")
    private static <S extends Storable> StorableSerializer<S> generateSerializer(Class<S> type)
        throws SupportException
    {
        Class<? extends S> abstractClass = StorableGenerator.getAbstractClass(type);

        // Use abstract class ClassLoader in order to access adapter instances.
        ClassInjector ci = ClassInjector.create
            (type.getName(), abstractClass.getClassLoader());
        ClassFile cf = new ClassFile(ci.getClassName(), StorableSerializer.class);
        cf.markSynthetic();
        cf.setSourceFile(StorableSerializer.class.getName());
        cf.setTarget("1.5");

        cf.addDefaultConstructor();

        Map<String, ? extends StorableProperty<S>> propertyMap =
            StorableIntrospector.examine(type).getAllProperties();

        StorableProperty<S>[] properties;
        {
            // Exclude derived properties and joins.
            List<StorableProperty<S>> list =
                new ArrayList<StorableProperty<S>>(propertyMap.size());
            for (StorableProperty<S> property : propertyMap.values()) {
                if (!property.isDerived() && !property.isJoin()) {
                    list.add(property);
                }
            }
            properties = new StorableProperty[list.size()];
            list.toArray(properties);
        }

        GenericEncodingStrategy<S> ges = new GenericEncodingStrategy<S>(type, null);

        TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
        TypeDesc storableType = TypeDesc.forClass(Storable.class);
        TypeDesc userStorableType = TypeDesc.forClass(type);
        TypeDesc storageType = TypeDesc.forClass(Storage.class);

        // Build method to encode storable into a byte array.
        {
            MethodInfo mi = cf.addMethod
                (Modifiers.PRIVATE.toStatic(true), ENCODE_METHOD_NAME, byteArrayType,
                 new TypeDesc[] {userStorableType});
            CodeBuilder b = new CodeBuilder(mi);
            LocalVariable encodedVar =
                ges.buildDataEncoding(b, properties, b.getParameter(0), abstractClass, true, -1);
            b.loadLocal(encodedVar);
            b.returnValue(byteArrayType);
        }

        // Build method to decode storable from a byte array.
        {
            MethodInfo mi = cf.addMethod
                (Modifiers.PRIVATE.toStatic(true), DECODE_METHOD_NAME, userStorableType,
                 new TypeDesc[] {storageType, byteArrayType});
            CodeBuilder b = new CodeBuilder(mi);
            LocalVariable instanceVar = b.createLocalVariable(null, userStorableType);
            b.loadLocal(b.getParameter(0));
            b.invokeInterface(storageType, PREPARE_METHOD_NAME,
                              storableType, null);
            b.checkCast(userStorableType);
            b.storeLocal(instanceVar);
            LocalVariable encodedVar = b.getParameter(1);
            ges.buildDataDecoding
                (b, properties, instanceVar, abstractClass, true, -1, null, encodedVar);
            b.loadLocal(instanceVar);
            b.returnValue(storableType);
        }

        // Build write method for DataOutput.
        {
            TypeDesc dataOutputType =  TypeDesc.forClass(DataOutput.class);

            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, WRITE_METHOD_NAME, null,
                                         new TypeDesc[] {storableType, dataOutputType});

            CodeBuilder b = new CodeBuilder(mi);
            LocalVariable storableVar = b.getParameter(0);
            LocalVariable doutVar = b.getParameter(1);

            b.loadLocal(storableVar);
            b.checkCast(userStorableType);
            b.invokeStatic(ENCODE_METHOD_NAME, byteArrayType, new TypeDesc[] {userStorableType});
            LocalVariable encodedVar = b.createLocalVariable(null, byteArrayType);
            b.storeLocal(encodedVar);

            b.loadLocal(doutVar);
            b.loadLocal(encodedVar);
            b.arrayLength();
            b.invokeInterface(dataOutputType, "writeInt", null, new TypeDesc[] {TypeDesc.INT});

            b.loadLocal(doutVar);
            b.loadLocal(encodedVar);
            b.invokeInterface(dataOutputType, "write", null, new TypeDesc[] {byteArrayType});
            b.returnVoid();
        }

        final TypeDesc storableSerializerType = TypeDesc.forClass(StorableSerializer.class);

        // Build write method for OutputStream.
        {
            TypeDesc outputStreamType =  TypeDesc.forClass(OutputStream.class);

            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, WRITE_METHOD_NAME, null,
                                         new TypeDesc[] {storableType, outputStreamType});

            CodeBuilder b = new CodeBuilder(mi);
            LocalVariable storableVar = b.getParameter(0);
            LocalVariable outVar = b.getParameter(1);

            b.loadLocal(storableVar);
            b.checkCast(userStorableType);
            b.invokeStatic(ENCODE_METHOD_NAME, byteArrayType, new TypeDesc[] {userStorableType});
            LocalVariable encodedVar = b.createLocalVariable(null, byteArrayType);
            b.storeLocal(encodedVar);

            b.loadLocal(outVar);
            b.loadLocal(encodedVar);
            b.arrayLength();
            b.invokeStatic(storableSerializerType, "writeInt", null,
                           new TypeDesc[] {outputStreamType, TypeDesc.INT});

            b.loadLocal(outVar);
            b.loadLocal(encodedVar);
            b.invokeVirtual(outputStreamType, "write", null, new TypeDesc[] {byteArrayType});
            b.returnVoid();
        }

        // Build read method for DataInput.
        {
            TypeDesc dataInputType =  TypeDesc.forClass(DataInput.class);

            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, READ_METHOD_NAME, storableType,
                                         new TypeDesc[] {storageType, dataInputType});

            CodeBuilder b = new CodeBuilder(mi);
            LocalVariable storageVar = b.getParameter(0);
            LocalVariable dinVar = b.getParameter(1);

            b.loadLocal(dinVar);
            b.invokeInterface(dataInputType, "readInt", TypeDesc.INT, null);
            b.newObject(byteArrayType);
            LocalVariable byteArrayVar = b.createLocalVariable(null, byteArrayType);
            b.storeLocal(byteArrayVar);

            b.loadLocal(dinVar);
            b.loadLocal(byteArrayVar);
            b.invokeInterface(dataInputType, "readFully", null, new TypeDesc[] {byteArrayType});

            b.loadLocal(storageVar);
            b.loadLocal(byteArrayVar);
            b.invokeStatic(DECODE_METHOD_NAME, userStorableType,
                           new TypeDesc[] {storageType, byteArrayType});
            b.returnValue(storableType);
        }

        // Build read method for InputStream.
        {
            TypeDesc inputStreamType =  TypeDesc.forClass(InputStream.class);

            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, READ_METHOD_NAME, storableType,
                                         new TypeDesc[] {storageType, inputStreamType});

            CodeBuilder b = new CodeBuilder(mi);
            LocalVariable storageVar = b.getParameter(0);
            LocalVariable inVar = b.getParameter(1);

            b.loadLocal(inVar);
            b.invokeStatic(storableSerializerType, "readInt", TypeDesc.INT,
                           new TypeDesc[] {inputStreamType});
            b.newObject(byteArrayType);
            LocalVariable byteArrayVar = b.createLocalVariable(null, byteArrayType);
            b.storeLocal(byteArrayVar);

            b.loadLocal(inVar);
            b.loadLocal(byteArrayVar);
            b.invokeStatic(storableSerializerType, "readFully", null,
                           new TypeDesc[] {inputStreamType, byteArrayType});

            b.loadLocal(storageVar);
            b.loadLocal(byteArrayVar);
            b.invokeStatic(DECODE_METHOD_NAME, userStorableType,
                           new TypeDesc[] {storageType, byteArrayType});
            b.returnValue(storableType);
        }

        Class<StorableSerializer> clazz = (Class<StorableSerializer>) ci.defineClass(cf);

        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new UndeclaredThrowableException(e);
        } catch (IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    protected StorableSerializer() {
    }

    public abstract void write(S storable, DataOutput out) throws IOException;

    public abstract void write(S storable, OutputStream out) throws IOException;

    public abstract S read(Storage<S> storage, DataInput in) throws IOException, EOFException;

    public abstract S read(Storage<S> storage, InputStream in) throws IOException, EOFException;

    public static void writeInt(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xff);
        out.write((v >>> 16) & 0xff);
        out.write((v >>>  8) & 0xff);
        out.write(v & 0xff);
    }

    public static int readInt(InputStream in) throws IOException {
        int a = in.read();
        int b = in.read();
        int c = in.read();
        int d = in.read();
        if ((a | b | c | d) < 0) {
            throw new EOFException();
        }
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    public static void readFully(InputStream in, byte[] b) throws IOException {
        int length = b.length;
        int n = 0;
        while (n < length) {
            int count = in.read(b, n, length - n);
            if (count < 0) {
                throw new EOFException();
            }
            n += count;
        }
    }
}
