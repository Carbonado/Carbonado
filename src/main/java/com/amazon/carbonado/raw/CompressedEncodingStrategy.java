/*
 * Copyright 2009 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.raw;

import org.cojen.classfile.CodeAssembler;
import org.cojen.classfile.Label;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.Opcode;
import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Extension of GenericEncodingStrategy that allows for compression.
 *
 * @author Olga Kuznetsova
 * @author Brian S O'Neill
 */
public class CompressedEncodingStrategy<S extends Storable> extends GenericEncodingStrategy<S> {
    private final CompressionType mCompressionType;

    public CompressedEncodingStrategy(Class<S> type,
                                      StorableIndex<S> pkIndex,
                                      CompressionType compressionType) {
        super(type, pkIndex);
        mCompressionType = compressionType;
    }

    @Override
    protected void extraDataEncoding(CodeAssembler a,
                                     LocalVariable dataVar, int prefix, int suffix)
    {
        switch (mCompressionType) {
        case GZIP:
            TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
            a.loadLocal(dataVar);
            a.loadConstant(prefix);
            a.invokeStatic(GzipCompressor.class.getName(), "compress", byteArrayType,
                           new TypeDesc[] {byteArrayType, TypeDesc.INT});
            a.storeLocal(dataVar);
            break;
        }
    }

    @Override
    protected void extraDataDecoding(CodeAssembler a,
                                     LocalVariable dataVar, int prefix, int suffix)
    {
        switch (mCompressionType) {
        case GZIP:
            TypeDesc byteArrayType = TypeDesc.forClass(byte[].class);
            a.loadLocal(dataVar);
            a.loadConstant(prefix);
            a.invokeStatic(GzipCompressor.class.getName(), "decompress", byteArrayType,
                           new TypeDesc[] {byteArrayType, TypeDesc.INT});
            a.storeLocal(dataVar);
            break;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof CompressedEncodingStrategy) {
            CompressedEncodingStrategy other = (CompressedEncodingStrategy) obj;
            return super.equals(obj) && mCompressionType.equals(other.mCompressionType);
        }
        return false;
    }

    @Override 
    public int hashCode() {
        return super.hashCode() + mCompressionType.hashCode();
    }
}
