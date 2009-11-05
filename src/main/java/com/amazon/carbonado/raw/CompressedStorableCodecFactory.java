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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.layout.LayoutOptions;

/**
 * Extension of GenericStorableCodecFactory that allows for compression.
 *
 * @author Olga Kuznetsova
 * @author Brian S O'Neill
 */
public class CompressedStorableCodecFactory extends GenericStorableCodecFactory {
    private final Map<String, CompressionType> mCompressionMap;

    public CompressedStorableCodecFactory(Map<String, CompressionType> compressionMap) {
        if (compressionMap == null || compressionMap.isEmpty()) {
            mCompressionMap = Collections.emptyMap();
        } else {
            mCompressionMap = new HashMap<String, CompressionType>(compressionMap);
        }
    }

    @Override
    public LayoutOptions getLayoutOptions(Class<? extends Storable> type) {
        CompressionType compType = getCompressionType(type);
        if (compType == CompressionType.NONE) {
            return null;
        }
        LayoutOptions options = new LayoutOptions();
        options.setCompressionType(compType.toString());
        return options;
    }

    @Override
    protected <S extends Storable> GenericEncodingStrategy<S> createStrategy
                         (Class<S> type,
                          StorableIndex<S> pkIndex,
                          LayoutOptions options)
        throws SupportException
    {
        CompressionType compType;
        if (options == null) {
            compType = getCompressionType(type);
        } else {
            compType = CompressionType.valueOf(options.getCompressionType());
        }

        return new CompressedEncodingStrategy<S>(type, pkIndex, compType);
    }

    /**
     * @return non-null compression type for the given storable
     */
    protected CompressionType getCompressionType(Class<? extends Storable> type) {
        CompressionType compType = mCompressionMap.get(type.getName());
        return compType == null ? CompressionType.NONE : compType;
    }
}
