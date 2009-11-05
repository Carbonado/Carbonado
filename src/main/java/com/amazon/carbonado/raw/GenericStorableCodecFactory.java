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

package com.amazon.carbonado.raw;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutOptions;

/**
 * Factory for generic codec that supports any kind of storable by
 * auto-generating and caching storable implementations.
 *
 * @author Brian S O'Neill
 */
public class GenericStorableCodecFactory implements StorableCodecFactory {
    public GenericStorableCodecFactory() {
    }

    /**
     * Returns null to let repository decide what the name should be.
     */
    public String getStorageName(Class<? extends Storable> type) throws SupportException {
        return null;
    }

    /**
     * Returns null.
     */
    @Override
    public LayoutOptions getLayoutOptions(Class<? extends Storable> type) {
        return null;
    }

    /**
     * @param type type of storable to create codec for
     * @param pkIndex suggested index for primary key (optional)
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, encode a storable layout generation
     * value in one or four bytes. Generation 0..127 is encoded in one byte, and
     * 128..max is encoded in four bytes, with the most significant bit set.
     * @throws SupportException if type is not supported
     */
    @SuppressWarnings("unchecked")
    public <S extends Storable> GenericStorableCodec<S> createCodec(Class<S> type,
                                                                    StorableIndex pkIndex,
                                                                    boolean isMaster,
                                                                    Layout layout)
        throws SupportException
    {
        return createCodec(type, pkIndex, isMaster, layout, null);
    }

    /**
     * @param type type of storable to create codec for
     * @param pkIndex suggested index for primary key (optional)
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, encode a storable layout generation
     * value in one or four bytes. Generation 0..127 is encoded in one byte, and
     * 128..max is encoded in four bytes, with the most significant bit set.
     * @param support binds generated storable with a storage layer
     * @throws SupportException if type is not supported
     * @since 1.2
     */
    @SuppressWarnings("unchecked")
    public <S extends Storable> GenericStorableCodec<S> createCodec(Class<S> type,
                                                                    StorableIndex pkIndex,
                                                                    boolean isMaster,
                                                                    Layout layout,
                                                                    RawSupport support)
        throws SupportException
    {
        return GenericStorableCodec.getInstance
            (this, createStrategy(type, pkIndex, null), isMaster, layout, support);
    }

    /**
     * Override to return a different EncodingStrategy.
     *
     * @param type type of Storable to generate code for
     * @param pkIndex specifies sequence and ordering of key properties (optional)
     */
    protected <S extends Storable> GenericEncodingStrategy<S> createStrategy
        (Class<S> type, StorableIndex<S> pkIndex)
        throws SupportException
    {
        return new GenericEncodingStrategy<S>(type, pkIndex);
    }

    /**
     * Override to return a different EncodingStrategy.
     *
     * @param type type of Storable to generate code for
     * @param pkIndex specifies sequence and ordering of key properties (optional)
     * @param options additional layout options (optional)
     * @since 1.2.1
     */
    protected <S extends Storable> GenericEncodingStrategy<S> createStrategy
        (Class<S> type, StorableIndex<S> pkIndex, LayoutOptions options)
        throws SupportException
    {
        // Call into original method for backwards compatibility.
        return createStrategy(type, pkIndex);
    }
}
