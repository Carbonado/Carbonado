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

/**
 * Factory for custom storable codecs.
 *
 * @author Brian S O'Neill
 */
public abstract class CustomStorableCodecFactory implements StorableCodecFactory {
    public CustomStorableCodecFactory() {
    }

    /**
     * Returns null to let repository decide what the name should be.
     */
    public String getStorageName(Class<? extends Storable> type) throws SupportException {
        return null;
    }

    /**
     * @param type type of storable to create codec for
     * @param pkIndex ignored
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, attempt to encode a storable layout
     * generation value in each storable
     * @throws SupportException if type is not supported
     */
    public <S extends Storable> CustomStorableCodec<S> createCodec(Class<S> type,
                                                                   StorableIndex pkIndex,
                                                                   boolean isMaster,
                                                                   Layout layout)
        throws SupportException
    {
        return createCodec(type, isMaster, layout);
    }

    /**
     * @param type type of storable to create codec for
     * @param pkIndex ignored
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, attempt to encode a storable layout
     * generation value in each storable
     * @param support binds generated storable with a storage layer
     * @throws SupportException if type is not supported
     * @since 1.2
     */
    public <S extends Storable> CustomStorableCodec<S> createCodec(Class<S> type,
                                                                   StorableIndex pkIndex,
                                                                   boolean isMaster,
                                                                   Layout layout,
                                                                   RawSupport support)
        throws SupportException
    {
        CustomStorableCodec<S> codec = createCodec(type, isMaster, layout, support);
        // Possibly set support after construction, for backwards compatibility.
        if (codec.mSupport == null) {
            codec.mSupport = support;
        }
        return codec;
    }

    /**
     * @param type type of storable to create codec for
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, attempt to encode a storable layout
     * generation value in each storable
     * @throws SupportException if type is not supported
     */
    protected abstract <S extends Storable> CustomStorableCodec<S>
        createCodec(Class<S> type, boolean isMaster, Layout layout)
        throws SupportException;

    /**
     * @param type type of storable to create codec for
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, attempt to encode a storable layout
     * generation value in each storable
     * @param support binds generated storable with a storage layer
     * @throws SupportException if type is not supported
     * @since 1.2
     */
    // Note: This factory method is not abstract for backwards compatibility.
    protected <S extends Storable> CustomStorableCodec<S>
        createCodec(Class<S> type, boolean isMaster, Layout layout, RawSupport support)
        throws SupportException
    {
        return createCodec(type, isMaster, layout);
    }
}
