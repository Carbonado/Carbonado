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
 * Factory for creating instances of {@link StorableCodec}.
 *
 * @author Brian S O'Neill
 */
public interface StorableCodecFactory {
    /**
     * Returns the preferred storage/database name for the given type. Return
     * null to let repository decide.
     *
     * @throws SupportException if type is not supported
     */
    String getStorageName(Class<? extends Storable> type) throws SupportException;

    /**
     * Optionally return additional information regarding storable encoding.
     *
     * @since 1.2.1
     */
    LayoutOptions getLayoutOptions(Class<? extends Storable> type);

    /**
     * @param type type of storable to create codec for
     * @param pkIndex suggested index for primary key (optional)
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, attempt to encode a storable layout
     * generation value in each storable
     * @throws SupportException if type is not supported
     */
    <S extends Storable> StorableCodec<S> createCodec(Class<S> type,
                                                      StorableIndex pkIndex,
                                                      boolean isMaster,
                                                      Layout layout)
        throws SupportException;

    /**
     * @param type type of storable to create codec for
     * @param pkIndex suggested index for primary key (optional)
     * @param isMaster when true, version properties and sequences are managed
     * @param layout when non-null, attempt to encode a storable layout
     * generation value in each storable
     * @param support binds generated storable with a storage layer
     * @throws SupportException if type is not supported
     * @since 1.2
     */
    <S extends Storable> StorableCodec<S> createCodec(Class<S> type,
                                                      StorableIndex pkIndex,
                                                      boolean isMaster,
                                                      Layout layout,
                                                      RawSupport support)
        throws SupportException;
}
