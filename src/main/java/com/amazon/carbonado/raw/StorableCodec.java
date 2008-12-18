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

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.info.StorableIndex;

/**
 * Supports encoding and decoding of storables.
 *
 * @author Brian S O'Neill
 * @see StorableCodecFactory
 */
public interface StorableCodec<S extends Storable> {
    /**
     * Returns the type of Storable produced by this codec.
     */
    Class<S> getStorableType();

    /**
     * Instantiate a Storable with no key or value defined yet. The default
     * {@link RawSupport} is supplied to the instance.
     *
     * @throws IllegalStateException if no default support exists
     * @since 1.2
     */
    S instantiate();

    /**
     * Instantiate a Storable with a specific key and value. The default
     * {@link RawSupport} is supplied to the instance.
     *
     * @throws IllegalStateException if no default support exists
     * @since 1.2
     */
    S instantiate(byte[] key, byte[] value) throws FetchException;

    /**
     * Instantiate a Storable with no key or value defined yet. Any
     * {@link RawSupport} can be supplied to the instance.
     *
     * @param support binds generated storable with a storage layer
     */
    S instantiate(RawSupport<S> support);

    /**
     * Instantiate a Storable with a specific key and value. Any
     * {@link RawSupport} can be supplied to the instance.
     *
     * @param support binds generated storable with a storage layer
     */
    S instantiate(RawSupport<S> support, byte[] key, byte[] value) throws FetchException;

    /**
     * Returns the sequence and directions of properties that make up the
     * primary key.
     */
    StorableIndex<S> getPrimaryKeyIndex();

    /**
     * Returns the number of prefix bytes in the primary key, which may be
     * zero.
     */
    int getPrimaryKeyPrefixLength();

    /**
     * Encode a key by extracting all the primary key properties from the given
     * storable.
     *
     * @param storable extract primary key properties from this instance
     * @return raw search key
     */
    byte[] encodePrimaryKey(S storable);

    /**
     * Encode a key by extracting all the primary key properties from the given
     * storable.
     *
     * @param storable extract primary key properties from this instance
     * @param rangeStart index of first property to use. Its value must be less
     * than the count of primary key properties.
     * @param rangeEnd index of last property to use, exlusive. Its value must
     * be less than or equal to the count of primary key properties.
     * @return raw search key
     */
    byte[] encodePrimaryKey(S storable, int rangeStart, int rangeEnd);

    /**
     * Encode a key by extracting all the primary key properties from the given
     * storable.
     *
     * @param values values to build into a key. It must be long enough to
     * accommodate all primary key properties.
     * @return raw search key
     */
    byte[] encodePrimaryKey(Object[] values);

    /**
     * Encode a key by extracting all the primary key properties from the given
     * storable.
     *
     * @param values values to build into a key. The length may be less than
     * the amount of primary key properties used by this factory. It must not
     * be less than the difference between rangeStart and rangeEnd.
     * @param rangeStart index of first property to use. Its value must be less
     * than the count of primary key properties.
     * @param rangeEnd index of last property to use, exlusive. Its value must
     * be less than or equal to the count of primary key properties.
     * @return raw search key
     */
    byte[] encodePrimaryKey(Object[] values, int rangeStart, int rangeEnd);

    /**
     * Encode the primary key for when there are no values, but there may be a
     * prefix. Returned value may be null if no prefix is defined.
     */
    byte[] encodePrimaryKeyPrefix();

    /**
     * Used for decoding different generations of Storable. If layout
     * generations are not supported, simply throw a CorruptEncodingException.
     *
     * @param dest storable to receive decoded properties
     * @param generation storable layout generation number
     * @param data decoded into properties, some of which may be dropped if
     * destination storable doesn't have it
     * @throws CorruptEncodingException if generation is unknown or if data cannot be decoded
     * @since 1.2.1
     */
    void decode(S dest, int generation, byte[] data) throws CorruptEncodingException;

    /**
     * Returns the default {@link RawSupport} object that is supplied to
     * Storable instances produced by this codec.
     *
     * @since 1.2
     */
    RawSupport<S> getSupport();
}
