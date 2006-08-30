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

package com.amazon.carbonado.lob;

import java.io.InputStream;
import java.io.OutputStream;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 * Provides access to BLOBs, which are Binary Large OBjects. Consider accessing
 * Blobs within a {@link com.amazon.carbonado.Transaction transaction} scope,
 * to prevent unexpected updates.
 *
 * @author Brian S O'Neill
 * @see Clob
 */
public interface Blob extends Lob {
    /**
     * Returns an InputStream for reading Blob data positioned at the
     * start. The Blob implementation selects an appropriate buffer size for
     * the stream.
     *
     * @return InputStream for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    InputStream openInputStream() throws FetchException;

    /**
     * Returns an InputStream for reading Blob data. The Blob implementation
     * selects an appropriate buffer size for the stream.
     *
     * @param pos desired zero-based position to read from
     * @return InputStream for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    InputStream openInputStream(long pos) throws FetchException;

    /**
     * Returns an InputStream for reading Blob data. A suggested buffer size
     * must be provided, but it might be ignored by the Blob implementation.
     *
     * @param pos desired zero-based position to read from
     * @param bufferSize suggest that the input stream buffer be at least this large (in bytes)
     * @return InputStream for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    InputStream openInputStream(long pos, int bufferSize) throws FetchException;

    /**
     * Returns the length of this Blob, in bytes.
     */
    long getLength() throws FetchException;

    /**
     * Convenience method to capture all the Blob data as a single String,
     * assuming UTF-8 encoding. Call within a transaction scope to ensure the
     * data does not change while the String is being built.
     *
     * @throws IllegalArgumentException if resulting String length would be
     * greater than Integer.MAX_VALUE
     * @throws OutOfMemoryError if not enough memory to hold Blob as a single String
     */
    String asString() throws FetchException;

    /**
     * Convenience method to capture all the Blob data as a single String,
     * decoded against the given charset. Call within a transaction scope to
     * ensure the data does not change while the String is being built.
     *
     * @param charsetName name of character set to decode String
     * @throws IllegalCharsetNameException if the given charset name is illegal
     * @throws IllegalArgumentException if resulting String length would be
     * greater than Integer.MAX_VALUE
     * @throws OutOfMemoryError if not enough memory to hold Blob as a single String
     */
    String asString(String charsetName) throws FetchException;

    /**
     * Convenience method to capture all the Blob data as a single String,
     * decoded against the given charset. Call within a transaction scope to
     * ensure the data does not change while the String is being built.
     *
     * @param charset character set to decode String
     * @throws IllegalArgumentException if resulting String length would be
     * greater than Integer.MAX_VALUE
     * @throws OutOfMemoryError if not enough memory to hold Blob as a single String
     */
    String asString(Charset charset) throws FetchException;

    /**
     * Returns an OutputStream for writing Blob data, positioned at the
     * start. The Blob implementation selects an appropriate buffer size for
     * the stream.
     *
     * @return OutputStream for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    OutputStream openOutputStream() throws PersistException;

    /**
     * Returns an OutputStream for writing Blob data. The Blob implementation
     * selects an appropriate buffer size for the stream.
     *
     * @param pos desired zero-based position to write to
     * @return OutputStream for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    OutputStream openOutputStream(long pos) throws PersistException;

    /**
     * Returns an OutputStream for writing Blob data. A suggested buffer size
     * must be provided, but it might be ignored by the Blob implementation.
     *
     * @param pos desired zero-based position to write to
     * @param bufferSize suggest that the output stream buffer be at least this large (in bytes)
     * @return OutputStream for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    OutputStream openOutputStream(long pos, int bufferSize) throws PersistException;

    /**
     * Set the length of this Blob, in bytes. If the new length is shorter, the
     * Blob is truncated. If the new length is longer, the Blob is padded with
     * zeros.
     *
     * @param length new length to set to
     * @throws IllegalArgumentException if length is negative
     * @throws com.amazon.carbonado.PersistDeniedException if Blob is read-only
     */
    void setLength(long length) throws PersistException;

    /**
     * Convenience method to overwrite all Blob data with the value of a single
     * String, applying UTF-8 encoding. The Blob length may grow or shrink, to
     * match the encoded String value. Call within a transaction scope to
     * ensure the data and length does not change while the value is set.
     *
     * @param value Blob is overwritten with this value
     * @throws IllegalArgumentException if value is null
     */
    void setValue(String value) throws PersistException;

    /**
     * Convenience method to overwrite all Blob data with the value of a single
     * String, applying the given charset encoding. The Blob length may grow or
     * shrink, to match the encoded String value. Call within a transaction
     * scope to ensure the data and length does not change while the value is
     * set.
     *
     * @param value Blob is overwritten with this value
     * @param charsetName name of character set to encode String
     * @throws IllegalCharsetNameException if the given charset name is illegal
     * @throws IllegalArgumentException if value is null
     */
    void setValue(String value, String charsetName) throws PersistException;

    /**
     * Convenience method to overwrite all Blob data with the value of a single
     * String, applying the given charset encoding. The Blob length may grow or
     * shrink, to match the encoded String value. Call within a transaction
     * scope to ensure the data and length does not change while the value is
     * set.
     *
     * @param value Blob is overwritten with this value
     * @param charset character set to encode String
     * @throws IllegalArgumentException if value is null
     */
    void setValue(String value, Charset charset) throws PersistException;
}
