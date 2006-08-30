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

import java.io.Reader;
import java.io.Writer;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 * Provides access to CLOBs, which are Character Large OBjects. Consider
 * accessing Clobs within a {@link com.amazon.carbonado.Transaction
 * transaction} scope, to prevent unexpected updates.
 *
 * @author Brian S O'Neill
 * @see Blob
 */
public interface Clob extends Lob {
    /**
     * Returns a Reader for reading Clob data, positioned at the start. The
     * Clob implementation selects an appropriate buffer size for the reader.
     *
     * @return Reader for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    Reader openReader() throws FetchException;

    /**
     * Returns a Reader for reading Clob data. The Clob implementation selects
     * an appropriate buffer size for the reader.
     *
     * @param pos desired zero-based position to read from
     * @return Reader for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    Reader openReader(long pos) throws FetchException;

    /**
     * Returns a Reader for reading Clob data. A suggested buffer size must be
     * provided, but it might be ignored by the Clob implementation.
     *
     * @param pos desired zero-based position to read from
     * @param bufferSize suggest that the reader buffer be at least this large (in characters)
     * @return Reader for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    Reader openReader(long pos, int bufferSize) throws FetchException;

    /**
     * Returns the length of this Clob, in characters.
     */
    long getLength() throws FetchException;

    /**
     * Convenience method to capture all the Clob data as a single String. Call
     * within a transaction scope to ensure the data does not change while the
     * String is being built.
     *
     * @throws IllegalArgumentException if Clob length is greater than Integer.MAX_VALUE
     * @throws OutOfMemoryError if not enough memory to hold Clob as a single String
     */
    String asString() throws FetchException;

    /**
     * Returns a Writer for writing Clob data, positioned at the start. The
     * Clob implementation selects an appropriate buffer size for the writer.
     *
     * @return Writer for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    Writer openWriter() throws PersistException;

    /**
     * Returns a Writer for writing Clob data. The Clob implementation selects
     * an appropriate buffer size for the writer.
     *
     * @param pos desired zero-based position to write to
     * @return Writer for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    Writer openWriter(long pos) throws PersistException;

    /**
     * Returns a Writer for writing Clob data. A suggested buffer size must be
     * provided, but it might be ignored by the Clob implementation.
     *
     * @param pos desired zero-based position to write to
     * @param bufferSize suggest that the writer buffer be at least this large (in characters)
     * @return Writer for this Blob, which is not guaranteed to be thread-safe
     * @throws IllegalArgumentException if position is negative
     */
    Writer openWriter(long pos, int bufferSize) throws PersistException;

    /**
     * Set the length of this Clob, in characters. If the new length is
     * shorter, the Clob is truncated. If the new length is longer, the Clob is
     * padded with '\0' characters.
     *
     * @param length new length to set to
     * @throws IllegalArgumentException if length is negative
     * @throws com.amazon.carbonado.PersistDeniedException if Clob is read-only
     */
    void setLength(long length) throws PersistException;

    /**
     * Convenience method to overwrite all Clob data with the value of a single
     * String. The Clob length may grow or shrink, to match the String
     * value. Call within a transaction scope to ensure the data and length
     * does not change while the value is set.
     *
     * @param value Clob is overwritten with this value
     * @throws IllegalArgumentException if value is null
     */
    void setValue(String value) throws PersistException;
}
