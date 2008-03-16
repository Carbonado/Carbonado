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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import java.nio.charset.Charset;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 * A Clob implementation which is backed by a Blob. Data is stored in the Blob
 * using UTF-16BE encoding.
 *
 * @author Brian S O'Neill
 * @author Bob Loblaw
 */
public class BlobClob extends AbstractClob {
    private static final Charset UTF_16BE = Charset.forName("UTF-16BE");

    private final Blob mBlob;

    /**
     * @param blob blob to wrap
     */
    public BlobClob(Blob blob) {
        mBlob = blob;
    }

    public Reader openReader() throws FetchException {
        return new BufferedReader(new InputStreamReader(mBlob.openInputStream(), UTF_16BE));
    }

    public Reader openReader(long pos) throws FetchException {
        return openReader(pos, -1);
    }

    public Reader openReader(long pos, int bufferSize) throws FetchException {
        rangeCheck(pos, "Position");
        Reader reader = new InputStreamReader(mBlob.openInputStream(pos << 1, 0), UTF_16BE);
        if (bufferSize < 0) {
            reader = new BufferedReader(reader);
        } else if (bufferSize > 0) {
            reader = new BufferedReader(reader, bufferSize);
        }
        return reader;
    }

    public long getLength() throws FetchException {
        return mBlob.getLength() >> 1;
    }

    public Writer openWriter() throws PersistException {
        return new BufferedWriter(new OutputStreamWriter(mBlob.openOutputStream(), UTF_16BE));
    }

    public Writer openWriter(long pos) throws PersistException {
        return openWriter(pos, -1);
    }

    public Writer openWriter(long pos, int bufferSize) throws PersistException {
        rangeCheck(pos, "Position");
        Writer writer = new OutputStreamWriter(mBlob.openOutputStream(pos << 1, 0), UTF_16BE);
        if (bufferSize < 0) {
            writer = new BufferedWriter(writer);
        } else if (bufferSize > 0) {
            writer = new BufferedWriter(writer, bufferSize);
        }
        return writer;
    }

    public void setLength(long length) throws PersistException {
        rangeCheck(length, "Length");
        mBlob.setLength(length << 1);
    }

    public Object getLocator() {
        return mBlob.getLocator();
    }

    protected Blob getWrappedBlob() {
        return mBlob;
    }

    private void rangeCheck(long value, String type) {
        if (value < 0) {
            throw new IllegalArgumentException(type + " is negative: " + value);
        }
        if (value > (Long.MAX_VALUE >> 1)) {
            throw new IllegalArgumentException(type + " too large: " + value);
        }
    }
}
