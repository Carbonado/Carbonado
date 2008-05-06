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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistDeniedException;
import com.amazon.carbonado.PersistException;

/**
 * Implementation of a Clob which is backed by a read-only String.
 *
 * @author Brian S O'Neill
 */
public class StringClob extends AbstractClob {
    // TODO: Make this copy-on-write via internal CharArrayClob.

    private final String mStr;

    public StringClob(String str) {
        mStr = str;
    }

    public Reader openReader() {
        return new StringReader(mStr);
    }

    public Reader openReader(long pos) throws FetchException {
        StringReader r = new StringReader(mStr);
        try {
            r.skip(pos);
        } catch (IOException e) {
            throw new FetchException(e);
        }
        return r;
    }

    public Reader openReader(long pos, int bufferSize) throws FetchException {
        return openReader(pos);
    }

    public long getLength() throws FetchException {
        return mStr.length();
    }

    @Override
    public String asString() {
        return mStr;
    }

    public Writer openWriter() throws PersistException {
        throw denied();
    }

    public Writer openWriter(long pos) throws PersistException {
        throw denied();
    }

    public Writer openWriter(long pos, int bufferSize) throws PersistException {
        throw denied();
    }

    public void setLength(long length) throws PersistException {
        throw denied();
    }

    @Override
    public void setValue(String value) throws PersistException {
        denied();
    }

    /**
     * Always returns null.
     */
    public Object getLocator() {
        return null;
    }

    private PersistException denied() {
        return new PersistDeniedException("Read-only");
    }
}
