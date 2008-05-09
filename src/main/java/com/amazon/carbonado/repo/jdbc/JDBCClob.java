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

package com.amazon.carbonado.repo.jdbc;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;

import java.sql.SQLException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;

import com.amazon.carbonado.lob.AbstractClob;

/**
 *
 *
 * @author Brian S O'Neill
 */
class JDBCClob extends AbstractClob implements JDBCLob {
    private static final int DEFAULT_BUFFER = 4000;

    protected final JDBCRepository mRepo;
    private java.sql.Clob mClob;
    private final JDBCClobLoader mLoader;

    JDBCClob(JDBCRepository repo, java.sql.Clob clob, JDBCClobLoader loader) {
        super(repo);
        mRepo = repo;
        mClob = clob;
        mLoader = loader;
    }

    // FIXME: I/O streams must have embedded transaction

    public Reader openReader() throws FetchException {
        try {
            return getInternalClobForFetch().getCharacterStream();
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public Reader openReader(long pos) throws FetchException {
        try {
            if (pos == 0) {
                return getInternalClobForFetch().getCharacterStream();
            }
            return new Input(getInternalClobForFetch(), DEFAULT_BUFFER, pos);
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public Reader openReader(long pos, int bufferSize) throws FetchException {
        try {
            if (pos == 0) {
                return getInternalClobForFetch().getCharacterStream();
            }
            if (bufferSize <= 0) {
                bufferSize = DEFAULT_BUFFER;
            }
            return new Input(getInternalClobForFetch(), bufferSize, pos);
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public long getLength() throws FetchException {
        try {
            return getInternalClobForFetch().length();
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public Writer openWriter() throws PersistException {
        return openWriter(0);
    }

    public Writer openWriter(long pos) throws PersistException {
        try {
            return getInternalClobForPersist().setCharacterStream(pos);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    public Writer openWriter(long pos, int bufferSize) throws PersistException {
        return openWriter(pos);
    }

    public void setLength(long length) throws PersistException {
        // FIXME: Add special code to support increasing length
        try {
            getInternalClobForPersist().truncate(length);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    public Object getLocator() {
        // FIXME
        return null;
    }

    public void close() {
        mClob = null;
    }

    java.sql.Clob getInternalClobForFetch() throws FetchException {
        if (mClob == null) {
            if ((mClob = mLoader.load(mRepo)) == null) {
                throw new FetchNoneException("Clob value is null");
            }
            try {
                JDBCTransaction txn = mRepo.localTransactionScope().getTxn();
                if (txn != null) {
                    txn.register(this);
                }
            } catch (Exception e) {
                throw mRepo.toFetchException(e);
            }
        }
        return mClob;
    }

    java.sql.Clob getInternalClobForPersist() throws PersistException {
        if (mClob == null) {
            try {
                if ((mClob = mLoader.load(mRepo)) == null) {
                    throw new PersistNoneException("Clob value is null");
                }
                JDBCTransaction txn = mRepo.localTransactionScope().getTxn();
                if (txn != null) {
                    txn.register(this);
                }
            } catch (Exception e) {
                throw mRepo.toPersistException(e);
            }
        }
        return mClob;
    }

    private static class Input extends Reader {
        private final java.sql.Clob mClob;
        private final int mBufferSize;

        private long mPos;
        private String mBuffer;
        private int mBufferPos;

        Input(java.sql.Clob clob, int bufferSize, long pos) {
            mClob = clob;
            mBufferSize = bufferSize;
            mPos = pos;
        }

        @Override
        public int read() throws IOException {
            if (fillBuffer() <= 0) {
                return -1;
            }
            return mBuffer.charAt(mBufferPos++);
        }

        @Override
        public int read(char[] c, int off, int len) throws IOException {
            int avail = fillBuffer();
            if (avail <= 0) {
                return -1;
            }
            if (len > avail) {
                len = avail;
            }
            mBuffer.getChars(mBufferPos, mBufferPos + len, c, off);
            mBufferPos += len;
            return len;
        }

        @Override
        public long skip(long n) throws IOException {
            if (n <= 0) {
                return 0;
            }
            long newPos = mPos + n;
            long length;
            try {
                length = mClob.length();
            } catch (SQLException e) {
                IOException ioe = new IOException();
                ioe.initCause(e);
                throw ioe;
            }
            if (newPos >= length) {
                newPos = length;
                n = newPos - mPos;
            }
            long newBufferPos = mBufferPos + n;
            if (mBuffer == null || newBufferPos >= mBuffer.length()) {
                mBuffer = null;
                mBufferPos = 0;
            } else {
                mBufferPos = (int) newBufferPos;
            }
            mPos = newPos;
            return n;
        }

        @Override
        public void close() {
        }

        private int fillBuffer() throws IOException {
            try {
                if (mBuffer == null || mBufferPos >= mBuffer.length()) {
                    mBuffer = mClob.getSubString(mPos, mBufferSize);
                    mPos += mBuffer.length();
                    mBufferPos = 0;
                }
                return mBuffer.length() - mBufferPos;
            } catch (SQLException e) {
                IOException ioe = new IOException();
                ioe.initCause(e);
                throw ioe;
            }
        }
    }
}
