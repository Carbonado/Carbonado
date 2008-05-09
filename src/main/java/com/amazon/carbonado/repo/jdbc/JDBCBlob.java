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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.sql.SQLException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;

import com.amazon.carbonado.lob.AbstractBlob;

/**
 *
 *
 * @author Brian S O'Neill
 */
class JDBCBlob extends AbstractBlob implements JDBCLob {
    private static final int DEFAULT_BUFFER = 4000;

    protected final JDBCRepository mRepo;
    private java.sql.Blob mBlob;
    private final JDBCBlobLoader mLoader;

    JDBCBlob(JDBCRepository repo, java.sql.Blob blob, JDBCBlobLoader loader) {
        super(repo);
        mRepo = repo;
        mBlob = blob;
        mLoader = loader;
    }

    // FIXME: I/O streams must have embedded transaction

    public InputStream openInputStream() throws FetchException {
        try {
            return getInternalBlobForFetch().getBinaryStream();
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public InputStream openInputStream(long pos) throws FetchException {
        try {
            if (pos == 0) {
                return getInternalBlobForFetch().getBinaryStream();
            }
            return new Input(getInternalBlobForFetch(), DEFAULT_BUFFER, pos);
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public InputStream openInputStream(long pos, int bufferSize) throws FetchException {
        try {
            if (pos == 0) {
                return getInternalBlobForFetch().getBinaryStream();
            }
            if (bufferSize <= 0) {
                bufferSize = DEFAULT_BUFFER;
            }
            return new Input(getInternalBlobForFetch(), bufferSize, pos);
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public long getLength() throws FetchException {
        try {
            return getInternalBlobForFetch().length();
        } catch (SQLException e) {
            throw mRepo.toFetchException(e);
        }
    }

    public OutputStream openOutputStream() throws PersistException {
        return openOutputStream(0);
    }

    public OutputStream openOutputStream(long pos) throws PersistException {
        try {
            return getInternalBlobForPersist().setBinaryStream(pos);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    public OutputStream openOutputStream(long pos, int bufferSize) throws PersistException {
        return openOutputStream(pos);
    }

    public void setLength(long length) throws PersistException {
        // FIXME: Add special code to support increasing length
        try {
            getInternalBlobForPersist().truncate(length);
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        }
    }

    public Object getLocator() {
        // FIXME
        return null;
    }

    public void close() {
        mBlob = null;
    }

    java.sql.Blob getInternalBlobForFetch() throws FetchException {
        if (mBlob == null) {
            if ((mBlob = mLoader.load(mRepo)) == null) {
                throw new FetchNoneException("Blob value is null");
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
        return mBlob;
    }

    java.sql.Blob getInternalBlobForPersist() throws PersistException {
        if (mBlob == null) {
            try {
                if ((mBlob = mLoader.load(mRepo)) == null) {
                    throw new PersistNoneException("Blob value is null");
                }
                JDBCTransaction txn = mRepo.localTransactionScope().getTxn();
                if (txn != null) {
                    txn.register(this);
                }
            } catch (Exception e) {
                throw mRepo.toPersistException(e);
            }
        }
        return mBlob;
    }

    private static class Input extends InputStream {
        private final java.sql.Blob mBlob;
        private final int mBufferSize;

        private long mPos;
        private byte[] mBuffer;
        private int mBufferPos;

        Input(java.sql.Blob blob, int bufferSize, long pos) {
            mBlob = blob;
            mBufferSize = bufferSize;
            mPos = pos;
        }

        @Override
        public int read() throws IOException {
            if (fillBuffer() <= 0) {
                return -1;
            }
            return mBuffer[mBufferPos++];
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int avail = fillBuffer();
            if (avail <= 0) {
                return -1;
            }
            if (len > avail) {
                len = avail;
            }
            System.arraycopy(mBuffer, mBufferPos, b, off, len);
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
                length = mBlob.length();
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
            if (mBuffer == null || newBufferPos >= mBuffer.length) {
                mBuffer = null;
                mBufferPos = 0;
            } else {
                mBufferPos = (int) newBufferPos;
            }
            mPos = newPos;
            return n;
        }

        private int fillBuffer() throws IOException {
            try {
                if (mBuffer == null || mBufferPos >= mBuffer.length) {
                    mBuffer = mBlob.getBytes(mPos, mBufferSize);
                    mPos += mBuffer.length;
                    mBufferPos = 0;
                }
                return mBuffer.length - mBufferPos;
            } catch (SQLException e) {
                IOException ioe = new IOException();
                ioe.initCause(e);
                throw ioe;
            }
        }
    }
}
