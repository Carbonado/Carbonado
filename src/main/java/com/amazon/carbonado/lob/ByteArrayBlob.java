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
import java.io.IOException;
import java.io.OutputStream;

import java.util.Arrays;

import com.amazon.carbonado.PersistException;

/**
 * Implementation of a Blob which is backed by a growable in-memory byte array.
 *
 * @author Brian S O'Neill
 */
public class ByteArrayBlob extends AbstractBlob {

    private final int mInitialCapacity;
    private byte[] mData;
    private int mLength;

    /**
     * Construct a ByteArrayBlob with the given initial capacity.
     *
     * @param capacity initial capacity of internal byte array
     */
    public ByteArrayBlob(int capacity) {
        if (capacity == 0) {
            throw new IllegalArgumentException();
        }
        mInitialCapacity = capacity;
        mData = new byte[capacity];
    }

    /**
     * Construct a ByteArrayBlob initially backed by the given byte array. The
     * byte array is not cloned until this ByteArrayBlob grows or shrinks.
     *
     * @param data initial data backing the Blob
     */
    public ByteArrayBlob(byte[] data) {
        if (data.length == 0) {
            throw new IllegalArgumentException();
        }
        mLength = mInitialCapacity = data.length;
        mData = data;
    }

    /**
     * Construct a ByteArrayBlob initially backed by the given byte array. The
     * byte array is not cloned until this ByteArrayBlob grows or shrinks.
     *
     * @param data initial data backing the Blob
     * @param length initial length of data
     */
    public ByteArrayBlob(byte[] data, int length) {
        if (data.length < length) {
            throw new IllegalArgumentException();
        }
        mInitialCapacity = data.length;
        mData = data;
        mLength = length;
    }

    public InputStream openInputStream() {
        return new Input(this, 0);
    }

    public InputStream openInputStream(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        return new Input(this, pos);
    }

    public InputStream openInputStream(long pos, int bufferSize) {
        return openInputStream(pos);
    }

    public synchronized long getLength() {
        return mLength;
    }

    synchronized int read(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }

        if (pos > Integer.MAX_VALUE) {
            return -1;
        }

        int ipos = (int) pos;
        if (ipos >= mLength) {
            return -1;
        }

        return mData[ipos];
    }

    synchronized int read(long pos, byte[] bytes) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array is null");
        }

        if (pos > Integer.MAX_VALUE) {
            return -1;
        }

        int ipos = (int) pos;
        if (ipos > mLength) { // the use of '>' instead of '>=' is intentional
            return -1;
        }

        int length = bytes.length;
        if (ipos + length > mLength) {
            length = mLength - ipos;
        }

        if (length > 0) {
            System.arraycopy(mData, ipos, bytes, 0, length);
            return length;
        } else {
            return -1;
        }
    }

    synchronized int read(long pos, byte[] bytes, int offset, int length) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array is null");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset is negative: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length is negative: " + length);
        }

        if (pos > Integer.MAX_VALUE) {
            return -1;
        }

        int ipos = (int) pos;
        if (ipos > mLength) { // the use of '>' instead of '>=' is intentional
            return -1;
        }

        if (ipos + length > mLength) {
            length = mLength - ipos;
        }

        if (length > 0) {
            try {
                System.arraycopy(mData, ipos, bytes, offset, length);
            } catch (IndexOutOfBoundsException e) {
                if (offset >= bytes.length && length > 0) {
                    throw new IllegalArgumentException("Offset is too large: " + offset);
                }
                throw e;
            }
            return length;
        } else {
            return -1;
        }
    }

    public OutputStream openOutputStream() {
        return new Output(this, 0);
    }

    public OutputStream openOutputStream(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        return new Output(this, pos);
    }

    public OutputStream openOutputStream(long pos, int bufferSize) {
        return openOutputStream(pos);
    }

    public synchronized void setLength(long length) throws PersistException {
        if (length < 0) {
            throw new IllegalArgumentException("Length is negative: " + length);
        }
        if (length > Integer.MAX_VALUE) {
            throw new PersistException("Length too long: " + length);
        }
        int ilength = (int) length;
        if (ilength < mLength) {
            mLength = ilength;
            if (mData.length > mInitialCapacity) {
                // Free up some space.
                mData = new byte[mInitialCapacity];
            }
        } else if (ilength > mLength) {
            if (ilength <= mData.length) {
                Arrays.fill(mData, mLength, ilength, (byte) 0);
                mLength = ilength;
            } else {
                int newLength = mData.length * 2;
                if (newLength < ilength) {
                    newLength = ilength;
                }
                byte[] newData = new byte[newLength];
                System.arraycopy(mData, 0, newData, 0, mLength);
            }
            mLength = ilength;
        }
    }

    synchronized void write(long pos, int b) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }

        if (pos > Integer.MAX_VALUE) {
            throw new IOException("Position too high: " + pos);
        }

        int ipos = (int) pos;
        ensureLengthForWrite(ipos + 1);
        mData[ipos] = (byte) b;
    }

    synchronized void write(long pos, byte[] bytes) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array is null");
        }

        if (pos > Integer.MAX_VALUE) {
            throw new IOException("Position too high: " + pos);
        }
        if (pos + bytes.length > Integer.MAX_VALUE) {
            throw new IOException("Position plus length too high: " + (pos + bytes.length));
        }

        int ipos = (int) pos;
        ensureLengthForWrite(ipos + bytes.length);
        System.arraycopy(bytes, 0, mData, ipos, bytes.length);
    }

    synchronized void write(long pos, byte[] bytes, int offset, int length) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array is null");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset is negative: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length is negative: " + length);
        }

        if (pos > Integer.MAX_VALUE) {
            throw new IOException("Position too high: " + pos);
        }
        if (pos + length > Integer.MAX_VALUE) {
            throw new IOException("Position plus length too high: " + (pos + length));
        }

        int ipos = (int) pos;
        ensureLengthForWrite(ipos + length);
        System.arraycopy(bytes, offset, mData, ipos, length);
    }

    // Caller must be synchronized
    private void ensureLengthForWrite(int ilength) {
        if (ilength > mLength) {
            if (ilength <= mData.length) {
                mLength = ilength;
            } else {
                int newLength = mData.length * 2;
                if (newLength < ilength) {
                    newLength = ilength;
                }
                byte[] newData = new byte[newLength];
                System.arraycopy(mData, 0, newData, 0, mLength);
                mData = newData;
            }
            mLength = ilength;
        }
    }

    /**
     * Always returns null.
     */
    public Object getLocator() {
        return null;
    }

    private static class Input extends InputStream {
        private final ByteArrayBlob mBlob;
        private long mPos;
        private long mMarkPos;

        Input(ByteArrayBlob blob, long pos) {
            mBlob = blob;
            mPos = pos;
            mMarkPos = pos;
        }

        @Override
        public int read() {
            int b = mBlob.read(mPos);
            if (b >= 0) {
                mPos++;
            }
            return b;
        }

        @Override
        public int read(byte[] bytes) {
            int length = mBlob.read(mPos, bytes);
            if (length > 0) {
                mPos += length;
            }
            return length;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            length = mBlob.read(mPos, bytes, offset, length);
            if (length > 0) {
                mPos += length;
            }
            return length;
        }

        @Override
        public long skip(long n) {
            if (n <= 0) {
                return 0;
            }

            long newPos = mPos + n;
            if (newPos < 0) {
                newPos = Long.MAX_VALUE;
            }

            long length = mBlob.getLength();
            if (newPos > length) {
                newPos = length;
            }

            n = newPos - mPos;
            mPos = newPos;
            return n;
        }

        @Override
        public int available() {
            long avail = mBlob.getLength() - mPos;
            if (avail > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) avail;
        }

        @Override
        public void mark(int readlimit) {
            mMarkPos = mPos;
        }

        @Override
        public void reset() {
            mPos = mMarkPos;
        }

        @Override
        public boolean markSupported() {
            return true;
        }
    }

    private static class Output extends OutputStream {
        private final ByteArrayBlob mBlob;
        private long mPos;

        Output(ByteArrayBlob blob, long pos) {
            mBlob = blob;
            mPos = pos;
        }

        @Override
        public void write(int b) throws IOException {
            mBlob.write(mPos, b);
            mPos++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            mBlob.write(mPos, b);
            mPos += b.length;
        }

        @Override
        public void write(byte[] b, int offset, int length) throws IOException {
            mBlob.write(mPos, b, offset, length);
            mPos += length;
        }
    }
}
