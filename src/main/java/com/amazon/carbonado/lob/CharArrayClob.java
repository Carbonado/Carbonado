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
import java.io.Writer;

import java.util.Arrays;

import com.amazon.carbonado.PersistException;

/**
 * Implementation of a Clob which is backed by a growable in-memory character
 * array.
 *
 * @author Brian S O'Neill
 */
public class CharArrayClob extends AbstractClob {

    private final int mInitialCapacity;
    private char[] mData;
    private int mLength;

    /**
     * Construct a CharArrayClob with the given initial capacity.
     *
     * @param capacity initial capacity of internal character array
     */
    public CharArrayClob(int capacity) {
        if (capacity == 0) {
            throw new IllegalArgumentException();
        }
        mInitialCapacity = capacity;
        mData = new char[capacity];
    }

    /**
     * Construct a CharArrayClob initially backed by the given character array. The
     * character array is not cloned until this CharArrayClob grows or shrinks.
     *
     * @param data initial data backing the Clob
     */
    public CharArrayClob(char[] data) {
        if (data.length == 0) {
            throw new IllegalArgumentException();
        }
        mLength = mInitialCapacity = data.length;
        mData = data;
    }

    /**
     * Construct a CharArrayClob initially backed by the given character array. The
     * character array is not cloned until this CharArrayClob grows or shrinks.
     *
     * @param data initial data backing the Clob
     * @param length initial length of data
     */
    public CharArrayClob(char[] data, int length) {
        if (data.length < length) {
            throw new IllegalArgumentException();
        }
        mInitialCapacity = data.length;
        mData = data;
        mLength = length;
    }

    public Reader openReader() {
        return new Input(this, 0);
    }

    public Reader openReader(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        return new Input(this, pos);
    }

    public Reader openReader(long pos, int bufferSize) {
        return openReader(pos);
    }

    public synchronized long getLength() {
        return mLength;
    }

    @Override
    public synchronized String asString() {
        return new String(mData, 0, mLength);
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

    synchronized int read(long pos, char[] chars) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (chars == null) {
            throw new IllegalArgumentException("Character array is null");
        }

        if (pos > Integer.MAX_VALUE) {
            return -1;
        }

        int ipos = (int) pos;
        if (ipos > mLength) { // the use of '>' instead of '>=' is intentional
            return -1;
        }

        int length = chars.length;
        if (ipos + length > mLength) {
            length = mLength - ipos;
        }

        if (length > 0) {
            System.arraycopy(mData, ipos, chars, 0, length);
            return length;
        } else {
            return -1;
        }
    }

    synchronized int read(long pos, char[] chars, int offset, int length) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (chars == null) {
            throw new IllegalArgumentException("Character array is null");
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
                System.arraycopy(mData, ipos, chars, offset, length);
            } catch (IndexOutOfBoundsException e) {
                if (offset >= chars.length && length > 0) {
                    throw new IllegalArgumentException("Offset is too large: " + offset);
                }
                throw e;
            }
            return length;
        } else {
            return -1;
        }
    }

    public Writer openWriter() {
        return new Output(this, 0);
    }

    public Writer openWriter(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        return new Output(this, pos);
    }

    public Writer openWriter(long pos, int bufferSize) {
        return openWriter(pos);
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
                mData = new char[mInitialCapacity];
            }
        } else if (ilength > mLength) {
            if (ilength <= mData.length) {
                Arrays.fill(mData, mLength, ilength, (char) 0);
                mLength = ilength;
            } else {
                int newLength = mData.length * 2;
                if (newLength < ilength) {
                    newLength = ilength;
                }
                char[] newData = new char[newLength];
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
        mData[ipos] = (char) b;
    }

    synchronized void write(long pos, char[] chars) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (chars == null) {
            throw new IllegalArgumentException("Character array is null");
        }

        if (pos > Integer.MAX_VALUE) {
            throw new IOException("Position too high: " + pos);
        }
        if (pos + chars.length > Integer.MAX_VALUE) {
            throw new IOException("Position plus length too high: " + (pos + chars.length));
        }

        int ipos = (int) pos;
        ensureLengthForWrite(ipos + chars.length);
        System.arraycopy(chars, 0, mData, ipos, chars.length);
    }

    synchronized void write(long pos, char[] chars, int offset, int length) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Position is negative: " + pos);
        }
        if (chars == null) {
            throw new IllegalArgumentException("Character array is null");
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
        System.arraycopy(chars, offset, mData, ipos, length);
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
                char[] newData = new char[newLength];
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

    private static class Input extends Reader {
        private final CharArrayClob mClob;
        private long mPos;
        private long mMarkPos;

        Input(CharArrayClob blob, long pos) {
            mClob = blob;
            mPos = pos;
            mMarkPos = pos;
        }

        @Override
        public int read() {
            int b = mClob.read(mPos);
            if (b >= 0) {
                mPos++;
            }
            return b;
        }

        @Override
        public int read(char[] chars) {
            int length = mClob.read(mPos, chars);
            if (length > 0) {
                mPos += length;
            }
            return length;
        }

        @Override
        public int read(char[] chars, int offset, int length) {
            length = mClob.read(mPos, chars, offset, length);
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

            long length = mClob.getLength();
            if (newPos > length) {
                newPos = length;
            }

            n = newPos - mPos;
            mPos = newPos;
            return n;
        }

        @Override
        public boolean ready() {
            return (mClob.getLength() - mPos) > 0;
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

        @Override
        public void close() {
        }
    }

    private static class Output extends Writer {
        private final CharArrayClob mClob;
        private long mPos;

        Output(CharArrayClob blob, long pos) {
            mClob = blob;
            mPos = pos;
        }

        @Override
        public void write(int b) throws IOException {
            mClob.write(mPos, b);
            mPos++;
        }

        @Override
        public void write(char[] b) throws IOException {
            mClob.write(mPos, b);
            mPos += b.length;
        }

        @Override
        public void write(char[] b, int offset, int length) throws IOException {
            mClob.write(mPos, b, offset, length);
            mPos += length;
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
