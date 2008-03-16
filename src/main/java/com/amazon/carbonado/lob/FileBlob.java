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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

import com.amazon.carbonado.spi.RAFInputStream;
import com.amazon.carbonado.spi.RAFOutputStream;

/**
 * Implementation of a Blob which is backed by a File.
 *
 * @author Brian S O'Neill
 */
public class FileBlob extends AbstractBlob {
    private final File mFile;

    public FileBlob(File file) {
        mFile = file;
    }

    public InputStream openInputStream() throws FetchException {
        return openInputStream(0, -1);
    }

    public InputStream openInputStream(long pos) throws FetchException {
        return openInputStream(pos, -1);
    }

    public InputStream openInputStream(long pos, int bufferSize) throws FetchException {
        try {
            RandomAccessFile raf = new RandomAccessFile(mFile, "r");
            if (pos != 0) {
                raf.seek(pos);
            }
            InputStream in = new RAFInputStream(raf);
            if (bufferSize < 0) {
                in = new BufferedInputStream(in);
            } else if (bufferSize > 0) {
                in = new BufferedInputStream(in, bufferSize);
            }
            return in;
        } catch (IOException e) {
            throw new FetchException(e);
        }
    }

    public long getLength() throws FetchException {
        return mFile.length();
    }

    public OutputStream openOutputStream() throws PersistException {
        return openOutputStream(0, -1);
    }

    public OutputStream openOutputStream(long pos) throws PersistException {
        return openOutputStream(pos, -1);
    }

    public OutputStream openOutputStream(long pos, int bufferSize) throws PersistException {
        try {
            RandomAccessFile raf = new RandomAccessFile(mFile, "rw");
            if (pos != 0) {
                raf.seek(pos);
            }
            OutputStream out = new RAFOutputStream(raf);
            if (bufferSize < 0) {
                out = new BufferedOutputStream(out);
            } else if (bufferSize > 0) {
                out = new BufferedOutputStream(out, bufferSize);
            }
            return out;
        } catch (IOException e) {
            throw new PersistException(e);
        }
    }

    public void setLength(long length) throws PersistException {
        try {
            RandomAccessFile raf = new RandomAccessFile(mFile, "rw");
            raf.setLength(length);
            raf.close();
        } catch (IOException e) {
            throw new PersistException(e);
        }
    }

    /**
     * Always returns null.
     */
    public Object getLocator() {
        return null;
    }
}
