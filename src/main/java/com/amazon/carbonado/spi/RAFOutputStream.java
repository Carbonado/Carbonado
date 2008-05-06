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

package com.amazon.carbonado.spi;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * OutputStream that wraps a RandomAccessFile. A stream can be obtained for a
 * RandomAccessFile by getting the file descriptor and creating a
 * FileOutputStream on it. Problem is that FileOutputStream has a finalizer
 * that closes the RandomAccessFile.
 *
 * @author Brian S O'Neill
 */
public class RAFOutputStream extends OutputStream {
    private final RandomAccessFile mRAF;

    public RAFOutputStream(RandomAccessFile raf) {
        mRAF = raf;
    }

    @Override
    public void write(int b) throws IOException {
        mRAF.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        mRAF.write(b);
    }

    @Override
    public void write(byte[] b, int offset, int length) throws IOException {
        mRAF.write(b, offset, length);
    }

    @Override
    public void close() throws IOException {
        mRAF.close();
    }
}
