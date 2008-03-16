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

import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Transaction;

/**
 * AbstractBlob implements a small set of common Blob methods.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractBlob implements Blob {
    private static Charset UTF_8 = Charset.forName("UTF-8");

    private final Repository mRepo;

    protected AbstractBlob() {
        mRepo = null;
    }

    /**
     * Use of this constructor indicates that setValue should operate within a
     * transaction. A Repository is passed in for entering the transaction.
     *
     * @param repo optional repository to use for performing string conversion
     * within transactions
     */
    protected AbstractBlob(Repository repo) {
        mRepo = repo;
    }

    public String asString() throws FetchException {
        return asString(UTF_8);
    }

    public String asString(String charsetName) throws FetchException {
        return asString(Charset.forName(charsetName));
    }

    public String asString(Charset charset) throws FetchException {
        Transaction txn = mRepo == null ? null : mRepo.enterTransaction();
        try {
            long length = getLength();

            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException
                    ("Blob is too long to fit in a String: " + length);
            }

            if (length <= 0) {
                return "";
            }

            CharsetDecoder decoder = charset.newDecoder();

            long charLength = (long) (decoder.averageCharsPerByte() * length);

            if (charLength > Integer.MAX_VALUE) {
                charLength = Integer.MAX_VALUE;
            }

            char[] buffer = new char[(int) charLength];

            try {
                Reader r = new InputStreamReader(openInputStream(), decoder);

                try {
                    int offset = 0;
                    int amt;
                    while ((amt = r.read(buffer, offset, buffer.length - offset)) >= 0) {
                        offset += amt;
                        if (amt == 0 && offset >= buffer.length) {
                            // Expand capacity.
                            charLength *= 2;
                            if (charLength >= Integer.MAX_VALUE) {
                                charLength = Integer.MAX_VALUE;
                            }
                            if (charLength <= buffer.length) {
                                throw new IllegalArgumentException
                                    ("Blob is too long to fit in a String");
                            }
                            char[] newBuffer = new char[(int) charLength];
                            System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
                            buffer = newBuffer;
                        }
                    }

                    return new String(buffer, 0, offset);
                } finally {
                    r.close();
                }
            } catch (IOException e) {
                throw AbstractClob.toFetchException(e);
            }
        } finally {
            if (txn != null) {
                try {
                    txn.exit();
                } catch (PersistException e) {
                    // Don't care.
                }
            }
        }
    }

    public void setValue(String value) throws PersistException {
        setValue(value, UTF_8);
    }

    public void setValue(String value, String charsetName) throws PersistException {
        setValue(value, Charset.forName(charsetName));
    }

    public void setValue(String value, Charset charset) throws PersistException {
        if (value == null) {
            throw new IllegalArgumentException("Blob value cannot be null");
        }

        if (mRepo == null) {
            setLength(0);
            try {
                Writer w = new OutputStreamWriter(openOutputStream(), charset);
                try {
                    w.write(value);
                } finally {
                    w.close();
                }
            } catch (IOException e) {
                throw AbstractClob.toPersistException(e);
            }
        } else {
            Transaction txn = mRepo.enterTransaction();
            try {
                long originalLength = getLength();

                int newLength;
                try {
                    DataOutputStream out = new DataOutputStream(openOutputStream());
                    Writer w = new OutputStreamWriter(out, charset);
                    try {
                        w.write(value);
                    } finally {
                        w.close();
                    }
                    newLength = out.size();
                } catch (IOException e) {
                    throw AbstractClob.toPersistException(e);
                }

                if (newLength < originalLength) {
                    // Truncate.
                    setLength(newLength);

                    // Note: DataOutputStream counts bytes written as an int
                    // instead of a long. If String is composed of high unicode
                    // characters and encoding is UTF-8 is used, then the maximum
                    // supported String length is 715,827,882 characters. I don't
                    // expect this to be a real problem however, since the String
                    // will consume over 1.3 gigabytes of memory.
                }

                txn.commit();
            } catch (FetchException e) {
                throw e.toPersistException();
            } finally {
                txn.exit();
            }
        }
    }

    @Override
    public int hashCode() {
        Object locator = getLocator();
        return locator == null ? super.hashCode() : locator.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof AbstractBlob) {
            Object locator = getLocator();
            if (locator != null) {
                AbstractBlob other = (AbstractBlob) obj;
                return locator == other.getLocator();
            }
        }
        return false;
    }

    @Override
    public String toString() {
        Object locator = getLocator();
        return locator == null ? super.toString() : ("Blob@" + locator);
    }
}
