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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Transaction;

/**
 * AbstractClob implements a small set of common Clob methods.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractClob implements Clob {
    static FetchException toFetchException(IOException e) {
        Throwable cause = e.getCause();
        if (cause instanceof FetchException) {
            return (FetchException) cause;
        }
        if (cause == null) {
            cause = e;
        }
        return new FetchException(cause);
    }

    static PersistException toPersistException(IOException e) {
        Throwable cause = e.getCause();
        if (cause instanceof PersistException) {
            return (PersistException) cause;
        }
        if (cause == null) {
            cause = e;
        }
        return new PersistException(cause);
    }

    private final Repository mRepo;

    protected AbstractClob() {
        mRepo = null;
    }

    /**
     * Use of this constructor indicates that setValue should operate within a
     * transaction. A Repository is passed in for entering the transaction.
     *
     * @param repo optional repository to use for performing string conversion
     * within transactions
     */
    protected AbstractClob(Repository repo) {
        mRepo = repo;
    }

    public String asString() throws FetchException {
        Transaction txn = mRepo == null ? null : mRepo.enterTransaction();
        try {
            long length = getLength();

            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException
                    ("Clob is too long to fit in a String: " + length);
            }

            int iLen = (int) length;

            if (iLen <= 0) {
                return "";
            }

            try {
                Reader r = openReader();

                try {
                    char[] buf = new char[iLen];
                    int offset = 0;
                    int amt;
                    while ((amt = r.read(buf, offset, iLen - offset)) > 0) {
                        offset += amt;
                    }

                    if (offset <= 0) {
                        return "";
                    }

                    return new String(buf, 0, offset);
                } finally {
                    r.close();
                }
            } catch (IOException e) {
                throw toFetchException(e);
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
        if (value == null) {
            throw new IllegalArgumentException("Clob value cannot be null");
        }

        if (mRepo == null) {
            setLength(0);
            try {
                Writer w = openWriter();
                w.write(value);
                w.close();
            } catch (IOException e) {
                throw toPersistException(e);
            }
        } else {
            Transaction txn = mRepo.enterTransaction();

            try {
                long originalLength = getLength();

                try {
                    Writer w = openWriter();
                    try {
                        w.write(value);
                    } finally {
                        w.close();
                    }
                } catch (IOException e) {
                    throw toPersistException(e);
                }

                if (value.length() < originalLength) {
                    // Truncate.
                    setLength(value.length());
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
        if (obj instanceof AbstractClob) {
            Object locator = getLocator();
            if (locator != null) {
                AbstractClob other = (AbstractClob) obj;
                return locator == other.getLocator();
            }
        }
        return false;
    }

    @Override
    public String toString() {
        Object locator = getLocator();
        return locator == null ? super.toString() : ("Clob@" + locator);
    }
}
