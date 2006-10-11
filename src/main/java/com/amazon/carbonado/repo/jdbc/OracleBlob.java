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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.sql.Blob;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 *
 *
 * @author Brian S O'Neill
 */
class OracleBlob extends JDBCBlob {
    OracleBlob(JDBCRepository repo, Blob blob, JDBCBlobLoader loader) {
        super(repo, blob, loader);
    }

    @Override
    public InputStream openInputStream() throws FetchException {
        return openInputStream(0);
    }

    @Override
    public InputStream openInputStream(long pos) throws FetchException {
        Method m = support().mBLOB_getBinaryStream;

        if (m == null) {
            return super.openInputStream(pos);
        }

        try {
            return (InputStream) m.invoke(getInternalBlobForFetch(), pos);
        } catch (InvocationTargetException e) {
            throw mRepo.toFetchException(e.getCause());
        } catch (Exception e) {
            throw mRepo.toFetchException(e);
        }
    }

    @Override
    public InputStream openInputStream(long pos, int bufferSize) throws FetchException {
        return openInputStream(pos);
    }

    @Override
    public long getLength() throws FetchException {
        Method m = support().mBLOB_length;

        if (m == null) {
            return super.getLength();
        }

        try {
            return (Long) m.invoke(getInternalBlobForFetch());
        } catch (InvocationTargetException e) {
            throw mRepo.toFetchException(e.getCause());
        } catch (Exception e) {
            throw mRepo.toFetchException(e);
        }
    }

    @Override
    public OutputStream openOutputStream() throws PersistException {
        return openOutputStream(0);
    }

    @Override
    public OutputStream openOutputStream(long pos) throws PersistException {
        Method m = support().mBLOB_getBinaryOutputStream;

        if (m == null) {
            return super.openOutputStream(pos);
        }

        try {
            return (OutputStream) m.invoke(getInternalBlobForPersist(), pos);
        } catch (InvocationTargetException e) {
            throw mRepo.toPersistException(e.getCause());
        } catch (Exception e) {
            throw mRepo.toPersistException(e);
        }
    }

    @Override
    public OutputStream openOutputStream(long pos, int bufferSize) throws PersistException {
        return openOutputStream(pos);
    }

    @Override
    public void setLength(long length) throws PersistException {
        // FIXME: Add special code to support increasing length
 
        Method m = support().mBLOB_trim;

        if (m == null) {
            super.setLength(length);
            return;
        }

        try {
            m.invoke(getInternalBlobForPersist(), length);
        } catch (InvocationTargetException e) {
            throw mRepo.toPersistException(e.getCause());
        } catch (Exception e) {
            throw mRepo.toPersistException(e);
        }
    }

    private OracleSupportStrategy support() {
        return (OracleSupportStrategy) mRepo.getSupportStrategy();
    }
}
