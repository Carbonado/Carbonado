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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.sql.Clob;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 *
 *
 * @author Brian S O'Neill
 */
class OracleClob extends JDBCClob {
    OracleClob(JDBCRepository repo, Clob clob, JDBCClobLoader loader) {
        super(repo, clob, loader);
    }

    @Override
    public Reader openReader() throws FetchException {
        return openReader(0);
    }

    @Override
    public Reader openReader(long pos) throws FetchException {
        Method m = support().mCLOB_getCharacterStream;

        if (m == null) {
            return super.openReader(pos);
        }

        try {
            return (Reader) m.invoke(getInternalClobForFetch(), pos);
        } catch (InvocationTargetException e) {
            throw mRepo.toFetchException(e.getCause());
        } catch (Exception e) {
            throw mRepo.toFetchException(e);
        }
    }

    @Override
    public Reader openReader(long pos, int bufferSize) throws FetchException {
        return openReader(pos);
    }

    @Override
    public long getLength() throws FetchException {
        Method m = support().mCLOB_length;

        if (m == null) {
            return super.getLength();
        }

        try {
            return (Long) m.invoke(getInternalClobForFetch());
        } catch (InvocationTargetException e) {
            throw mRepo.toFetchException(e.getCause());
        } catch (Exception e) {
            throw mRepo.toFetchException(e);
        }
    }

    @Override
    public Writer openWriter() throws PersistException {
        return openWriter(0);
    }

    @Override
    public Writer openWriter(long pos) throws PersistException {
        Method m = support().mCLOB_getCharacterOutputStream;

        if (m == null) {
            return super.openWriter(pos);
        }

        try {
            return (Writer) m.invoke(getInternalClobForPersist(), pos);
        } catch (InvocationTargetException e) {
            throw mRepo.toPersistException(e.getCause());
        } catch (Exception e) {
            throw mRepo.toPersistException(e);
        }
    }

    @Override
    public Writer openWriter(long pos, int bufferSize) throws PersistException {
        return openWriter(pos);
    }

    @Override
    public void setLength(long length) throws PersistException {
        // FIXME: Add special code to support increasing length
 
        Method m = support().mCLOB_trim;

        if (m == null) {
            super.setLength(length);
            return;
        }

        try {
            m.invoke(getInternalClobForPersist(), length);
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
