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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

import com.amazon.carbonado.gen.MasterSupport;

/**
 *
 *
 * @author Brian S O'Neill
 */
public interface JDBCSupport<S extends Storable> extends MasterSupport<S>, JDBCConnectionCapability
{
    /**
     * @param loader used to reload Blob outside original transaction
     */
    public com.amazon.carbonado.lob.Blob convertBlob(java.sql.Blob blob, JDBCBlobLoader loader)
        throws FetchException;

    /**
     * @param loader used to reload Clob outside original transaction
     */
    public com.amazon.carbonado.lob.Clob convertClob(java.sql.Clob clob, JDBCClobLoader loader)
        throws FetchException;

    /**
     * @return original blob if too large and post-insert update is required, null otherwise
     * @throws PersistException instead of FetchException since this code is
     * called during an insert operation
     */
    public com.amazon.carbonado.lob.Blob setBlobValue(PreparedStatement ps, int column,
                                                      com.amazon.carbonado.lob.Blob blob)
        throws PersistException;

    /**
     * @return original clob if too large and post-insert update is required, null otherwise
     * @throws PersistException instead of FetchException since this code is
     * called during an insert operation
     */
    public com.amazon.carbonado.lob.Clob setClobValue(PreparedStatement ps, int column,
                                                      com.amazon.carbonado.lob.Clob clob)
        throws PersistException;

    public void updateBlob(com.amazon.carbonado.lob.Blob oldBlob,
                           com.amazon.carbonado.lob.Blob newBlob)
        throws PersistException;

    public void updateClob(com.amazon.carbonado.lob.Clob oldClob,
                           com.amazon.carbonado.lob.Clob newClob)
        throws PersistException;
}
