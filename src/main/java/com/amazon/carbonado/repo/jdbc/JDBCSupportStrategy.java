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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.UnsupportedTypeException;

import com.amazon.carbonado.sequence.SequenceValueGenerator;
import com.amazon.carbonado.sequence.SequenceValueProducer;
import com.amazon.carbonado.sequence.StoredSequence;

import com.amazon.carbonado.util.ThrowUnchecked;

/**
 * Allows database product specific features to be abstracted.
 *
 * @author Brian S O'Neill
 * @author bcastill
 * @author Matt Tucker
 */
class JDBCSupportStrategy {
    private static final int BLOB_BUFFER_SIZE = 4000;
    private static final int CLOB_BUFFER_SIZE = 2000;

    /**
     * Create a strategy based on the name of the database product.
     * If one can't be found by product name the default will be used.
     */
    @SuppressWarnings("unchecked")
    static JDBCSupportStrategy createStrategy(JDBCRepository repo) {
        String databaseProductName = repo.getDatabaseProductName().trim();
        if (databaseProductName != null && databaseProductName.length() > 0) {
            String strategyName = Character.toUpperCase(databaseProductName.charAt(0))
                + databaseProductName.substring(1).toLowerCase();
            if (strategyName.indexOf(' ') > 0) {
                strategyName = strategyName.substring(0, strategyName.indexOf(' '));
            }
            strategyName = strategyName.replaceAll("[^A-Za-z0-9]", "");
            String className =
                "com.amazon.carbonado.repo.jdbc." + strategyName + "SupportStrategy";
            try {
                Class<JDBCSupportStrategy> clazz =
                    (Class<JDBCSupportStrategy>) Class.forName(className);
                return clazz.getDeclaredConstructor(JDBCRepository.class).newInstance(repo);
            } catch (ClassNotFoundException e) {
                // just use default strategy
            } catch (Exception e) {
                ThrowUnchecked.fireFirstDeclaredCause(e);
            }
        }

        return new JDBCSupportStrategy(repo);
    }
    
    protected final JDBCRepository mRepo;
    private String mSequenceSelectStatement;
    private boolean mForceStoredSequence = false;
    private String mTruncateTableStatement;
    
    protected JDBCSupportStrategy(JDBCRepository repo) {
        mRepo = repo;
    }

    JDBCExceptionTransformer createExceptionTransformer() {
        return new JDBCExceptionTransformer();
    }
    
    /**
     * @since 1.2
     */
    SequenceValueProducer createSequenceValueProducer(String name) 
        throws RepositoryException
    {
        if (name == null) {
            throw new IllegalArgumentException("Sequence name is null");
        }
        String format = getSequenceSelectStatement();
        if (format != null && format.length() > 0 && !isForceStoredSequence()) {
            String sequenceQuery = String.format(format, name);
            return new JDBCSequenceValueProducer(mRepo, sequenceQuery);
        } else {
            try {
                return new SequenceValueGenerator(mRepo, name);
            } catch (UnsupportedTypeException e) {
                if (e.getType() != StoredSequence.class) {
                    throw e;
                }
                throw new PersistException
                    ("Native sequences are not currently supported for \"" +
                     mRepo.getDatabaseProductName() + "\". Instead, define a table named " +
                     "CARBONADO_SEQUENCE as required by " + StoredSequence.class.getName() + '.');
            }
        }
    }

    /**
     * @param loader used to reload Blob outside original transaction
     */
    JDBCBlob convertBlob(java.sql.Blob blob, JDBCBlobLoader loader) {
        return blob == null ? null: new JDBCBlob(mRepo, blob, loader);
    }

    /**
     * @param loader used to reload Clob outside original transaction
     */
    JDBCClob convertClob(java.sql.Clob clob, JDBCClobLoader loader) {
        return clob == null ? null : new JDBCClob(mRepo, clob, loader);
    }

    /**
     * @return original blob if too large and post-insert update is required, null otherwise
     */
    com.amazon.carbonado.lob.Blob setBlobValue(PreparedStatement ps, int column,
                                               com.amazon.carbonado.lob.Blob blob)
        throws PersistException
    {
        try {
            if (blob instanceof JDBCBlob) {
                ps.setBlob(column, ((JDBCBlob) blob).getInternalBlobForPersist());
                return null;
            }

            long length = blob.getLength();

            if (((long) ((int) length)) != length) {
                throw new PersistException("BLOB length is too long: " + length);
            }

            ps.setBinaryStream(column, blob.openInputStream(), (int) length);
            return null;
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    /**
     * @return original clob if too large and post-insert update is required, null otherwise
     */
    com.amazon.carbonado.lob.Clob setClobValue(PreparedStatement ps, int column,
                                               com.amazon.carbonado.lob.Clob clob)
        throws PersistException
    {
        try {
            if (clob instanceof JDBCClob) {
                ps.setClob(column, ((JDBCClob) clob).getInternalClobForPersist());
                return null;
            }

            long length = clob.getLength();

            if (((long) ((int) length)) != length) {
                throw new PersistException("CLOB length is too long: " + length);
            }

            ps.setCharacterStream(column, clob.openReader(), (int) length);
            return null;
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    void updateBlob(com.amazon.carbonado.lob.Blob oldBlob, com.amazon.carbonado.lob.Blob newBlob)
        throws PersistException
    {
        try {
            OutputStream out = oldBlob.openOutputStream();
            InputStream in = newBlob.openInputStream();
            byte[] buf = new byte[BLOB_BUFFER_SIZE];
            int amt;
            while ((amt = in.read(buf)) > 0) {
                out.write(buf, 0, amt);
            }
            in.close();
            out.close();
            oldBlob.setLength(newBlob.getLength());
        } catch (FetchException e) {
            throw e.toPersistException();
        } catch (IOException e) {
            throw mRepo.toPersistException(e);
        }
    }

    void updateClob(com.amazon.carbonado.lob.Clob oldClob, com.amazon.carbonado.lob.Clob newClob)
        throws PersistException
    {
        try {
            Writer out = oldClob.openWriter();
            Reader in = newClob.openReader();
            char[] buf = new char[CLOB_BUFFER_SIZE];
            int amt;
            while ((amt = in.read(buf)) > 0) {
                out.write(buf, 0, amt);
            }
            in.close();
            out.close();
            oldClob.setLength(newClob.getLength());
        } catch (FetchException e) {
            throw e.toPersistException();
        } catch (IOException e) {
            throw mRepo.toPersistException(e);
        }
    }

    boolean printPlan(Appendable app, int indentLevel, String statement)
        throws FetchException, IOException
    {
        return false;
    }

    /**
     * Returns the optional sequence select statement format. The format is
     * printf style with 1 string parameter which can be passed through {@link
     * String#format(String, Object[])} to create a sql statement.
     *
     * @since 1.2
     */
    String getSequenceSelectStatement() {
        return mSequenceSelectStatement;
    }

    /**
     * @since 1.2
     */
    void setSequenceSelectStatement(String sequenceSelectStatement) {
        mSequenceSelectStatement = sequenceSelectStatement;
    }

    /**
     * @since 1.2
     */
    boolean isForceStoredSequence() {
        return mForceStoredSequence;
    }

    /**
     * @since 1.2
     */
    void setForceStoredSequence(boolean forceStoredSequence) {
        mForceStoredSequence = forceStoredSequence;
    }

    /**
     * Return the optional truncate table statement format. The format is
     * printf style with 1 string parameter which can be passed through {@link
     * String#format(String, Object[])} to create a sql statement.
     *
     * @since 1.2
     */
    String getTruncateTableStatement() {
        return mTruncateTableStatement;
    }

    /**
     * @since 1.2
     */
    void setTruncateTableStatement(String truncateTableStatement) {
        mTruncateTableStatement = truncateTableStatement;
    }

    /**
     * @since 1.2
     */
    enum SliceOption {
        // Slice is always emulated
        NOT_SUPPORTED,
        // Slice is emulated if from is not zero
        LIMIT_ONLY,
        // Slice is emulated if to is not null
        OFFSET_ONLY,
        // Slice is fully supported with limit parameter first
        LIMIT_AND_OFFSET,
        // Slice is fully supported with offset parameter first
        OFFSET_AND_LIMIT,
        // Slice is fully supported with from parameter first
        FROM_AND_TO,
    }

    /**
     * @since 1.2
     */
    SliceOption getSliceOption() {
        return SliceOption.NOT_SUPPORTED;
    }

    /**
     * @param select base select statement
     * @param from when true, select must support slice from bound
     * @param to when true, select must support slice to bound
     * @return revised select statement
     * @throws UnsupportedOperationException
     * @since 1.2
     */
    String buildSelectWithSlice(String select, boolean from, boolean to) {
        throw new UnsupportedOperationException();
    }
}
