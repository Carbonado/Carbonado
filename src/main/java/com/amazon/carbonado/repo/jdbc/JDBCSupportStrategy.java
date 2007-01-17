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
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.HashMap;
import java.util.Map;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

import com.amazon.carbonado.util.ThrowUnchecked;

import com.amazon.carbonado.spi.SequenceValueProducer;

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

    private Map<String, SequenceValueProducer> mSequences;

    protected JDBCSupportStrategy(JDBCRepository repo) {
        mRepo = repo;
    }

    JDBCExceptionTransformer createExceptionTransformer() {
        return new JDBCExceptionTransformer();
    }

    /**
     * Utility method used by generated storables to get sequence values during
     * an insert operation.
     *
     * @param sequenceName name of sequence
     * @throws PersistException instead of FetchException since this code is
     * called during an insert operation
     */
    synchronized SequenceValueProducer getSequenceValueProducer(String sequenceName)
        throws PersistException
    {
        SequenceValueProducer sequence = mSequences == null ? null : mSequences.get(sequenceName);

        if (sequence == null) {
            String sequenceQuery = createSequenceQuery(sequenceName);
            sequence = new JDBCSequenceValueProducer(mRepo, sequenceQuery);
            if (mSequences == null) {
                mSequences = new HashMap<String, SequenceValueProducer>();
            }
            mSequences.put(sequenceName, sequence);
        }

        return sequence;
    }

    String createSequenceQuery(String sequenceName) {
        throw new UnsupportedOperationException
            ("Sequences are not supported by default JDBC support strategy. " +
             "If \"" + mRepo.getDatabaseProductName() + "\" actually does support sequences, " +
             "then a custom support strategy might be available in a separate jar. " +
             "If so, simply add it to your classpath.");
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
}
