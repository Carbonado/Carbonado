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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 *
 *
 * @author Brian S O'Neill
 * @author bcastill
 */
class OracleSupportStrategy extends JDBCSupportStrategy {

    private static final String DEFAULT_SEQUENCE_SELECT_STATEMENT = "SELECT %s.NEXTVAL FROM DUAL";

    private static final String TRUNCATE_STATEMENT = "TRUNCATE TABLE %s";

    private static final int BLOB_CHUNK_LIMIT = 2000;
    private static final int CLOB_CHUNK_LIMIT = 1000;

    //private static final String PLAN_TABLE_NAME = "TEMP_CARBONADO_PLAN_TABLE";

    final Method mBLOB_empty_lob;
    final Method mBLOB_getBinaryStream;
    final Method mBLOB_length;
    final Method mBLOB_getBinaryOutputStream;
    final Method mBLOB_trim;

    final Method mCLOB_empty_lob;
    final Method mCLOB_getCharacterStream;
    final Method mCLOB_length;
    final Method mCLOB_getCharacterOutputStream;
    final Method mCLOB_trim;

    protected OracleSupportStrategy(JDBCRepository repo) {
        super(repo);

        // Set printf style format to create sequence query
        setSequenceSelectStatement(DEFAULT_SEQUENCE_SELECT_STATEMENT);

        setTruncateTableStatement(TRUNCATE_STATEMENT);

        // Access all the custom oracle.sql.BLOB methods via reflection.
        {
            Method blob_empty_lob = null;
            Method blob_getBinaryStream = null;
            Method blob_length = null;
            Method blob_getBinaryOutputStream = null;
            Method blob_trim = null;

            try {
                Class blobClass = Class.forName("oracle.sql.BLOB");

                blob_empty_lob = blobClass.getMethod("empty_lob");
                blob_getBinaryStream = blobClass.getMethod("getBinaryStream", long.class);
                blob_length = blobClass.getMethod("length");
                blob_getBinaryOutputStream =
                    blobClass.getMethod("getBinaryOutputStream", long.class);
                blob_trim = blobClass.getMethod("trim", long.class);
            } catch (ClassNotFoundException e) {
                LogFactory.getLog(getClass()).warn("Unable to find Oracle BLOB class", e);
            } catch (NoSuchMethodException e) {
                LogFactory.getLog(getClass()).warn("Unable to find Oracle BLOB method", e);
            }

            mBLOB_empty_lob = blob_empty_lob;
            mBLOB_getBinaryStream = blob_getBinaryStream;
            mBLOB_length = blob_length;
            mBLOB_getBinaryOutputStream = blob_getBinaryOutputStream;
            mBLOB_trim = blob_trim;
        }

        // Access all the custom oracle.sql.CLOB methods via reflection.
        {
            Method clob_empty_lob = null;
            Method clob_getCharacterStream = null;
            Method clob_length = null;
            Method clob_getCharacterOutputStream = null;
            Method clob_trim = null;

            try {
                Class clobClass = Class.forName("oracle.sql.CLOB");

                clob_empty_lob = clobClass.getMethod("empty_lob");
                clob_getCharacterStream = clobClass.getMethod("getCharacterStream", long.class);
                clob_length = clobClass.getMethod("length");
                clob_getCharacterOutputStream =
                    clobClass.getMethod("getCharacterOutputStream", long.class);
                clob_trim = clobClass.getMethod("trim", long.class);
            } catch (ClassNotFoundException e) {
                LogFactory.getLog(getClass()).warn("Unable to find Oracle CLOB class", e);
            } catch (NoSuchMethodException e) {
                LogFactory.getLog(getClass()).warn("Unable to find Oracle CLOB method", e);
            }

            mCLOB_empty_lob = clob_empty_lob;
            mCLOB_getCharacterStream = clob_getCharacterStream;
            mCLOB_length = clob_length;
            mCLOB_getCharacterOutputStream = clob_getCharacterOutputStream;
            mCLOB_trim = clob_trim;
        }
    }

    @Override
    JDBCExceptionTransformer createExceptionTransformer() {
        return new OracleExceptionTransformer();
    }

    @Override
    JDBCBlob convertBlob(java.sql.Blob blob, JDBCBlobLoader loader) {
        return blob == null ? null : new OracleBlob(mRepo, blob, loader);
    }

    @Override
    JDBCClob convertClob(java.sql.Clob clob, JDBCClobLoader loader) {
        return clob == null ? null : new OracleClob(mRepo, clob, loader);
    }

    /**
     * @return original blob if too large and post-insert update is required, null otherwise
     * @throws PersistException instead of FetchException since this code is
     * called during an insert operation
     */
    @Override
    com.amazon.carbonado.lob.Blob setBlobValue(PreparedStatement ps, int column,
                                               com.amazon.carbonado.lob.Blob blob)
        throws PersistException
    {
        try {
            long length = blob.getLength();
            if (length > BLOB_CHUNK_LIMIT || ((long) ((int) length)) != length) {
                if (mBLOB_empty_lob == null) {
                    return super.setBlobValue(ps, column, blob);
                }

                try {
                    ps.setBlob(column, (java.sql.Blob) mBLOB_empty_lob.invoke(null));
                    return blob;
                } catch (InvocationTargetException e) {
                    throw mRepo.toPersistException(e.getCause());
                } catch (Exception e) {
                    throw mRepo.toPersistException(e);
                }
            }

            if (blob instanceof OracleBlob) {
                ps.setBlob(column, ((OracleBlob) blob).getInternalBlobForPersist());
                return null;
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
    @Override
    com.amazon.carbonado.lob.Clob setClobValue(PreparedStatement ps, int column,
                                               com.amazon.carbonado.lob.Clob clob)
        throws PersistException
    {
        try {
            long length = clob.getLength();
            if (length > CLOB_CHUNK_LIMIT || ((long) ((int) length)) != length) {
                if (mCLOB_empty_lob == null) {
                    return super.setClobValue(ps, column, clob);
                }

                try {
                    ps.setClob(column, (java.sql.Clob) mCLOB_empty_lob.invoke(null));
                    return clob;
                } catch (InvocationTargetException e) {
                    throw mRepo.toPersistException(e.getCause());
                } catch (Exception e) {
                    throw mRepo.toPersistException(e);
                }
            }

            if (clob instanceof OracleClob) {
                ps.setClob(column, ((OracleClob) clob).getInternalClobForPersist());
                return null;
            }

            ps.setCharacterStream(column, clob.openReader(), (int) length);
            return null;
        } catch (SQLException e) {
            throw mRepo.toPersistException(e);
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    @Override
    SliceOption getSliceOption() {
        return SliceOption.FROM_AND_TO;
    }

    @Override
    String buildSelectWithSlice(String select, boolean from, boolean to) {
        if (to) {
            if (from) {
                // Use quoted identifier with space to prevent clash with
                // Storable property name.
                return "SELECT * FROM (SELECT \"A ROW\".*, ROWNUM \"A ROWNUM\" FROM (" +
                    select + ") \"A ROW\") WHERE \"A ROWNUM\" > ? AND \"A ROWNUM\" <= ?";
            } else {
                return "SELECT * FROM (" + select + ") WHERE ROWNUM <= ?";
            }
        } else if (from) {
            return "SELECT * FROM (SELECT \"A ROW\".*, ROWNUM \"A ROWNUM\" FROM (" +
                select + ") \"A ROW\") WHERE \"A ROWNUM\" > ?";
        } else {
            return select;
        }
    }

    /* FIXME
    @Override
    boolean printPlan(Appendable app, int indentLevel, String statement)
        throws FetchException, IOException
    {
        Transaction txn = mRepo.enterTransaction();
        try {
            Connection con = mRepo.getConnection();
            try {
                try {
                    return printPlan(app, indentLevel, statement, con);
                } catch (SQLException e) {
                    throw mRepo.toFetchException(e);
                }
            } finally {
                mRepo.yieldConnection(con);
            }
        } finally {
            try {
                txn.exit();
            } catch (PersistException e) {
                // I don't care.
            }
        }
    }

    private boolean printPlan(Appendable app, int indentLevel, String statement, Connection con)
        throws SQLException, IOException
    {
        preparePlanTable(con);

        String explainPlanStatement =
            "EXPLAIN PLAN INTO " + PLAN_TABLE_NAME + " FOR " +
            statement;

        Statement st = con.createStatement();
        try {
            st.execute(explainPlanStatement);
        } finally {
            st.close();
        }

        st = con.createStatement();
        try {
            String planStatement =
                "SELECT LEVEL, OPERATION, OPTIONS, OBJECT_NAME, CARDINALITY, BYTES, COST " +
                "FROM " + PLAN_TABLE_NAME + " " +
                "START WITH ID=0 " +
                "CONNECT BY PRIOR ID = PARENT_ID " +
                "AND PRIOR NVL(STATEMENT_ID, ' ') = NVL(STATEMENT_ID, ' ') " +
                "AND PRIOR TIMESTAMP <= TIMESTAMP " +
                "ORDER BY ID, POSITION";

            ResultSet rs = st.executeQuery(planStatement);
            try {
                while (rs.next()) {
                    BaseQuery.indent(app, indentLevel + (rs.getInt(1) - 1) * 2);

                    app.append(rs.getString(2));
                    String options = rs.getString(3);
                    if (options != null && options.length() > 0) {
                        app.append(" (");
                        app.append(options);
                        app.append(')');
                    }

                    String name = rs.getString(4);
                    if (name != null && name.length() > 0) {
                        app.append(' ');
                        app.append(name);
                    }

                    app.append(" {");

                    String[] extraNames = {
                        "rows", "CARDINALITY",
                        "bytes", "BYTES",
                        "cost", "COST",
                    };

                    boolean comma = false;
                    for (int i=0; i<extraNames.length; i+=2) {
                        String str = rs.getString(extraNames[i + 1]);
                        if (str != null && str.length() > 0) {
                            if (comma) {
                                app.append(", ");
                            }
                            app.append(extraNames[i]);
                            app.append('=');
                            app.append(str);
                            comma = true;
                        }
                    }

                    app.append('}');
                    app.append('\n');
                }
            } finally {
                rs.close();
            }
        } finally {
            st.close();
        }

        return true;
    }

    private void preparePlanTable(Connection con) throws SQLException {
        Statement st = con.createStatement();
        try {
            // TODO: Is there a better way to check if a table exists?
            st.execute("SELECT COUNT(*) FROM " + PLAN_TABLE_NAME);
            return;
        } catch (SQLException e) {
            // Assume table doesn't exist, so create it.
        } finally {
            st.close();
        }

        String statement =
            "CREATE GLOBAL TEMPORARY TABLE " + PLAN_TABLE_NAME + " (" +
            "STATEMENT_ID VARCHAR2(30)," +
            "TIMESTAMP DATE," +
            "REMARKS VARCHAR2(80)," +
            "OPERATION VARCHAR2(30)," +
            "OPTIONS VARCHAR2(30)," +
            "OBJECT_NODE VARCHAR2(128)," +
            "OBJECT_OWNER VARCHAR2(30)," +
            "OBJECT_NAME VARCHAR2(30)," +
            "OBJECT_INSTANCE NUMBER(38)," +
            "OBJECT_TYPE VARCHAR2(30)," +
            "OPTIMIZER VARCHAR2(255)," +
            "SEARCH_COLUMNS NUMBER," +
            "ID NUMBER(38)," +
            "PARENT_ID NUMBER(38)," +
            "POSITION NUMBER(38)," +
            "COST NUMBER(38)," +
            "CARDINALITY NUMBER(38)," +
            "BYTES NUMBER(38)," +
            "OTHER_TAG VARCHAR2(255)," +
            "PARTITION_START VARCHAR2(255)," +
            "PARTITION_STOP VARCHAR2(255)," +
            "PARTITION_ID NUMBER(38),"+
            "OTHER LONG," +
            "DISTRIBUTION VARCHAR2(30)" +
            ")";

        st = con.createStatement();
        try {
            st.execute(statement);
        } finally {
            st.close();
        }
    }
    */
}
