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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.sql.*;

import org.apache.commons.logging.Log;

/**
 * PreparedStatement returned by LoggingConnection;
 *
 * @author Brian S O'Neill
 */
class LoggingPreparedStatement extends LoggingStatement implements PreparedStatement {
    protected final String mSQL;

    private Object[] mParams;
    private BigInteger mSetParams = BigInteger.ZERO;

    LoggingPreparedStatement(Log log, Connection con, PreparedStatement ps, String sql) {
        super(log, con, ps);
        mSQL = sql;
    }

    public ResultSet executeQuery() throws SQLException {
        logStatement();
        return ps().executeQuery();
    }

    public int executeUpdate() throws SQLException {
        logStatement();
        return ps().executeUpdate();
    }

    public boolean execute() throws SQLException {
        logStatement();
        return ps().execute();
    }

    private void logStatement() {
        String statement = mSQL;

        Object[] params;
        BigInteger setParams;
        if ((params = mParams) != null && (setParams = mSetParams) != BigInteger.ZERO) {
            int length = setParams.bitLength();
            StringBuilder b = new StringBuilder(statement.length() + length * 10);
            b.append(statement);
            b.append(" -- ");
            boolean any = false;
            for (int i=0; i<length; i++) {
                if (setParams.testBit(i)) {
                    if (any) {
                        b.append(", [");
                    } else {
                        b.append('[');
                        any = true;
                    }
                    b.append(i);
                    b.append("]=");
                    b.append(params[i]);
                }
            }
            statement = b.toString();
        }

        mLog.debug(statement);
    }

    public void addBatch() throws SQLException {
        // TODO: add local batch
        ps().addBatch();
    }

    public void clearParameters() throws SQLException {
        mParams = null;
        mSetParams = BigInteger.ZERO;
        ps().clearParameters();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam(parameterIndex, null);
        ps().setNull(parameterIndex, sqlType);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setByte(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setShort(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setLong(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setFloat(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setDouble(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setBigDecimal(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setString(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        setParam(parameterIndex, x);
        ps().setBytes(parameterIndex, x);
    }

    public void setDate(int parameterIndex, java.sql.Date x)
        throws SQLException
    {
        setParam(parameterIndex, x);
        ps().setDate(parameterIndex, x);
    }

    public void setTime(int parameterIndex, java.sql.Time x)
        throws SQLException
    {
        setParam(parameterIndex, x);
        ps().setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, java.sql.Timestamp x)
        throws SQLException
    {
        setParam(parameterIndex, x);
        ps().setTimestamp(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length)
        throws SQLException
    {
        setParam(parameterIndex, "{asciiStream=" + x + ", length=" + length + '}');
        ps().setAsciiStream(parameterIndex, x, length);
    }

    @Deprecated
    public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length)
        throws SQLException
    {
        setParam(parameterIndex, "{unicodeStream=" + x + ", length=" + length + '}');
        ps().setUnicodeStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length)
        throws SQLException
    {
        setParam(parameterIndex, "{binaryStream=" + x + ", length=" + length + '}');
        ps().setBinaryStream(parameterIndex, x, length);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
        throws SQLException
    {
        setParam(parameterIndex, "{object=" + x + ", targetSqlType=" + targetSqlType +
                 ", scale=" + scale + '}');
        ps().setObject(parameterIndex, x, targetSqlType, scale);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType)
        throws SQLException
    {
        setParam(parameterIndex, "{object=" + x + ", targetSqlType=" + targetSqlType + '}');
        ps().setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setObject(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex,
                                   java.io.Reader reader,
                                   int length)
        throws SQLException
    {
        setParam(parameterIndex, "{characterStream=" + reader + ", length=" + length + '}');
        ps().setCharacterStream(parameterIndex, reader, length);
    }

    public void setRef(int i, Ref x) throws SQLException {
        setParam(i, x);
        ps().setRef(i, x);
    }

    public void setBlob(int i, Blob x) throws SQLException {
        setParam(i, x);
        ps().setBlob(i, x);
    }

    public void setClob(int i, Clob x) throws SQLException {
        setParam(i, x);
        ps().setClob(i, x);
    }

    public void setArray(int i, Array x) throws SQLException {
        setParam(i, x);
        ps().setArray(i, x);
    }

    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal)
        throws SQLException
    {
        setParam(parameterIndex, "{date=" + x + ", calendar=" + cal + '}');
        ps().setDate(parameterIndex, x, cal);
    }

    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal)
        throws SQLException
    {
        setParam(parameterIndex, "{time=" + x + ", calendar=" + cal + '}');
        ps().setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal)
        throws SQLException
    {
        setParam(parameterIndex, "{timestamp=" + x + ", calendar=" + cal + '}');
        ps().setTimestamp(parameterIndex, x, cal);
    }

    public void setNull(int paramIndex, int sqlType, String typeName)
        throws SQLException
    {
        setParam(paramIndex, "{null, sqlType=" + sqlType + ", typeName=" + typeName + '}');
        ps().setNull(paramIndex, sqlType, typeName);
    }

    public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
        setParam(parameterIndex, x);
        ps().setURL(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return ps().getMetaData();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps().getParameterMetaData();
    }

    /**
     * @since 1.2
     */
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ps().setRowId(parameterIndex, x);
    }
 
    /**
     * @since 1.2
     */
    public void setNString(int parameterIndex, String value) throws SQLException {
        ps().setNString(parameterIndex, value);
    }

    /**
     * @since 1.2
     */
    public void setNCharacterStream(int parameterIndex, java.io.Reader value, long length)
        throws SQLException
    {
        ps().setNCharacterStream(parameterIndex, value, length);
    }

    /**
     * @since 1.2
     */
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        ps().setNClob(parameterIndex, value);
    }

    /**
     * @since 1.2
     */
    public void setClob(int parameterIndex, java.io.Reader reader, long length)
        throws SQLException
    {
        ps().setClob(parameterIndex, reader, length);
    }

    /**
     * @since 1.2
     */
    public void setBlob(int parameterIndex, java.io.InputStream inputStream, long length)
        throws SQLException
    {
        ps().setBlob(parameterIndex, inputStream, length);
    }

    /**
     * @since 1.2
     */
    public void setNClob(int parameterIndex, java.io.Reader reader, long length)
        throws SQLException
    {
        ps().setNClob(parameterIndex, reader, length);
    }

    /**
     * @since 1.2
     */
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        ps().setSQLXML(parameterIndex, xmlObject);
    }

    /**
     * @since 1.2
     */
    public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length)
        throws SQLException
    {
        ps().setAsciiStream(parameterIndex, x, length);
    }

    /**
     * @since 1.2
     */
    public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length)
        throws SQLException
    {
        ps().setBinaryStream(parameterIndex, x, length);
    }

    /**
     * @since 1.2
     */
    public void setCharacterStream(int parameterIndex,
                                   java.io.Reader reader,
                                   long length)
        throws SQLException
    {
        ps().setCharacterStream(parameterIndex, reader, length);
    }

    /**
     * @since 1.2
     */
    public void setAsciiStream(int parameterIndex, java.io.InputStream x) throws SQLException {
        ps().setAsciiStream(parameterIndex, x);
    }

    /**
     * @since 1.2
     */
    public void setBinaryStream(int parameterIndex, java.io.InputStream x) throws SQLException {
        ps().setBinaryStream(parameterIndex, x);
    }

    /**
     * @since 1.2
     */
    public void setCharacterStream(int parameterIndex, java.io.Reader reader) throws SQLException {
        ps().setCharacterStream(parameterIndex, reader);
    }

    /**
     * @since 1.2
     */
    public void setNCharacterStream(int parameterIndex, java.io.Reader value) throws SQLException {
        ps().setNCharacterStream(parameterIndex, value);
    }

    /**
     * @since 1.2
     */
    public void setClob(int parameterIndex, java.io.Reader reader) throws SQLException {
        ps().setClob(parameterIndex, reader);
    }

    /**
     * @since 1.2
     */
    public void setBlob(int parameterIndex, java.io.InputStream inputStream)
        throws SQLException
    {
        ps().setBlob(parameterIndex, inputStream);
    }

    /**
     * @since 1.2
     */
    public void setNClob(int parameterIndex, java.io.Reader reader) throws SQLException {
        ps().setNClob(parameterIndex, reader);
    }

    private void setParam(int parameterIndex, Object value) {
        Object[] params = mParams;
        if (params == null) {
            mParams = params = new Object[Math.max(parameterIndex + 1, 10)];
        } else if (parameterIndex + 1 >= params.length) {
            Object[] newParams = new Object[Math.max(parameterIndex + 1, params.length * 2)];
            System.arraycopy(params, 0, newParams, 0, params.length);
            mParams = params = newParams;
        }
        params[parameterIndex] = value;
        mSetParams = mSetParams.setBit(parameterIndex);
    }

    private PreparedStatement ps() {
        return (PreparedStatement) mStatement;
    }
}
