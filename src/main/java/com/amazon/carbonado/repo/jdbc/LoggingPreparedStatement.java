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

    LoggingPreparedStatement(Log log, Connection con, PreparedStatement ps, String sql) {
        super(log, con, ps);
        mSQL = sql;
    }

    public ResultSet executeQuery() throws SQLException {
        mLog.debug(mSQL);
        return ps().executeQuery();
    }

    public int executeUpdate() throws SQLException {
        mLog.debug(mSQL);
        return ps().executeUpdate();
    }

    public boolean execute() throws SQLException {
        mLog.debug(mSQL);
        return ps().execute();
    }

    public void addBatch() throws SQLException {
        // TODO: add local batch
        ps().addBatch();
    }

    public void clearParameters() throws SQLException {
        // TODO: clear locally
        ps().clearParameters();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        // TODO: set locally
        ps().setNull(parameterIndex, sqlType);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        // TODO: set locally
        ps().setBoolean(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        // TODO: set locally
        ps().setByte(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        // TODO: set locally
        ps().setShort(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        // TODO: set locally
        ps().setInt(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        // TODO: set locally
        ps().setLong(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        // TODO: set locally
        ps().setFloat(parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        // TODO: set locally
        ps().setDouble(parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        // TODO: set locally
        ps().setBigDecimal(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        // TODO: set locally
        ps().setString(parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        // TODO: set locally
        ps().setBytes(parameterIndex, x);
    }

    public void setDate(int parameterIndex, java.sql.Date x)
        throws SQLException
    {
        // TODO: set locally
        ps().setDate(parameterIndex, x);
    }

    public void setTime(int parameterIndex, java.sql.Time x)
        throws SQLException
    {
        // TODO: set locally
        ps().setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, java.sql.Timestamp x)
        throws SQLException
    {
        // TODO: set locally
        ps().setTimestamp(parameterIndex, x);
    }

    public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length)
        throws SQLException
    {
        // TODO: set locally
        ps().setAsciiStream(parameterIndex, x, length);
    }

    @Deprecated
    public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length)
        throws SQLException
    {
        // TODO: set locally
        ps().setUnicodeStream(parameterIndex, x, length);
    }

    public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length)
        throws SQLException
    {
        // TODO: set locally
        ps().setBinaryStream(parameterIndex, x, length);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
        throws SQLException
    {
        // TODO: set locally
        ps().setObject(parameterIndex, x, targetSqlType, scale);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType)
        throws SQLException
    {
        // TODO: set locally
        ps().setObject(parameterIndex, x, targetSqlType);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        // TODO: set locally
        ps().setObject(parameterIndex, x);
    }

    public void setCharacterStream(int parameterIndex,
                                   java.io.Reader reader,
                                   int length)
        throws SQLException
    {
        // TODO: set locally
        ps().setCharacterStream(parameterIndex, reader, length);
    }

    public void setRef(int i, Ref x) throws SQLException {
        // TODO: set locally
        ps().setRef(i, x);
    }

    public void setBlob(int i, Blob x) throws SQLException {
        // TODO: set locally
        ps().setBlob(i, x);
    }

    public void setClob(int i, Clob x) throws SQLException {
        // TODO: set locally
        ps().setClob(i, x);
    }

    public void setArray(int i, Array x) throws SQLException {
        // TODO: set locally
        ps().setArray(i, x);
    }

    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal)
        throws SQLException
    {
        // TODO: set locally
        ps().setDate(parameterIndex, x, cal);
    }

    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal)
        throws SQLException
    {
        // TODO: set locally
        ps().setTime(parameterIndex, x, cal);
    }

    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal)
        throws SQLException
    {
        // TODO: set locally
        ps().setTimestamp(parameterIndex, x, cal);
    }

    public void setNull(int paramIndex, int sqlType, String typeName)
        throws SQLException
    {
        // TODO: set locally
        ps().setNull(paramIndex, sqlType, typeName);
    }

    public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
        // TODO: set locally
        ps().setURL(parameterIndex, x);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return ps().getMetaData();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps().getParameterMetaData();
    }

    private PreparedStatement ps() {
        return (PreparedStatement) mStatement;
    }
}
