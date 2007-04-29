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
 * CallableStatement returned by LoggingConnection;
 *
 * @author Brian S O'Neill
 */
class LoggingCallableStatement extends LoggingPreparedStatement implements CallableStatement {
    LoggingCallableStatement(Log log, Connection con, CallableStatement ps, String sql) {
        super(log, con, ps, sql);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        cs().registerOutParameter(parameterIndex, sqlType);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
        throws SQLException
    {
        cs().registerOutParameter(parameterIndex, sqlType, scale);
    }

    public boolean wasNull() throws SQLException {
        return cs().wasNull();
    }

    public String getString(int parameterIndex) throws SQLException {
        return cs().getString(parameterIndex);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return cs().getBoolean(parameterIndex);
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return cs().getByte(parameterIndex);
    }

    public short getShort(int parameterIndex) throws SQLException {
        return cs().getShort(parameterIndex);
    }

    public int getInt(int parameterIndex) throws SQLException {
        return cs().getInt(parameterIndex);
    }

    public long getLong(int parameterIndex) throws SQLException {
        return cs().getLong(parameterIndex);
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return cs().getFloat(parameterIndex);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return cs().getDouble(parameterIndex);
    }

    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return cs().getBigDecimal(parameterIndex, scale);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return cs().getBytes(parameterIndex);
    }

    public java.sql.Date getDate(int parameterIndex) throws SQLException {
        return cs().getDate(parameterIndex);
    }

    public java.sql.Time getTime(int parameterIndex) throws SQLException {
        return cs().getTime(parameterIndex);
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex)
        throws SQLException
    {
        return cs().getTimestamp(parameterIndex);
    }

    public Object getObject(int parameterIndex) throws SQLException {
        return cs().getObject(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return cs().getBigDecimal(parameterIndex);
    }

    public Object getObject(int i, java.util.Map<String,Class<?>> map) throws SQLException {
        return cs().getObject(i, map);
    }

    public Ref getRef(int i) throws SQLException {
        return cs().getRef(i);
    }

    public Blob getBlob(int i) throws SQLException {
        return cs().getBlob(i);
    }

    public Clob getClob(int i) throws SQLException {
        return cs().getClob(i);
    }

    public Array getArray(int i) throws SQLException {
        return cs().getArray(i);
    }

    public java.sql.Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return cs().getDate(parameterIndex, cal);
    }

    public java.sql.Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return cs().getTime(parameterIndex, cal);
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return cs().getTimestamp(parameterIndex, cal);
    }

    public void registerOutParameter(int paramIndex, int sqlType, String typeName)
        throws SQLException
    {
        cs().registerOutParameter(paramIndex, sqlType, typeName);
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        cs().registerOutParameter(parameterName, sqlType);
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale)
        throws SQLException
    {
        cs().registerOutParameter(parameterName, sqlType, scale);
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName)
        throws SQLException
    {
        cs().registerOutParameter(parameterName, sqlType, typeName);
    }

    public java.net.URL getURL(int parameterIndex) throws SQLException {
        return cs().getURL(parameterIndex);
    }

    public void setURL(String parameterName, java.net.URL val) throws SQLException {
        cs().setURL(parameterName, val);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        cs().setNull(parameterName, sqlType);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
        cs().setBoolean(parameterName, x);
    }

    public void setByte(String parameterName, byte x) throws SQLException {
        cs().setByte(parameterName, x);
    }

    public void setShort(String parameterName, short x) throws SQLException {
        cs().setShort(parameterName, x);
    }

    public void setInt(String parameterName, int x) throws SQLException {
        cs().setInt(parameterName, x);
    }

    public void setLong(String parameterName, long x) throws SQLException {
        cs().setLong(parameterName, x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        cs().setFloat(parameterName, x);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        cs().setDouble(parameterName, x);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        cs().setBigDecimal(parameterName, x);
    }

    public void setString(String parameterName, String x) throws SQLException {
        cs().setString(parameterName, x);
    }

    public void setBytes(String parameterName, byte x[]) throws SQLException {
        cs().setBytes(parameterName, x);
    }

    public void setDate(String parameterName, java.sql.Date x) throws SQLException {
        cs().setDate(parameterName, x);
    }

    public void setTime(String parameterName, java.sql.Time x) throws SQLException {
        cs().setTime(parameterName, x);
    }

    public void setTimestamp(String parameterName, java.sql.Timestamp x) throws SQLException {
        cs().setTimestamp(parameterName, x);
    }

    public void setAsciiStream(String parameterName, java.io.InputStream x, int length)
        throws SQLException
    {
        cs().setAsciiStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, java.io.InputStream x,
                                int length)
        throws SQLException
    {
        cs().setBinaryStream(parameterName, x, length);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale)
        throws SQLException
    {
        cs().setObject(parameterName, x, targetSqlType, scale);
    }

    public void setObject(String parameterName, Object x, int targetSqlType)
        throws SQLException
    {
        cs().setObject(parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        cs().setObject(parameterName, x);
    }

    public void setCharacterStream(String parameterName,
                                   java.io.Reader reader,
                                   int length)
        throws SQLException
    {
        cs().setCharacterStream(parameterName, reader, length);
    }

    public void setDate(String parameterName, java.sql.Date x, Calendar cal)
        throws SQLException
    {
        cs().setDate(parameterName, x, cal);
    }

    public void setTime(String parameterName, java.sql.Time x, Calendar cal)
        throws SQLException
    {
        cs().setTime(parameterName, x, cal);
    }

    public void setTimestamp(String parameterName, java.sql.Timestamp x, Calendar cal)
        throws SQLException
    {
        cs().setTimestamp(parameterName, x, cal);
    }

    public void setNull(String parameterName, int sqlType, String typeName)
        throws SQLException
    {
        cs().setNull(parameterName, sqlType, typeName);
    }

    public String getString(String parameterName) throws SQLException {
        return cs().getString(parameterName);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return cs().getBoolean(parameterName);
    }

    public byte getByte(String parameterName) throws SQLException {
        return cs().getByte(parameterName);
    }

    public short getShort(String parameterName) throws SQLException {
        return cs().getShort(parameterName);
    }

    public int getInt(String parameterName) throws SQLException {
        return cs().getInt(parameterName);
    }

    public long getLong(String parameterName) throws SQLException {
        return cs().getLong(parameterName);
    }

    public float getFloat(String parameterName) throws SQLException {
        return cs().getFloat(parameterName);
    }

    public double getDouble(String parameterName) throws SQLException {
        return cs().getDouble(parameterName);
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return cs().getBytes(parameterName);
    }

    public java.sql.Date getDate(String parameterName) throws SQLException {
        return cs().getDate(parameterName);
    }

    public java.sql.Time getTime(String parameterName) throws SQLException {
        return cs().getTime(parameterName);
    }

    public java.sql.Timestamp getTimestamp(String parameterName) throws SQLException {
        return cs().getTimestamp(parameterName);
    }

    public Object getObject(String parameterName) throws SQLException {
        return cs().getObject(parameterName);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return cs().getBigDecimal(parameterName);
    }

    public Object getObject(String parameterName, java.util.Map<String,Class<?>> map)
        throws SQLException
    {
        return cs().getObject(parameterName, map);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return cs().getRef(parameterName);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return cs().getBlob(parameterName);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return cs().getClob(parameterName);
    }

    public Array getArray(String parameterName) throws SQLException {
        return cs().getArray(parameterName);
    }

    public java.sql.Date getDate(String parameterName, Calendar cal) throws SQLException {
        return cs().getDate(parameterName, cal);
    }

    public java.sql.Time getTime(String parameterName, Calendar cal) throws SQLException {
        return cs().getTime(parameterName, cal);
    }

    public java.sql.Timestamp getTimestamp(String parameterName, Calendar cal)
        throws SQLException
    {
        return cs().getTimestamp(parameterName, cal);
    }

    public java.net.URL getURL(String parameterName) throws SQLException {
        return cs().getURL(parameterName);
    }

    /**
     * @since 1.2
     */
    public RowId getRowId(int parameterIndex) throws SQLException {
        return cs().getRowId(parameterIndex);
    }
    
    /**
     * @since 1.2
     */
    public RowId getRowId(String parameterName) throws SQLException {
        return cs().getRowId(parameterName);
    }
    
    /**
     * @since 1.2
     */
    public void setRowId(String parameterName, RowId x) throws SQLException {
        cs().setRowId(parameterName, x);
    }

    /**
     * @since 1.2
     */
    public void setNString(String parameterName, String value) throws SQLException {
        cs().setNString(parameterName, value);
    }

    /**
     * @since 1.2
     */
    public void setNCharacterStream(String parameterName, java.io.Reader value, long length)
        throws SQLException
    {
        cs().setNCharacterStream(parameterName, value, length);
    }

    /**
     * @since 1.2
     */
    public void setNClob(String parameterName, NClob value) throws SQLException {
        cs().setNClob(parameterName, value);
    }

    /**
     * @since 1.2
     */
    public void setClob(String parameterName, java.io.Reader reader, long length)
        throws SQLException
    {
        cs().setClob(parameterName, reader, length);
    }

    /**
     * @since 1.2
     */
    public void setBlob(String parameterName, java.io.InputStream inputStream, long length)
        throws SQLException
    {
        cs().setBlob(parameterName, inputStream, length);
    }

    /**
     * @since 1.2
     */
    public void setNClob(String parameterName, java.io.Reader reader, long length)
        throws SQLException
    {
        cs().setNClob(parameterName, reader, length);
    }
     
    /**
     * @since 1.2
     */
    public NClob getNClob (int parameterIndex) throws SQLException {
        return cs().getNClob(parameterIndex);
    }
    
    /**
     * @since 1.2
     */
    public NClob getNClob (String parameterName) throws SQLException {
        return cs().getNClob(parameterName);
    }

    /**
     * @since 1.2
     */
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        cs().setSQLXML(parameterName, xmlObject);
    }

    /**
     * @since 1.2
     */
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return cs().getSQLXML(parameterIndex);
    }

    /**
     * @since 1.2
     */
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return cs().getSQLXML(parameterName);
    }
    
    /**
     * @since 1.2
     */
    public String getNString(int parameterIndex) throws SQLException {
        return cs().getNString(parameterIndex);
    }

    /**
     * @since 1.2
     */
    public String getNString(String parameterName) throws SQLException {
        return cs().getNString(parameterName);
    }
      
    /**
     * @since 1.2
     */
    public java.io.Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return cs().getNCharacterStream(parameterIndex);
    }

    /**
     * @since 1.2
     */
    public java.io.Reader getNCharacterStream(String parameterName) throws SQLException {
        return cs().getNCharacterStream(parameterName);
    }
        
    /**
     * @since 1.2
     */
    public java.io.Reader getCharacterStream(int parameterIndex) throws SQLException {
        return cs().getCharacterStream(parameterIndex);
    }

    /**
     * @since 1.2
     */
    public java.io.Reader getCharacterStream(String parameterName) throws SQLException {
        return cs().getCharacterStream(parameterName);
    }
    
    /**
     * @since 1.2
     */
    public void setBlob(String parameterName, Blob x) throws SQLException {
        cs().setBlob(parameterName, x);
    }

    /**
     * @since 1.2
     */
    public void setClob(String parameterName, Clob x) throws SQLException {
        cs().setClob(parameterName, x);
    }

    /**
     * @since 1.2
     */
    public void setAsciiStream(String parameterName, java.io.InputStream x, long length)
        throws SQLException
    {
        cs().setAsciiStream(parameterName, x, length);
    }

    /**
     * @since 1.2
     */
    public void setBinaryStream(String parameterName, java.io.InputStream x, long length)
        throws SQLException
    {
        cs().setBinaryStream(parameterName, x, length);
    }

    /**
     * @since 1.2
     */
    public void setCharacterStream(String parameterName,
                                   java.io.Reader reader,
                                   long length)
        throws SQLException
    {
        cs().setCharacterStream(parameterName, reader, length);
    }

    /**
     * @since 1.2
     */
    public void setAsciiStream(String parameterName, java.io.InputStream x)
        throws SQLException
    {
        cs().setAsciiStream(parameterName, x);
    }

    /**
     * @since 1.2
     */
    public void setBinaryStream(String parameterName, java.io.InputStream x)
        throws SQLException
    {
        cs().setBinaryStream(parameterName, x);
    }

    /**
     * @since 1.2
     */
    public void setCharacterStream(String parameterName, java.io.Reader reader)
        throws SQLException
    {
        cs().setCharacterStream(parameterName, reader);
    }

    /**
     * @since 1.2
     */
    public void setNCharacterStream(String parameterName, java.io.Reader value)
        throws SQLException
    {
        cs().setNCharacterStream(parameterName, value);
    }

    /**
     * @since 1.2
     */
    public void setClob(String parameterName, java.io.Reader reader) throws SQLException {
        cs().setClob(parameterName, reader);
    }

    /**
     * @since 1.2
     */
    public void setBlob(String parameterName, java.io.InputStream inputStream)
        throws SQLException
    {
        cs().setBlob(parameterName, inputStream);
    }

    /**
     * @since 1.2
     */
    public void setNClob(String parameterName, java.io.Reader reader)
        throws SQLException
    {
        cs().setNClob(parameterName, reader);
    }

    private CallableStatement cs() {
        return (CallableStatement) mStatement;
    }
}
