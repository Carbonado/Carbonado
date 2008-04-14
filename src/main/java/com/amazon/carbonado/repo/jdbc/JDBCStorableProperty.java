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

import java.lang.reflect.Method;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;

/**
 * Contains all the metadata describing a property of a specific {@link
 * Storable} type as needed by JDBCRepository.
 *
 * @author Brian S O'Neill
 * @see JDBCStorableIntrospector
 */
public interface JDBCStorableProperty<S extends Storable> extends StorableProperty<S> {
    /**
     * Returns false only if property is independent and no matching column was
     * found.
     */
    boolean isSupported();

    /**
     * Returns true if property is both supported and not a join. Simply put,
     * it can appear in a select statement.
     */
    boolean isSelectable();

    /**
     * Returns true if property is declared as @Automatic and column is
     * designated as auto-increment.
     *
     * @since 1.2
     */
    boolean isAutoIncrement();

    /**
     * Returns the table column for this property.
     *
     * @return null if property is unsupported
     */
    String getColumnName();

    /**
     * Returns the data type as defined by {@link java.sql.Types}.
     *
     * @return null if property is unsupported
     */
    Integer getDataType();

    /**
     * Returns the data type name.
     *
     * @return null if property is unsupported
     */
    String getDataTypeName();

    /**
     * @return true if column is nullable
     * @since 1.2
     */
    boolean isColumnNullable();

    /**
     * Returns the method to use to access this property (by index) from a
     * ResultSet.
     *
     * @return null if property is unsupported
     */
    Method getResultSetGetMethod();

    /**
     * Returns the method to use to set this property (by index) into a
     * PreparedStatement.
     *
     * @return null if property is unsupported
     */
    Method getPreparedStatementSetMethod();

    /**
     * Returns the adapter that needs to be applied to properties returned from
     * ResultSets and set into PreparedStatements. Is null if not needed.
     *
     * @return null if property is unsupported or if adapter not needed.
     */
    StorablePropertyAdapter getAppliedAdapter();

    /**
     * The column size is either the maximum number of characters or the
     * numeric precision.
     *
     * @return null if property is unsupported
     */
    Integer getColumnSize();

    /**
     * Returns the amount of fractional decimal digits.
     *
     * @return null if property is unsupported
     */
    Integer getDecimalDigits();

    /**
     * Returns the maximum amount of bytes for property value.
     *
     * @return null if property is unsupported
     */
    Integer getCharOctetLength();

    /**
     * Returns the one-based index of the column in the table.
     *
     * @return null if property is unsupported
     */
    Integer getOrdinalPosition();

    JDBCStorableProperty<S> getInternalJoinElement(int index);

    JDBCStorableProperty<S>[] getInternalJoinElements();

    JDBCStorableProperty<?> getExternalJoinElement(int index);

    JDBCStorableProperty<?>[] getExternalJoinElements();
}
