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

import java.sql.SQLException;

import com.amazon.carbonado.ConstraintException;
import com.amazon.carbonado.PersistDeniedException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.UniqueConstraintException;
import com.amazon.carbonado.spi.ExceptionTransformer;

/**
 * Custom exception transform rules.
 *
 * @author Brian S O'Neill
 */
class JDBCExceptionTransformer extends ExceptionTransformer {
    // Getting actual SQLSTATE codes is quite difficult, unless you shell out
    // cash for the proper manuals. SQLSTATE codes are five characters long,
    // where the first two indicate error class. Although the following links
    // are for DB2 SQLSTATE codes, the codes are fairly standard across all
    // major database implementations.
    //
    // ftp://ftp.software.ibm.com/ps/products/db2/info/vr6/htm/db2m0/db2state.htm
    // http://publib.boulder.ibm.com/infocenter/db2help/topic/com.ibm.db2.udb.doc/core/r0sttmsg.htm

    /** Two digit SQLSTATE class prefix for all constraint violations */
    public static String SQLSTATE_CONSTRAINT_VIOLATION_CLASS_CODE = "23";

    /**
     * Five digit SQLSTATE code for "A violation of the constraint imposed by a
     * unique index or a unique constraint occurred"
     */
    public static String SQLSTATE_UNIQUE_CONSTRAINT_VIOLATION = "23505";

    /**
     * Examines the SQLSTATE code of the given SQL exception and determines if
     * it is a generic constaint violation.
     */
    public boolean isConstraintError(SQLException e) {
        if (e != null) {
            String sqlstate = e.getSQLState();
            if (sqlstate != null) {
                return sqlstate.startsWith(SQLSTATE_CONSTRAINT_VIOLATION_CLASS_CODE);
            }
        }
        return false;
    }

    /**
     * Examines the SQLSTATE code of the given SQL exception and determines if
     * it is a unique constaint violation.
     */
    public boolean isUniqueConstraintError(SQLException e) {
        if (isConstraintError(e)) {
            String sqlstate = e.getSQLState();
            return SQLSTATE_UNIQUE_CONSTRAINT_VIOLATION.equals(sqlstate);
        }
        return false;
    }

    /**
     * Examines the SQLSTATE code of the given SQL exception and determines if
     * it indicates insufficient privileges.
     */
    public boolean isInsufficientPrivilegesError(SQLException e) {
        return false;
    }

    JDBCExceptionTransformer() {
    }

    @Override
    protected PersistException transformIntoPersistException(Throwable e) {
        PersistException pe = super.transformIntoPersistException(e);
        if (pe != null) {
            return pe;
        }
        if (e instanceof SQLException) {
            SQLException se = (SQLException) e;
            if (isUniqueConstraintError(se)) {
                return new UniqueConstraintException(e);
            }
            if (isConstraintError(se)) {
                return new ConstraintException(e);
            }
            if (isInsufficientPrivilegesError(se)) {
                return new PersistDeniedException(e);
            }
        }
        return null;
    }
}
