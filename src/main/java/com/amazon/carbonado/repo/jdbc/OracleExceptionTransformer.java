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

/**
 *
 *
 * @author Brian S O'Neill
 */
class OracleExceptionTransformer extends JDBCExceptionTransformer {
    public static int UNIQUE_CONSTRAINT_VIOLATION = 1;

    public static int INSUFFICIENT_PRIVILEGES = 1031;

    @Override
    public boolean isUniqueConstraintError(SQLException e) {
        if (isConstraintError(e)) {
            String sqlstate = e.getSQLState();
            int errorCode = e.getErrorCode();
            return UNIQUE_CONSTRAINT_VIOLATION == errorCode
                || SQLSTATE_UNIQUE_CONSTRAINT_VIOLATION.equals(sqlstate);
        }
        return false;
    }

    @Override
    public boolean isInsufficientPrivilegesError(SQLException e) {
        if (e != null) {
            int errorCode = e.getErrorCode();
            return INSUFFICIENT_PRIVILEGES == errorCode;
        }
        return false;
    }
}
