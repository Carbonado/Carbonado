/*
 * Copyright 2008-2012 Amazon Technologies, Inc. or its affiliates.
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
 * @since 1.2
 */
class H2ExceptionTransformer extends JDBCExceptionTransformer {
    public static int DUPLICATE_KEY = 23001;
    public static int PROCESSING_CANCELED = 90051;

    @Override
    public boolean isUniqueConstraintError(SQLException e) {
        return super.isUniqueConstraintError(e) || DUPLICATE_KEY == e.getErrorCode();
    }

    @Override
    public boolean isTimeoutError(SQLException e) {
        return super.isTimeoutError(e) || PROCESSING_CANCELED == e.getErrorCode();
    }
}
