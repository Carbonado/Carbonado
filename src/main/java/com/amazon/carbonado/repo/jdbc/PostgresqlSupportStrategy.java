/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

/**
 * 
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
class PostgresqlSupportStrategy extends JDBCSupportStrategy {
    private static final String TRUNCATE_STATEMENT = "TRUNCATE TABLE %s";

    protected PostgresqlSupportStrategy(JDBCRepository repo) {
        super(repo);

        setTruncateTableStatement(TRUNCATE_STATEMENT);
    }

    @Override
    SliceOption getSliceOption() {
        return SliceOption.LIMIT_AND_OFFSET;
    }

    @Override
    String buildSelectWithSlice(String select, boolean from, boolean to) {
        if (to) {
            if (from) {
                return select.concat(" LIMIT ? OFFSET ?");
            } else {
                return select.concat(" LIMIT ?");
            }
        } else if (from) {
            return select.concat(" LIMIT ALL OFFSET ?");
        } else {
            return select;
        }
    }
}
