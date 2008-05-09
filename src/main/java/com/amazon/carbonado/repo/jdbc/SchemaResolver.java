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

import java.sql.Connection;
import java.sql.SQLException;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.info.StorableInfo;

/**
 * Experimental interface for allowing tables to be created or altered when the
 * Storable definition doesn't match the table or the table doesn't
 * exist. Currently only used by unit tests.
 *
 * @author Brian S O'Neill
 */
interface SchemaResolver {
    /**
     * @return true if support has been resolved
     */
    <S extends Storable> boolean resolve(StorableInfo<S> info,
                                         Connection con, String catalog, String schema)
        throws SQLException;
}
