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

/**
 * Repository implementation that connects to an external SQL database via
 * JDBC. JDBC repository is not independent of the underlying database schema,
 * and so it requires matching tables and columns in the database. It will not
 * alter or create tables. Use the {@link com.amazon.carbonado.Alias Alias}
 * annotation to control precisely which tables and columns must be matched up.
 *
 * @see com.amazon.carbonado.repo.jdbc.JDBCRepositoryBuilder
 */
package com.amazon.carbonado.repo.jdbc;
