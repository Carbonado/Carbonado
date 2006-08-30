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

import java.sql.Connection;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.capability.Capability;

/**
 * Capability to directly access the JDBC connection being used by the current
 * transaction, which is thread-local. If no transaction is in progress, then
 * the connection is in auto-commit mode.
 *
 * <p>All connections retrieved from this capability must be properly
 * yielded. Do not close the connection directly, as this interferes with the
 * transaction's ability to properly manage it.
 *
 * <p>It is perfectly okay for other Carbonado calls to be made while the
 * connection is in use.  Also, it is okay to request more connections,
 * although they will usually be the same instance. Failing to yield a
 * connection has an undefined behavior.
 *
 * <pre>
 * JDBCConnectionCapability cap = repo.getCapability(JDBCConnectionCapability.class);
 * Transaction txn = repo.enterTransaction();
 * try {
 *     Connection con = cap.getConnection();
 *     try {
 *         ...
 *     } finally {
 *         cap.yieldConnection(con);
 *     }
 *     ...
 *     txn.commit();
 * } finally {
 *     txn.exit();
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 */
public interface JDBCConnectionCapability extends Capability {
    /**
     * Any connection returned by this method must be closed by calling
     * yieldConnection.
     */
    Connection getConnection() throws FetchException;

    /**
     * Gives up a connection returned from getConnection. Connection must be
     * yielded in same thread that retrieved it.
     */
    void yieldConnection(Connection con) throws FetchException;

    /**
     * Returns the name of the database product connected to.
     */
    String getDatabaseProductName();
}
