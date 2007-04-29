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
import java.sql.ResultSet;
import java.sql.Statement;

import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.sequence.AbstractSequenceValueProducer;

/**
 *
 *
 * @author Brian S O'Neill
 */
class JDBCSequenceValueProducer extends AbstractSequenceValueProducer {
    private final JDBCRepository mRepo;
    private final String mQuery;

    JDBCSequenceValueProducer(JDBCRepository repo, String sequenceQuery) {
        mRepo = repo;
        mQuery = sequenceQuery;
    }

    public long nextLongValue() throws PersistException {
        try {
            Connection con = mRepo.getConnection();
            try {
                Statement st = con.createStatement();
                try {
                    ResultSet rs = st.executeQuery(mQuery);
                    try {
                        if (rs.next()) {
                            return rs.getLong(1);
                        }
                        throw new PersistException("No results from sequence query: " + mQuery);
                    } finally {
                        rs.close();
                    }
                } finally {
                    st.close();
                }
            } finally {
                mRepo.yieldConnection(con);
            }
        } catch (Exception e) {
            throw mRepo.toPersistException(e);
        }
    }

    /**
     * @since 1.2
     */
    public boolean returnReservedValues() {
        return false;
    }
}
