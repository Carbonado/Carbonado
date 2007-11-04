/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

import java.util.ArrayList;
import java.util.List;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class SQLStatementBuilder<S extends Storable> {
    private final JDBCRepository mRepository;

    private List<SQLStatement<S>> mStatements;
    private StringBuilder mLiteralBuilder;

    SQLStatementBuilder(JDBCRepository repository) {
        mRepository = repository;
        mStatements = new ArrayList<SQLStatement<S>>();
        mLiteralBuilder = new StringBuilder();
    }

    public SQLStatement<S> build() {
        if (mStatements.size() == 0 || mLiteralBuilder.length() > 0) {
            mStatements.add(new LiteralStatement<S>(mLiteralBuilder.toString()));
            mLiteralBuilder.setLength(0);
        }
        if (mStatements.size() == 1) {
            return mStatements.get(0);
        } else {
            return new CompositeStatement<S>(mStatements);
        }
    }

    public void append(char c) {
        mLiteralBuilder.append(c);
    }

    public void append(String str) {
        mLiteralBuilder.append(str);
    }

    public void append(LiteralStatement<S> statement) {
        append(statement.toString());
    }

    public void append(SQLStatement<S> statement) {
        if (statement instanceof LiteralStatement) {
            append((LiteralStatement<S>) statement);
        } else {
            mStatements.add(new LiteralStatement<S>(mLiteralBuilder.toString()));
            mLiteralBuilder.setLength(0);
            mStatements.add(statement);
        }
    }

    public void appendColumn(JoinNode jn, ChainedProperty<?> chained)
        throws FetchException
    {
        String alias;
        if (jn == null) {
            alias = null;
        } else {
            alias = jn.findAliasFor(chained);
        }
        if (alias != null) {
            mLiteralBuilder.append(alias);
            mLiteralBuilder.append('.');
        }
        StorableProperty<?> property = chained.getLastProperty();
        JDBCStorableProperty<?> jProperty;
        try {
            jProperty = mRepository.getJDBCStorableProperty(property);
        } catch (RepositoryException e) {
            throw mRepository.toFetchException(e);
        }
        if (jProperty.isJoin()) {
            throw new UnsupportedOperationException
                ("Join property doesn't have a corresponding column: " + chained);
        }
        mLiteralBuilder.append(jProperty.getColumnName());
    }

    JDBCRepository getRepository() {
        return mRepository;
    }
}
