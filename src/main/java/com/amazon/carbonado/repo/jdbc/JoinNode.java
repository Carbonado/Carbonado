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

import java.util.LinkedHashMap;
import java.util.Map;

import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Node in a tree structure describing how tables are joined together.
 *
 * @author Brian S O'Neill
 * @see JoinNodeBuilder
 */
class JoinNode {
    // Joined property which led to this node. For root node, it is null.
    private final JDBCStorableProperty<?> mProperty;
    private final boolean mOuterJoin;

    private final JDBCStorableInfo<?> mInfo;
    private final String mAlias;

    private final Map<SubNodeKey, JoinNode> mSubNodes;

    private boolean mAliasRequired;

    /**
     * @param alias table alias in SQL statement, i.e. "T1"
     */
    JoinNode(JDBCStorableInfo<?> info, String alias) {
        this(null, false, info, alias);
    }

    private JoinNode(JDBCStorableProperty<?> property,
                     boolean outerJoin,
                     JDBCStorableInfo<?> info,
                     String alias)
    {
        mProperty = property;
        mOuterJoin = outerJoin;
        mInfo = info;
        mAlias = alias;
        mSubNodes = new LinkedHashMap<SubNodeKey, JoinNode>();
    }

    /**
     * Returns the table alias to use in SQL statement, i.e. "T1"
     */
    public String getAlias() {
        return mAlias;
    }

    public String findAliasFor(ChainedProperty<?> chained) {
        return findAliasFor(chained, 0);
    }

    private String findAliasFor(ChainedProperty<?> chained, int offset) {
        if ((chained.getChainCount() - offset) <= 0) {
            // At this point in the chain, there are no more joins.
            return mAlias;
        }
        StorableProperty<?> property;
        if (offset == 0) {
            property = chained.getPrimeProperty();
        } else {
            property = chained.getChainedProperty(offset - 1);
        }
        boolean outerJoin = chained.isOuterJoin(offset);
        SubNodeKey key = new SubNodeKey(property.getName(), outerJoin); 
        JoinNode subNode = mSubNodes.get(key);
        if (subNode != null) {
            return subNode.findAliasFor(chained, offset + 1);
        }
        return null;
    }

    public boolean isAliasRequired() {
        return mAliasRequired || mSubNodes.size() > 0;
    }

    /**
     * Appends table name to the given FROM clause builder.
     */
    public void appendTableNameTo(SQLStatementBuilder fromClause) {
        fromClause.append(' ');
        fromClause.append(mInfo.getQualifiedTableName());
    }

    /**
     * Appends table name and alias to the given FROM clause builder.
     */
    public void appendTableNameAndAliasTo(SQLStatementBuilder fromClause) {
        appendTableNameTo(fromClause);
        fromClause.append(' ');
        fromClause.append(mAlias);
    }

    /**
     * Appends table names, aliases, and joins to the given FROM clause
     * builder.
     */
    public void appendFullJoinTo(SQLStatementBuilder fromClause) {
        appendTableNameAndAliasTo(fromClause);
        appendTailJoinTo(fromClause);
    }

    private void appendTailJoinTo(SQLStatementBuilder fromClause) {
        for (JoinNode jn : mSubNodes.values()) {
            if (jn.mOuterJoin) {
                fromClause.append(" LEFT OUTER JOIN");
            } else {
                fromClause.append(" INNER JOIN");
            }
            jn.appendTableNameAndAliasTo(fromClause);
            fromClause.append(" ON ");
            int count = jn.mProperty.getJoinElementCount();
            for (int i=0; i<count; i++) {
                if (i > 0) {
                    fromClause.append(" AND ");
                }
                fromClause.append(mAlias);
                fromClause.append('.');
                fromClause.append(jn.mProperty.getInternalJoinElement(i).getColumnName());
                fromClause.append('=');
                fromClause.append(jn.mAlias);
                fromClause.append('.');
                fromClause.append(jn.mProperty.getExternalJoinElement(i).getColumnName());
            }

            jn.appendTailJoinTo(fromClause);
        }
    }

    public void addJoin(JDBCRepository repository,
                        ChainedProperty<?> chained,
                        TableAliasGenerator aliasGenerator)
        throws RepositoryException
    {
        addJoin(repository, chained, aliasGenerator, 0);
    }

    private void addJoin(JDBCRepository repository,
                         ChainedProperty<?> chained,
                         TableAliasGenerator aliasGenerator,
                         int offset)
        throws RepositoryException
    {
        if ((chained.getChainCount() - offset) <= 0) {
            // At this point in the chain, there are no more joins.
            return;
        }
        StorableProperty<?> property;
        if (offset == 0) {
            property = chained.getPrimeProperty();
        } else {
            property = chained.getChainedProperty(offset - 1);
        }
        boolean outerJoin = chained.isOuterJoin(offset);
        SubNodeKey key = new SubNodeKey(property.getName(), outerJoin); 
        JoinNode subNode = mSubNodes.get(key);
        if (subNode == null) {
            JDBCStorableInfo<?> info = repository.examineStorable(property.getJoinedType());
            JDBCStorableProperty<?> jProperty = repository.getJDBCStorableProperty(property);
            subNode = new JoinNode(jProperty, outerJoin, info, aliasGenerator.nextAlias());
            mSubNodes.put(key, subNode);
        }
        subNode.addJoin(repository, chained, aliasGenerator, offset + 1);
    }

    public void aliasIsRequired() {
        mAliasRequired = true;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("{table=");
        b.append(mInfo.getQualifiedTableName());
        b.append(", alias=");
        b.append(mAlias);
        if (mSubNodes.size() > 0) {
            b.append(", subNodes=");
            b.append(mSubNodes);
        }
        b.append('}');
        return b.toString();
    }

    private static class SubNodeKey {
        final String mPropertyName;
        final boolean mOuterJoin;

        SubNodeKey(String propertyName, boolean outerJoin) {
            mPropertyName = propertyName;
            mOuterJoin = outerJoin;
        }

        @Override
        public int hashCode() {
            return mPropertyName.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof SubNodeKey) {
                SubNodeKey other = (SubNodeKey) obj;
                return mPropertyName.equals(other.mPropertyName)
                    && mOuterJoin == other.mOuterJoin;
            }
            return false;
        }

        @Override
        public String toString() {
            return "propertyName=" + mPropertyName + ", outerJoin=" + mOuterJoin;
        }
    }
}
