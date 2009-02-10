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

import java.lang.reflect.UndeclaredThrowableException;

import java.util.ArrayList;
import java.util.List;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.ClosedFilter;
import com.amazon.carbonado.filter.ExistsFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class WhereBuilder<S extends Storable> extends Visitor<S, FetchException, Object> {
    private final SQLStatementBuilder mStatementBuilder;
    private final JoinNode mJoinNode;
    private final TableAliasGenerator mAliasGenerator;

    private List<PropertyFilter<S>> mPropertyFilters;
    private List<Boolean> mPropertyFilterNullable;

    /**
     * @param aliasGenerator used for supporting "EXISTS" filter
     */
    WhereBuilder(SQLStatementBuilder statementBuilder, JoinNode jn,
                 TableAliasGenerator aliasGenerator)
    {
        mStatementBuilder = statementBuilder;
        mJoinNode = jn;
        mAliasGenerator = aliasGenerator;
        mPropertyFilters = new ArrayList<PropertyFilter<S>>();
        mPropertyFilterNullable = new ArrayList<Boolean>();
    }

    @SuppressWarnings("unchecked")
    public PropertyFilter<S>[] getPropertyFilters() {
        return mPropertyFilters.toArray(new PropertyFilter[mPropertyFilters.size()]);
    }

    public boolean[] getPropertyFilterNullable() {
        boolean[] array = new boolean[mPropertyFilterNullable.size()];
        for (int i=0; i<array.length; i++) {
            array[i] = mPropertyFilterNullable.get(i);
        }
        return array;
    }

    @Override
    public FetchException visit(OrFilter<S> filter, Object param) {
        FetchException e;
        mStatementBuilder.append('(');
        e = filter.getLeftFilter().accept(this, null);
        if (e != null) {
            return e;
        }
        mStatementBuilder.append(" OR ");
        e = filter.getRightFilter().accept(this, null);
        if (e != null) {
            return e;
        }
        mStatementBuilder.append(')');
        return null;
    }

    @Override
    public FetchException visit(AndFilter<S> filter, Object param) {
        FetchException e;
        mStatementBuilder.append('(');
        e = filter.getLeftFilter().accept(this, null);
        if (e != null) {
            return e;
        }
        mStatementBuilder.append(" AND ");
        e = filter.getRightFilter().accept(this, null);
        if (e != null) {
            return e;
        }
        mStatementBuilder.append(')');
        return null;
    }

    @Override
    public FetchException visit(PropertyFilter<S> filter, Object param) {
        try {
            mStatementBuilder.appendColumn(mJoinNode, filter.getChainedProperty());
        } catch (FetchException e) {
            return e;
        }

        if (!filter.isConstant()) {
            addBindParameter(filter);
        } else {
            RelOp op = filter.getOperator();

            Object constant = filter.constant();
            if (constant == null) {
                if (op == RelOp.EQ) {
                    mStatementBuilder.append("IS NULL");
                } else if (op == RelOp.NE) {
                    mStatementBuilder.append("IS NOT NULL");
                } else {
                    mStatementBuilder.append(sqlOperatorFor(op));
                    mStatementBuilder.append("NULL");
                }
            } else if (filter.getType() == String.class) {
                mStatementBuilder.append(sqlOperatorFor(op));
                mStatementBuilder.append('\'');
                mStatementBuilder.append(String.valueOf(constant).replace("'", "''"));
                mStatementBuilder.append('\'');
            } else if (Number.class.isAssignableFrom(filter.getBoxedType())) {
                mStatementBuilder.append(sqlOperatorFor(op));
                mStatementBuilder.append(String.valueOf(constant));
            } else {
                // Don't try to create literal for special type. Instead,
                // fallback to bind parameter and let JDBC driver do the work.
                addBindParameter(filter);
            }
        }

        return null;
    }

    @Override
    public FetchException visit(ExistsFilter<S> filter, Object param) {
        if (filter.isNotExists()) {
            mStatementBuilder.append("NOT ");
        }
        mStatementBuilder.append("EXISTS (SELECT * FROM");

        ChainedProperty<S> chained = filter.getChainedProperty();

        JDBCStorableInfo<?> oneToManyInfo;
        JDBCStorableProperty<?> oneToMany;

        final JDBCRepository repo = mStatementBuilder.getRepository();
        try {
            StorableProperty<?> lastProp = chained.getLastProperty();
            oneToManyInfo = repo.examineStorable(lastProp.getJoinedType());
            oneToMany = repo.getJDBCStorableProperty(lastProp);
        } catch (RepositoryException e) {
            return repo.toFetchException(e);
        }

        Filter<?> subFilter = filter.getSubFilter();

        JoinNode oneToManyNode;
        try {
            JoinNodeBuilder jnb =
                new JoinNodeBuilder(repo, oneToManyInfo, mAliasGenerator);
            if (subFilter != null) {
                subFilter.accept(jnb, null);
            }
            oneToManyNode = jnb.getRootJoinNode();
        } catch (UndeclaredThrowableException e) {
            return repo.toFetchException(e);
        }

        oneToManyNode.appendFullJoinTo(mStatementBuilder);

        mStatementBuilder.append(" WHERE ");

        int count = oneToMany.getJoinElementCount();
        for (int i=0; i<count; i++) {
            if (i > 0) {
                mStatementBuilder.append(" AND ");
            }
            mStatementBuilder.append(oneToManyNode.getAlias());
            mStatementBuilder.append('.');
            mStatementBuilder.append(oneToMany.getInternalJoinElement(i).getColumnName());
            mStatementBuilder.append('=');
            mStatementBuilder.append(mJoinNode.findAliasFor(chained));
            mStatementBuilder.append('.');
            mStatementBuilder.append(oneToMany.getExternalJoinElement(i).getColumnName());
        }

        if (subFilter != null && !subFilter.isOpen()) {
            mStatementBuilder.append(" AND (");

            WhereBuilder wb = new WhereBuilder
                (mStatementBuilder, oneToManyNode, mAliasGenerator);

            FetchException e = (FetchException) subFilter.accept(wb, null);
            if (e != null) {
                return e;
            }

            mStatementBuilder.append(')');

            // Transfer property filters from sub-builder as joined from exists filter.
            int size = wb.mPropertyFilters.size();
            for (int i=0; i<size; i++) {
                PropertyFilter propFilter = (PropertyFilter) wb.mPropertyFilters.get(i);
                mPropertyFilters.add(propFilter.asJoinedFromAny(chained));
            }
            mPropertyFilterNullable.addAll(wb.mPropertyFilterNullable);
        }

        mStatementBuilder.append(')');

        return null;
    }

    @Override
    public FetchException visit(ClosedFilter<S> filter, Object param) {
        mStatementBuilder.append("1=0");
        return null;
    }

    private void addBindParameter(PropertyFilter<S> filter) {
        RelOp op = filter.getOperator();
        StorableProperty<?> property = filter.getChainedProperty().getLastProperty();

        mPropertyFilters.add(filter);

        if (property.isNullable() && (op == RelOp.EQ || op == RelOp.NE)) {
            mPropertyFilterNullable.add(true);
            mStatementBuilder.append(new NullablePropertyStatement<S>(filter, op == RelOp.EQ));
        } else {
            mPropertyFilterNullable.add(false);
            mStatementBuilder.append(sqlOperatorFor(op));
            mStatementBuilder.append('?');
        }
    }

    private String sqlOperatorFor(RelOp op) {
        if (op == RelOp.NE) {
            return "<>";
        } else {
            return op.toString();
        }
    }
}
