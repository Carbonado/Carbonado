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

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.ExistsFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.Visitor;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.OrderedProperty;

import com.amazon.carbonado.qe.OrderingList;

/**
 * Filter visitor that constructs a JoinNode tree.
 *
 * @author Brian S O'Neill
 */
class JoinNodeBuilder<S extends Storable> extends Visitor<S, Object, Object> {
    private final JDBCRepository mRepository;
    private final TableAliasGenerator mAliasGenerator;
    private final JoinNode mRootJoinNode;

    JoinNodeBuilder(JDBCRepository repository,
                    JDBCStorableInfo<S> info,
                    TableAliasGenerator aliasGenerator)
    {
        mRepository = repository;
        mAliasGenerator = aliasGenerator;
        mRootJoinNode = new JoinNode(info, aliasGenerator.nextAlias());
    }

    public JoinNode getRootJoinNode() {
        return mRootJoinNode;
    }

    /**
     * Processes the given property orderings and ensures that they are
     * part of the JoinNode tree.
     *
     * @throws UndeclaredThrowableException wraps a RepositoryException
     */
    public void captureOrderings(OrderingList<?> ordering) {
        try {
            if (ordering != null) {
                for (OrderedProperty<?> orderedProperty : ordering) {
                    ChainedProperty<?> chained = orderedProperty.getChainedProperty();
                    if (!chained.isDerived()) {
                        mRootJoinNode.addJoin(mRepository, chained, mAliasGenerator);
                    }
                }
            }
        } catch (RepositoryException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /**
     * @throws UndeclaredThrowableException wraps a RepositoryException
     * since RepositoryException cannot be thrown directly
     */
    @Override
    public Object visit(PropertyFilter<S> filter, Object param) {
        try {
            visit(filter);
            return null;
        } catch (RepositoryException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private void visit(PropertyFilter<S> filter) throws RepositoryException {
        ChainedProperty<S> chained = filter.getChainedProperty();
        mRootJoinNode.addJoin(mRepository, chained, mAliasGenerator);
    }

    /**
     * @throws UndeclaredThrowableException wraps a RepositoryException
     * since RepositoryException cannot be thrown directly
     */
    @Override
    public Object visit(ExistsFilter<S> filter, Object param) {
        try {
            visit(filter);
            return null;
        } catch (RepositoryException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private void visit(ExistsFilter<S> filter) throws RepositoryException {
        mRootJoinNode.aliasIsRequired();
        ChainedProperty<S> chained = filter.getChainedProperty();
        mRootJoinNode.addJoin(mRepository, chained, mAliasGenerator);
    }
}
