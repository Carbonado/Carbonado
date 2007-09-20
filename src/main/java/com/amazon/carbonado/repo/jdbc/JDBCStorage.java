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

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;
import com.amazon.carbonado.filter.Visitor;
import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;
import com.amazon.carbonado.qe.AbstractQueryExecutor;
import com.amazon.carbonado.qe.OrderingList;
import com.amazon.carbonado.qe.QueryExecutor;
import com.amazon.carbonado.qe.QueryExecutorCache;
import com.amazon.carbonado.qe.QueryExecutorFactory;
import com.amazon.carbonado.qe.QueryFactory;
import com.amazon.carbonado.qe.StandardQuery;
import com.amazon.carbonado.qe.StandardQueryFactory;
import com.amazon.carbonado.sequence.SequenceValueProducer;
import com.amazon.carbonado.spi.TriggerManager;
import com.amazon.carbonado.util.QuickConstructorGenerator;

/**
 *
 *
 * @author Brian S O'Neill
 */
class JDBCStorage<S extends Storable> extends StandardQueryFactory<S>
    implements Storage<S>, JDBCSupport<S>
{
    private static final int FIRST_RESULT_INDEX = 1;

    final JDBCRepository mRepository;
    final JDBCSupportStrategy mSupportStrategy;
    final JDBCStorableInfo<S> mInfo;
    final InstanceFactory mInstanceFactory;
    final QueryExecutorFactory<S> mExecutorFactory;

    final TriggerManager<S> mTriggerManager;

    JDBCStorage(JDBCRepository repository, JDBCStorableInfo<S> info, boolean autoVersioning)
        throws SupportException, RepositoryException
    {
        super(info.getStorableType());
        mRepository = repository;
        mSupportStrategy = repository.getSupportStrategy();
        mInfo = info;

        Class<? extends S> generatedStorableClass = JDBCStorableGenerator
            .getGeneratedClass(info, autoVersioning);

        mInstanceFactory = QuickConstructorGenerator
            .getInstance(generatedStorableClass, InstanceFactory.class);

        mExecutorFactory = new QueryExecutorCache<S>(new ExecutorFactory());

        mTriggerManager = new TriggerManager<S>
            (info.getStorableType(), repository.mTriggerFactories);
    }

    public Class<S> getStorableType() {
        return mInfo.getStorableType();
    }

    public S prepare() {
        return (S) mInstanceFactory.instantiate(this);
    }

    public JDBCRepository getJDBCRepository() {
        return mRepository;
    }

    public Repository getRootRepository() {
        return mRepository.getRootRepository();
    }

    public boolean isPropertySupported(String propertyName) {
        JDBCStorableProperty<S> property = mInfo.getAllProperties().get(propertyName);
        return property != null && property.isSupported();
    }

    /**
     * @since 1.2
     */
    public void truncate() throws PersistException {
        String truncateFormat = mSupportStrategy.getTruncateTableStatement();

        try {
            if (truncateFormat == null || mTriggerManager.getDeleteTrigger() != null) {
                query().deleteAll();
                return;
            }

            Connection con = mRepository.getConnection();
            try {
                java.sql.Statement st = con.createStatement();
                try {
                    st.execute(String.format(truncateFormat, mInfo.getQualifiedTableName()));
                } finally {
                    st.close();
                }
            } catch (SQLException e) {
                throw JDBCExceptionTransformer.getInstance().toPersistException(e);
            } finally {
                mRepository.yieldConnection(con);
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.removeTrigger(trigger);
    }

    public IndexInfo[] getIndexInfo() {
        return mInfo.getIndexInfo();
    }

    public SequenceValueProducer getSequenceValueProducer(String name) throws PersistException {
        try {
            return mRepository.getSequenceValueProducer(name);
        } catch (RepositoryException e) {
            throw e.toPersistException();
        }
    }

    public Trigger<? super S> getInsertTrigger() {
        return mTriggerManager.getInsertTrigger();
    }

    public Trigger<? super S> getUpdateTrigger() {
        return mTriggerManager.getUpdateTrigger();
    }

    public Trigger<? super S> getDeleteTrigger() {
        return mTriggerManager.getDeleteTrigger();
    }

    public Trigger<? super S> getLoadTrigger() {
        return mTriggerManager.getLoadTrigger();
    }

    public void locallyDisableLoadTrigger() {
        mTriggerManager.locallyDisableLoad();
    }

    public void locallyEnableLoadTrigger() {
        mTriggerManager.locallyEnableLoad();
    }

    /**
     * @param loader used to reload Blob outside original transaction
     */
    public com.amazon.carbonado.lob.Blob convertBlob(java.sql.Blob blob, JDBCBlobLoader loader)
        throws FetchException
    {
        JDBCBlob jblob = mSupportStrategy.convertBlob(blob, loader);

        if (jblob != null) {
            try {
                JDBCTransaction txn = mRepository.localTxnManager().getTxn();
                if (txn != null) {
                    txn.register(jblob);
                }
            } catch (Exception e) {
                throw mRepository.toFetchException(e);
            }
        }

        return jblob;
    }

    /**
     * @param loader used to reload Clob outside original transaction
     */
    public com.amazon.carbonado.lob.Clob convertClob(java.sql.Clob clob, JDBCClobLoader loader)
        throws FetchException
    {
        JDBCClob jclob = mSupportStrategy.convertClob(clob, loader);

        if (jclob != null) {
            try {
                JDBCTransaction txn = mRepository.localTxnManager().getTxn();
                if (txn != null) {
                    txn.register(jclob);
                }
            } catch (Exception e) {
                throw mRepository.toFetchException(e);
            }
        }

        return jclob;
    }

    /**
     * @return original blob if too large and post-insert update is required, null otherwise
     * @throws PersistException instead of FetchException since this code is
     * called during an insert operation
     */
    public com.amazon.carbonado.lob.Blob setBlobValue(PreparedStatement ps, int column,
                                                      com.amazon.carbonado.lob.Blob blob)
        throws PersistException
    {
        return mSupportStrategy.setBlobValue(ps, column, blob);
    }

    /**
     * @return original clob if too large and post-insert update is required, null otherwise
     * @throws PersistException instead of FetchException since this code is
     * called during an insert operation
     */
    public com.amazon.carbonado.lob.Clob setClobValue(PreparedStatement ps, int column,
                                                      com.amazon.carbonado.lob.Clob clob)
        throws PersistException
    {
        return mSupportStrategy.setClobValue(ps, column, clob);
    }

    public void updateBlob(com.amazon.carbonado.lob.Blob oldBlob,
                           com.amazon.carbonado.lob.Blob newBlob)
        throws PersistException
    {
        mSupportStrategy.updateBlob(oldBlob, newBlob);
    }

    public void updateClob(com.amazon.carbonado.lob.Clob oldClob,
                           com.amazon.carbonado.lob.Clob newClob)
        throws PersistException
    {
        mSupportStrategy.updateClob(oldClob, newClob);
    }

    protected JDBCStorableInfo<S> getStorableInfo() {
        return mInfo;
    }

    protected StandardQuery<S> createQuery(FilterValues<S> values, OrderingList<S> ordering) {
        return new JDBCQuery(values, ordering, null);
    }

    public S instantiate(ResultSet rs) throws SQLException {
        return (S) mInstanceFactory.instantiate(this, rs, FIRST_RESULT_INDEX);
    }

    public static interface InstanceFactory {
        Storable instantiate(JDBCSupport storage);

        Storable instantiate(JDBCSupport storage, ResultSet rs, int offset) throws SQLException;
    }

    private class ExecutorFactory implements QueryExecutorFactory<S> {
        public Class<S> getStorableType() {
            return JDBCStorage.this.getStorableType();
        }

        public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering)
            throws RepositoryException
        {
            TableAliasGenerator aliasGenerator = new TableAliasGenerator();

            JoinNode jn;
            try {
                JoinNodeBuilder jnb = new JoinNodeBuilder(aliasGenerator);
                if (filter == null) {
                    jn = new JoinNode(getStorableInfo(), null);
                } else {
                    filter.accept(jnb, null);
                    jn = jnb.getRootJoinNode();
                }
                jnb.captureOrderings(ordering);
            } catch (UndeclaredThrowableException e) {
                throw mRepository.toFetchException(e);
            }

            StatementBuilder selectBuilder = new StatementBuilder();
            selectBuilder.append("SELECT ");

            // Don't bother using a table alias for one table. With just one table,
            // there's no need to disambiguate.
            String alias = jn.hasAnyJoins() ? jn.getAlias() : null;

            Map<String, JDBCStorableProperty<S>> properties = getStorableInfo().getAllProperties();
            int ordinal = 0;
            for (JDBCStorableProperty<S> property : properties.values()) {
                if (!property.isSelectable()) {
                    continue;
                }
                if (ordinal > 0) {
                    selectBuilder.append(',');
                }
                if (alias != null) {
                    selectBuilder.append(alias);
                    selectBuilder.append('.');
                }
                selectBuilder.append(property.getColumnName());
                ordinal++;
            }

            selectBuilder.append(" FROM");

            StatementBuilder fromWhereBuilder = new StatementBuilder();
            fromWhereBuilder.append(" FROM");

            if (alias == null) {
                // Don't bother defining a table alias for one table.
                jn.appendTableNameTo(selectBuilder);
                jn.appendTableNameTo(fromWhereBuilder);
            } else {
                jn.appendFullJoinTo(selectBuilder);
                jn.appendFullJoinTo(fromWhereBuilder);
            }

            PropertyFilter<S>[] propertyFilters;
            boolean[] propertyFilterNullable;

            if (filter == null) {
                propertyFilters = null;
                propertyFilterNullable = null;
            } else {
                // Build the WHERE clause only if anything to filter on.
                selectBuilder.append(" WHERE ");
                fromWhereBuilder.append(" WHERE ");

                WhereBuilder wb = new WhereBuilder(selectBuilder, alias == null ? null : jn);
                FetchException e = filter.accept(wb, null);
                if (e != null) {
                    throw e;
                }

                propertyFilters = wb.getPropertyFilters();
                propertyFilterNullable = wb.getPropertyFilterNullable();

                wb = new WhereBuilder(fromWhereBuilder, alias == null ? null : jn);
                e = filter.accept(wb, null);
                if (e != null) {
                    throw e;
                }
            }

            // Append order-by clause.
            if (ordering != null && ordering.size() != 0) {
                selectBuilder.append(" ORDER BY ");
                ordinal = 0;
                for (OrderedProperty<S> orderedProperty : ordering) {
                    if (ordinal > 0) {
                        selectBuilder.append(',');
                    }
                    selectBuilder.appendColumn(alias == null ? null : jn,
                                               orderedProperty.getChainedProperty());
                    if (orderedProperty.getDirection() == Direction.DESCENDING) {
                        selectBuilder.append(" DESC");
                    }
                    ordinal++;
                }
            }

            return new Executor(filter,
                                ordering,
                                selectBuilder.build(),
                                fromWhereBuilder.build(),
                                propertyFilters,
                                propertyFilterNullable);
        }
    }

    private class Executor extends AbstractQueryExecutor<S> {
        private final Filter<S> mFilter;
        private final OrderingList<S> mOrdering;

        private final SQLStatement<S> mSelectStatement;
        private final int mMaxSelectStatementLength;
        private final SQLStatement<S> mFromWhereStatement;
        private final int mMaxFromWhereStatementLength;

        // The following arrays all have the same length, or they may all be null.

        private final PropertyFilter<S>[] mPropertyFilters;
        private final boolean[] mPropertyFilterNullable;

        private final Method[] mPreparedStatementSetMethods;

        // Some entries may be null if no adapter required.
        private final Method[] mAdapterMethods;

        // Some entries may be null if no adapter required.
        private final Object[] mAdapterInstances;

        Executor(Filter<S> filter,
                 OrderingList<S> ordering,
                 SQLStatement<S> selectStatement,
                 SQLStatement<S> fromWhereStatement,
                 PropertyFilter<S>[] propertyFilters,
                 boolean[] propertyFilterNullable)
            throws RepositoryException
        {
            mFilter = filter;
            mOrdering = ordering;

            mSelectStatement = selectStatement;
            mMaxSelectStatementLength = selectStatement.maxLength();
            mFromWhereStatement = fromWhereStatement;
            mMaxFromWhereStatementLength = fromWhereStatement.maxLength();

            if (propertyFilters == null) {
                mPropertyFilters = null;
                mPropertyFilterNullable = null;
                mPreparedStatementSetMethods = null;
                mAdapterMethods = null;
                mAdapterInstances = null;
            } else {
                mPropertyFilters = propertyFilters;
                mPropertyFilterNullable = propertyFilterNullable;

                int length = propertyFilters.length;

                mPreparedStatementSetMethods = new Method[length];
                mAdapterMethods = new Method[length];
                mAdapterInstances = new Object[length];

                gatherAdapterMethods(propertyFilters);
            }
        }

        private void gatherAdapterMethods(PropertyFilter<S>[] filters)
            throws RepositoryException
        {
            for (int i=0; i<filters.length; i++) {
                PropertyFilter<S> filter = filters[i];
                ChainedProperty<S> chained = filter.getChainedProperty();
                StorableProperty<?> property = chained.getLastProperty();
                JDBCStorableProperty<?> jProperty =
                    mRepository.getJDBCStorableProperty(property);

                Method psSetMethod = jProperty.getPreparedStatementSetMethod();
                mPreparedStatementSetMethods[i] = psSetMethod;

                StorablePropertyAdapter adapter = jProperty.getAppliedAdapter();
                if (adapter != null) {
                    Class toType = psSetMethod.getParameterTypes()[1];
                    mAdapterMethods[i] = adapter.findAdaptMethod(jProperty.getType(), toType);
                    mAdapterInstances[i] = adapter.getAdapterInstance();
                }
            }
        }

        public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
            boolean forUpdate = mRepository.localTxnManager().isForUpdate();
            Connection con = mRepository.getConnection();
            try {
                PreparedStatement ps = con.prepareStatement(prepareSelect(values, forUpdate));
                Integer fetchSize = mRepository.getFetchSize();
                if (fetchSize != null) {
                    ps.setFetchSize(fetchSize);
                }

                try {
                    setParameters(ps, values);
                    return new JDBCCursor<S>(JDBCStorage.this, con, ps);
                } catch (Exception e) {
                    // in case of exception, close statement
                    try {
                        ps.close();
                    } catch (SQLException e2) {
                        // ignore and allow triggering exception to propagate
                    }
                    throw e;
                }
            } catch (Exception e) {
                // in case of exception, yield connection
                try {
                    mRepository.yieldConnection(con);
                } catch (FetchException e2) {
                   // ignore and allow triggering exception to propagate
                }
                throw mRepository.toFetchException(e);
            }
        }

        @Override
        public long count(FilterValues<S> values) throws FetchException {
            Connection con = mRepository.getConnection();
            try {
                PreparedStatement ps = con.prepareStatement(prepareCount(values));
                try {
                    setParameters(ps, values);
                    ResultSet rs = ps.executeQuery();
                    try {
                        rs.next();
                        return rs.getLong(1);
                    } finally {
                        rs.close();
                    }
                } finally {
                    ps.close();
                }
            } catch (Exception e) {
                throw mRepository.toFetchException(e);
            } finally {
                mRepository.yieldConnection(con);
            }
        }

        public Filter<S> getFilter() {
            return mFilter;
        }

        public OrderingList<S> getOrdering() {
            return mOrdering;
        }

        public boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            boolean forUpdate = mRepository.localTxnManager().isForUpdate();
            app.append(prepareSelect(values, forUpdate));
            app.append('\n');
            return true;
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            try {
                boolean forUpdate = mRepository.localTxnManager().isForUpdate();
                String statement = prepareSelect(values, forUpdate);
                return mRepository.getSupportStrategy().printPlan(app, indentLevel, statement);
            } catch (FetchException e) {
                LogFactory.getLog(JDBCStorage.class).error(null, e);
                return false;
            }
        }

        /**
         * Delete operation is included in cursor factory for ease of implementation.
         */
        int executeDelete(FilterValues<S> filterValues) throws PersistException {
            Connection con;
            try {
                con = mRepository.getConnection();
            } catch (FetchException e) {
                throw e.toPersistException();
            }
            try {
                PreparedStatement ps = con.prepareStatement(prepareDelete(filterValues));
                try {
                    setParameters(ps, filterValues);
                    return ps.executeUpdate();
                } finally {
                    ps.close();
                }
            } catch (Exception e) {
                throw mRepository.toPersistException(e);
            } finally {
                try {
                    mRepository.yieldConnection(con);
                } catch (FetchException e) {
                    throw e.toPersistException();
                }
            }
        }

        private String prepareSelect(FilterValues<S> filterValues, boolean forUpdate) {
            if (!forUpdate) {
                return mSelectStatement.buildStatement(mMaxSelectStatementLength, filterValues);
            }

            // Allocate with extra room for " FOR UPDATE"
            StringBuilder b = new StringBuilder(mMaxSelectStatementLength + 11);
            mSelectStatement.appendTo(b, filterValues);
            b.append(" FOR UPDATE");
            return b.toString();
        }

        private String prepareDelete(FilterValues<S> filterValues) {
            // Allocate with extra room for "DELETE"
            StringBuilder b = new StringBuilder(6 + mMaxFromWhereStatementLength);
            b.append("DELETE");
            mFromWhereStatement.appendTo(b, filterValues);
            return b.toString();
        }

        private String prepareCount(FilterValues<S> filterValues) {
            // Allocate with extra room for "SELECT COUNT(*)"
            StringBuilder b = new StringBuilder(15 + mMaxFromWhereStatementLength);
            b.append("SELECT COUNT(*)");
            mFromWhereStatement.appendTo(b, filterValues);
            return b.toString();
        }

        private void setParameters(PreparedStatement ps, FilterValues<S> filterValues)
            throws Exception
        {
            PropertyFilter<S>[] propertyFilters = mPropertyFilters;

            if (propertyFilters == null) {
                return;
            }

            boolean[] propertyFilterNullable = mPropertyFilterNullable;
            Method[] psSetMethods = mPreparedStatementSetMethods;
            Method[] adapterMethods = mAdapterMethods;
            Object[] adapterInstances = mAdapterInstances;

            int ordinal = 0;
            int psOrdinal = 1; // Start at one since JDBC ordinals are one-based.
            for (PropertyFilter<S> filter : propertyFilters) {
                setValue: {
                    Object value = filterValues.getAssignedValue(filter);

                    if (value == null && propertyFilterNullable[ordinal]) {
                        // No '?' parameter to fill since value "IS NULL" or "IS NOT NULL"
                        break setValue;
                    }

                    Method adapter = adapterMethods[ordinal];
                    if (adapter != null) {
                        value = adapter.invoke(adapterInstances[ordinal], value);
                    }

                    // Special case for converting character to String.
                    if (value != null && value instanceof Character) {
                        value = String.valueOf((Character) value);
                    }

                    psSetMethods[ordinal].invoke(ps, psOrdinal, value);
                    psOrdinal++;
                }

                ordinal++;
            }
        }
    }

    private class JDBCQuery extends StandardQuery<S> {
        JDBCQuery(FilterValues<S> values, OrderingList<S> ordering, QueryExecutor<S> executor) {
            super(values, ordering, executor);
        }

        @Override
        public void deleteAll() throws PersistException {
            if (mTriggerManager.getDeleteTrigger() != null) {
                // Super implementation loads one at time and calls
                // delete. This allows delete trigger to be invoked on each.
                super.deleteAll();
            } else {
                try {
                    ((Executor) executor()).executeDelete(getFilterValues());
                } catch (RepositoryException e) {
                    throw e.toPersistException();
                }
            }
        }

        protected Transaction enterTransaction(IsolationLevel level) {
            return getRootRepository().enterTransaction(level);
        }

        protected QueryFactory<S> queryFactory() {
            return JDBCStorage.this;
        }

        protected QueryExecutorFactory<S> executorFactory() {
            return JDBCStorage.this.mExecutorFactory;
        }

        protected StandardQuery<S> newInstance(FilterValues<S> values,
                                               OrderingList<S> ordering,
                                               QueryExecutor<S> executor)
        {
            return new JDBCQuery(values, ordering, executor);
        }
    }

    /**
     * Node in a tree structure describing how tables are joined together.
     */
    private class JoinNode {
        // Joined property which led to this node. For root node, it is null.
        private final JDBCStorableProperty<?> mProperty;

        private final JDBCStorableInfo<?> mInfo;
        private final String mAlias;

        private final Map<String, JoinNode> mSubNodes;

        /**
         * @param alias table alias in SQL statement, i.e. "T1"
         */
        JoinNode(JDBCStorableInfo<?> info, String alias) {
            this(null, info, alias);
        }

        private JoinNode(JDBCStorableProperty<?> property, JDBCStorableInfo<?> info, String alias)
        {
            mProperty = property;
            mInfo = info;
            mAlias = alias;
            mSubNodes = new LinkedHashMap<String, JoinNode>();
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
            String name = property.getName();
            JoinNode subNode = mSubNodes.get(name);
            if (subNode != null) {
                return subNode.findAliasFor(chained, offset + 1);
            }
            return null;
        }

        public boolean hasAnyJoins() {
            return mSubNodes.size() > 0;
        }

        /**
         * Appends table name to the given FROM clause builder.
         */
        public void appendTableNameTo(StatementBuilder fromClause) {
            fromClause.append(' ');
            fromClause.append(mInfo.getQualifiedTableName());
        }

        /**
         * Appends table name and alias to the given FROM clause builder.
         */
        public void appendTableNameAndAliasTo(StatementBuilder fromClause) {
            appendTableNameTo(fromClause);
            fromClause.append(' ');
            fromClause.append(mAlias);
        }

        /**
         * Appends table names, aliases, and joins to the given FROM clause
         * builder.
         */
        public void appendFullJoinTo(StatementBuilder fromClause) {
            appendTableNameAndAliasTo(fromClause);
            appendTailJoinTo(fromClause);
        }

        private void appendTailJoinTo(StatementBuilder fromClause) {
            for (JoinNode jn : mSubNodes.values()) {
                // TODO: By default, joins are all inner. A join could become
                // LEFT OUTER JOIN if the query filter has a term like this:
                // "address = ? | address.state = ?", and the runtime value of
                // address is null. Because of DNF transformation and lack of
                // short-circuit ops, this syntax might be difficult to parse.
                // This might be a better way of expressing an outer join:
                // "address(.)state = ?".

                fromClause.append(" INNER JOIN");
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

        public void addJoin(ChainedProperty<?> chained, TableAliasGenerator aliasGenerator)
            throws RepositoryException
        {
            addJoin(chained, aliasGenerator, 0);
        }

        private void addJoin(ChainedProperty<?> chained,
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
            String name = property.getName();
            JoinNode subNode = mSubNodes.get(name);
            if (subNode == null) {
                JDBCStorableInfo<?> info = mRepository.examineStorable(property.getJoinedType());
                JDBCStorableProperty<?> jProperty = mRepository.getJDBCStorableProperty(property);
                subNode = new JoinNode(jProperty, info, aliasGenerator.nextAlias());
                mSubNodes.put(name, subNode);
            }
            subNode.addJoin(chained, aliasGenerator, offset + 1);
        }

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
    }

    /**
     * Filter visitor that constructs a JoinNode tree.
     */
    private class JoinNodeBuilder extends Visitor<S, Object, Object> {
        private final TableAliasGenerator mAliasGenerator;
        private final JoinNode mRootJoinNode;

        JoinNodeBuilder(TableAliasGenerator aliasGenerator) {
            mAliasGenerator = aliasGenerator;
            mRootJoinNode = new JoinNode(getStorableInfo(), aliasGenerator.nextAlias());
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
                        mRootJoinNode.addJoin(chained, mAliasGenerator);
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
            mRootJoinNode.addJoin(chained, mAliasGenerator);
        }
    }

    private class StatementBuilder {
        private List<SQLStatement<S>> mStatements;
        private StringBuilder mLiteralBuilder;

        StatementBuilder() {
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
    }

    private class WhereBuilder extends Visitor<S, FetchException, Object> {
        private final StatementBuilder mStatementBuilder;
        private final JoinNode mJoinNode;

        private List<PropertyFilter<S>> mPropertyFilters;
        private List<Boolean> mPropertyFilterNullable;

        WhereBuilder(StatementBuilder statementBuilder, JoinNode jn) {
            mStatementBuilder = statementBuilder;
            mJoinNode = jn;
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
}
