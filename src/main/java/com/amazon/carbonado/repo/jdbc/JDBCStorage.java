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
import com.amazon.carbonado.cursor.EmptyCursor;
import com.amazon.carbonado.cursor.LimitCursor;
import com.amazon.carbonado.filter.AndFilter;
import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.Visitor;
import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableProperty;
import com.amazon.carbonado.info.StorablePropertyAdapter;
import com.amazon.carbonado.qe.AbstractQueryExecutor;
import com.amazon.carbonado.qe.FilteredQueryExecutor;
import com.amazon.carbonado.qe.OrderingList;
import com.amazon.carbonado.qe.QueryExecutor;
import com.amazon.carbonado.qe.QueryExecutorCache;
import com.amazon.carbonado.qe.QueryExecutorFactory;
import com.amazon.carbonado.qe.QueryFactory;
import com.amazon.carbonado.qe.QueryHints;
import com.amazon.carbonado.qe.SortedQueryExecutor;
import com.amazon.carbonado.qe.StandardQuery;
import com.amazon.carbonado.qe.StandardQueryFactory;
import com.amazon.carbonado.sequence.SequenceValueProducer;
import com.amazon.carbonado.spi.TriggerManager;
import com.amazon.carbonado.txn.TransactionScope;
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

    JDBCStorage(JDBCRepository repository, JDBCStorableInfo<S> info,
                boolean isMaster, boolean autoVersioning, boolean suppressReload)
        throws SupportException, RepositoryException
    {
        super(info.getStorableType());
        mRepository = repository;
        mSupportStrategy = repository.getSupportStrategy();
        mInfo = info;

        Class<? extends S> generatedStorableClass = JDBCStorableGenerator
            .getGeneratedClass(info, isMaster, autoVersioning, suppressReload);

        mInstanceFactory = QuickConstructorGenerator
            .getInstance(generatedStorableClass, InstanceFactory.class);

        mExecutorFactory = new QueryExecutorCache<S>(new ExecutorFactory());

        mTriggerManager = new TriggerManager<S>
            (info.getStorableType(), repository.mTriggerFactories);
    }

    @Override
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

            Connection con = getConnection();
            try {
                java.sql.Statement st = con.createStatement();
                try {
                    st.execute(String.format(truncateFormat, mInfo.getQualifiedTableName()));
                } finally {
                    st.close();
                }
            } catch (SQLException e) {
                throw toPersistException(e);
            } finally {
                yieldConnection(con);
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

    public boolean isTransactionForUpdate() {
        return mRepository.isTransactionForUpdate();
    }

    public FetchException toFetchException(Throwable e) {
        return mRepository.toFetchException(e);
    }

    public PersistException toPersistException(Throwable e) {
        return mRepository.toPersistException(e);
    }

    public boolean isUniqueConstraintError(SQLException e) {
        return mRepository.isUniqueConstraintError(e);
    }

    public Connection getConnection() throws FetchException {
        return mRepository.getConnection();
    }

    public void yieldConnection(Connection con) throws FetchException {
        mRepository.yieldConnection(con);
    }

    public String getDatabaseProductName() {
        return mRepository.getDatabaseProductName();
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
                JDBCTransaction txn = mRepository.localTransactionScope().getTxn();
                if (txn != null) {
                    txn.register(jblob);
                }
            } catch (Exception e) {
                throw toFetchException(e);
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
                JDBCTransaction txn = mRepository.localTransactionScope().getTxn();
                if (txn != null) {
                    txn.register(jclob);
                }
            } catch (Exception e) {
                throw toFetchException(e);
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

    @Override
    protected StandardQuery<S> createQuery(Filter<S> filter,
                                           FilterValues<S> values,
                                           OrderingList<S> ordering,
                                           QueryHints hints)
    {
        return new JDBCQuery(filter, values, ordering, hints);
    }

    public S instantiate(ResultSet rs) throws SQLException {
        return (S) mInstanceFactory.instantiate(this, rs, FIRST_RESULT_INDEX);
    }

    public static interface InstanceFactory {
        Storable instantiate(JDBCSupport storage);

        Storable instantiate(JDBCSupport storage, ResultSet rs, int offset) throws SQLException;
    }

    private class ExecutorFactory implements QueryExecutorFactory<S> {
        ExecutorFactory() {
        }

        public Class<S> getStorableType() {
            return JDBCStorage.this.getStorableType();
        }

        public QueryExecutor<S> executor(Filter<S> filter, OrderingList<S> ordering,
                                         QueryHints hints)
            throws RepositoryException
        {
            TableAliasGenerator aliasGenerator = new TableAliasGenerator();

            JoinNode jn;
            try {
                JoinNodeBuilder<S> jnb =
                    new JoinNodeBuilder<S>(mRepository, getStorableInfo(), aliasGenerator);
                if (filter != null) {
                    filter.accept(jnb, null);
                }
                jn = jnb.getRootJoinNode();
                jnb.captureOrderings(ordering);
            } catch (UndeclaredThrowableException e) {
                throw toFetchException(e);
            }

            SQLStatementBuilder<S> selectBuilder = new SQLStatementBuilder<S>(mRepository);
            selectBuilder.append("SELECT ");

            // Don't bother using a table alias for one table. With just one table,
            // there's no need to disambiguate.
            String alias = jn.isAliasRequired() ? jn.getAlias() : null;

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

            SQLStatementBuilder<S> fromWhereBuilder = new SQLStatementBuilder<S>(mRepository);
            fromWhereBuilder.append(" FROM");

            if (alias == null) {
                // Don't bother defining a table alias for one table.
                jn.appendTableNameTo(selectBuilder);
                jn.appendTableNameTo(fromWhereBuilder);
            } else {
                jn.appendFullJoinTo(selectBuilder);
                jn.appendFullJoinTo(fromWhereBuilder);
            }

            // Appending where clause. Remainder filter is required if a
            // derived property is used. Derived properties in exists filters
            // is not supported.

            Filter<S> remainderFilter = null;

            PropertyFilter<S>[] propertyFilters = null;
            boolean[] propertyFilterNullable = null;

            if (filter != null && !filter.isOpen()) {
                Filter<S> sqlFilter = null;
                
                List<Filter<S>> splitList = filter.conjunctiveNormalFormSplit();
                for (Filter<S> split : splitList) {
                    if (usesDerivedProperty(split)) {
                        remainderFilter = and(remainderFilter, split);
                    } else {
                        sqlFilter = and(sqlFilter, split);
                    }
                }

                if (remainderFilter == null) {
                    // Just use original filter.
                    sqlFilter = filter;
                }

                if (sqlFilter == null) {
                    // Just use original filter for remainder.
                    remainderFilter = filter;
                } else {
                    // Build the WHERE clause only if anything to filter on.
                    selectBuilder.append(" WHERE ");
                    fromWhereBuilder.append(" WHERE ");

                    WhereBuilder<S> wb = new WhereBuilder<S>
                        (selectBuilder, alias == null ? null : jn, aliasGenerator);
                    FetchException e = sqlFilter.accept(wb, null);
                    if (e != null) {
                        throw e;
                    }

                    propertyFilters = wb.getPropertyFilters();
                    propertyFilterNullable = wb.getPropertyFilterNullable();

                    wb = new WhereBuilder<S>
                        (fromWhereBuilder, alias == null ? null : jn, aliasGenerator);
                    e = sqlFilter.accept(wb, null);
                    if (e != null) {
                        throw e;
                    }
                }
            }

            // Append order-by clause. Remainder ordering is required if a derived
            // property is used.

            OrderingList<S> sqlOrdering = ordering;
            OrderingList<S> remainderOrdering = null;

            if (ordering != null && ordering.size() > 0) {
                ordinal = 0;
                for (OrderedProperty<S> orderedProperty : ordering) {
                    if (orderedProperty.getChainedProperty().isDerived()) {
                        sqlOrdering = ordering.subList(0, ordinal);
                        remainderOrdering = ordering.subList(ordinal, ordering.size());
                        break;
                    }
                    ordinal++;
                }

                if (sqlOrdering != null && sqlOrdering.size() > 0) {
                    selectBuilder.append(" ORDER BY ");
                    ordinal = 0;
                    for (OrderedProperty<S> orderedProperty : sqlOrdering) {
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
            }

            QueryExecutor<S> executor = new Executor(filter,
                                                     sqlOrdering,
                                                     selectBuilder.build(),
                                                     fromWhereBuilder.build(),
                                                     propertyFilters,
                                                     propertyFilterNullable);

            if (remainderFilter != null && !remainderFilter.isOpen()) {
                executor = new FilteredQueryExecutor<S>(executor, remainderFilter);
            }

            if (remainderOrdering != null && remainderOrdering.size() > 0) {
                executor = new SortedQueryExecutor<S>
                    (new SortedQueryExecutor.MergeSortSupport(),
                     executor, sqlOrdering, remainderOrdering);
            }

            return executor;
        }

        private Filter<S> and(Filter<S> left, Filter<S> right) {
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return left.and(right);
        }

        private boolean usesDerivedProperty(Filter<S> filter) {
            Boolean result = filter.accept(new Visitor<S, Boolean, Object>() {
                @Override
                public Boolean visit(OrFilter<S> orFilter, Object param) {
                    Boolean result = orFilter.getLeftFilter().accept(this, param);
                    if (result != null && result) {
                        // Short-circuit.
                        return result;
                    }
                    return orFilter.getRightFilter().accept(this, param);
                }

                @Override
                public Boolean visit(AndFilter<S> andFilter, Object param) {
                    Boolean result = andFilter.getLeftFilter().accept(this, param);
                    if (result != null && result) {
                        // Short-circuit.
                        return result;
                    }
                    return andFilter.getRightFilter().accept(this, param);
                }

                @Override
                public Boolean visit(PropertyFilter<S> propFilter, Object param) {
                    return propFilter.getChainedProperty().isDerived();
                }
            }, null);

            return result != null && result;
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
                    // Special case for converting character to String.
                    if (mAdapterMethods[i] == null) {
                        if (toType == String.class) {
                            mAdapterMethods[i] = adapter
                                .findAdaptMethod(jProperty.getType(), Character.class);
                            if (mAdapterMethods[i] == null) {
                                mAdapterMethods[i] = adapter
                                    .findAdaptMethod(jProperty.getType(), char.class);
                            }
                        }
                    }
                    mAdapterInstances[i] = adapter.getAdapterInstance();
                }
            }
        }

        public Cursor<S> fetch(FilterValues<S> values) throws FetchException {
            TransactionScope<JDBCTransaction> scope = mRepository.localTransactionScope();
            boolean forUpdate = scope.isForUpdate();
            Connection con = getConnection();
            try {
                PreparedStatement ps = con.prepareStatement(prepareSelect(values, forUpdate));
                Integer fetchSize = mRepository.getFetchSize();
                if (fetchSize != null) {
                    ps.setFetchSize(fetchSize);
                }

                try {
                    setParameters(ps, values);
                    return new JDBCCursor<S>(JDBCStorage.this, scope, con, ps);
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
                    yieldConnection(con);
                } catch (FetchException e2) {
                   // ignore and allow triggering exception to propagate
                }
                throw toFetchException(e);
            }
        }

        @Override
        public Cursor<S> fetchSlice(FilterValues<S> values, long from, Long to)
            throws FetchException
        {
            if (to != null && (to - from) <= 0) {
                return EmptyCursor.the();
            }

            JDBCSupportStrategy.SliceOption option = mSupportStrategy.getSliceOption();

            String select;

            switch (option) {
            case NOT_SUPPORTED: default:
                return super.fetchSlice(values, from, to);
            case LIMIT_ONLY:
                if (from > 0 || to == null) {
                    return super.fetchSlice(values, from, to);
                }
                select = prepareSelect(values, false);
                select = mSupportStrategy.buildSelectWithSlice(select, false, true);
                break;
            case OFFSET_ONLY:
                if (from <= 0) {
                    return super.fetchSlice(values, from, to);
                }
                select = prepareSelect(values, false);
                select = mSupportStrategy.buildSelectWithSlice(select, true, false);
                break;
            case LIMIT_AND_OFFSET:
            case OFFSET_AND_LIMIT:
            case FROM_AND_TO:
                select = prepareSelect(values, false);
                select = mSupportStrategy.buildSelectWithSlice(select, from > 0, to != null);
                break;
            }

            TransactionScope<JDBCTransaction> scope = mRepository.localTransactionScope();
            if (scope.isForUpdate()) {
                select = select.concat(" FOR UPDATE");
            }

            Connection con = getConnection();
            try {
                PreparedStatement ps = con.prepareStatement(select);
                Integer fetchSize = mRepository.getFetchSize();
                if (fetchSize != null) {
                    ps.setFetchSize(fetchSize);
                }

                try {
                    int psOrdinal = setParameters(ps, values);

                    if (from > 0) {
                        if (to != null) {
                            switch (option) {
                            case OFFSET_ONLY:
                                ps.setLong(psOrdinal, from);
                                Cursor<S> c = new JDBCCursor<S>(JDBCStorage.this, scope, con, ps);
                                return new LimitCursor<S>(c, to - from);
                            case LIMIT_AND_OFFSET:
                                ps.setLong(psOrdinal, to - from);
                                ps.setLong(psOrdinal + 1, from);
                                break;
                            case OFFSET_AND_LIMIT:
                                ps.setLong(psOrdinal, from);
                                ps.setLong(psOrdinal + 1, to - from);
                                break;
                            case FROM_AND_TO:
                                ps.setLong(psOrdinal, from);
                                ps.setLong(psOrdinal + 1, to);
                                break;
                            }
                        } else {
                            ps.setLong(psOrdinal, from);
                        }
                    } else if (to != null) {
                        ps.setLong(psOrdinal, to);
                    }

                    return new JDBCCursor<S>(JDBCStorage.this, scope, con, ps);
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
                    yieldConnection(con);
                } catch (FetchException e2) {
                   // ignore and allow triggering exception to propagate
                }
                throw toFetchException(e);
            }
        }

        @Override
        public long count(FilterValues<S> values) throws FetchException {
            Connection con = getConnection();
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
                throw toFetchException(e);
            } finally {
                yieldConnection(con);
            }
        }

        public Filter<S> getFilter() {
            return mFilter;
        }

        public OrderingList<S> getOrdering() {
            return mOrdering;
        }

        @Override
        public boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            indent(app, indentLevel);
            boolean forUpdate = mRepository.localTransactionScope().isForUpdate();
            app.append(prepareSelect(values, forUpdate));
            app.append('\n');
            return true;
        }

        public boolean printPlan(Appendable app, int indentLevel, FilterValues<S> values)
            throws IOException
        {
            try {
                boolean forUpdate = mRepository.localTransactionScope().isForUpdate();
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
                con = getConnection();
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
                throw toPersistException(e);
            } finally {
                try {
                    yieldConnection(con);
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

        /**
         * @return next value ordinal
         */
        private int setParameters(PreparedStatement ps, FilterValues<S> filterValues)
            throws Exception
        {
            PropertyFilter<S>[] propertyFilters = mPropertyFilters;

            if (propertyFilters == null) {
                return 1;
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

            return psOrdinal;
        }
    }

    private class JDBCQuery extends StandardQuery<S> {
        JDBCQuery(Filter<S> filter,
                  FilterValues<S> values,
                  OrderingList<S> ordering,
                  QueryHints hints)
        {
            super(filter, values, ordering, hints);
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

        @Override
        protected Transaction enterTransaction(IsolationLevel level) {
            return getRootRepository().enterTransaction(level);
        }

        @Override
        protected QueryFactory<S> queryFactory() {
            return JDBCStorage.this;
        }

        @Override
        protected QueryExecutorFactory<S> executorFactory() {
            return JDBCStorage.this.mExecutorFactory;
        }

        @Override
        protected StandardQuery<S> newInstance(FilterValues<S> values, OrderingList<S> ordering,
                                               QueryHints hints)
        {
            return new JDBCQuery(values.getFilter(), values, ordering, hints);
        }
    }
}
