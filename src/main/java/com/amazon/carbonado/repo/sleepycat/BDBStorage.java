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

package com.amazon.carbonado.repo.sleepycat;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistDeadlockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.capability.IndexInfo;

import com.amazon.carbonado.cursor.EmptyCursor;
import com.amazon.carbonado.cursor.MergeSortBuffer;
import com.amazon.carbonado.cursor.SingletonCursor;
import com.amazon.carbonado.cursor.SortBuffer;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutFactory;
import com.amazon.carbonado.layout.LayoutOptions;
import com.amazon.carbonado.layout.Unevolvable;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.Clob;

import com.amazon.carbonado.qe.BoundaryType;
import com.amazon.carbonado.qe.QueryEngine;
import com.amazon.carbonado.qe.QueryExecutorFactory;
import com.amazon.carbonado.qe.StorableIndexSet;
import com.amazon.carbonado.qe.StorageAccess;

import com.amazon.carbonado.raw.StorableCodec;
import com.amazon.carbonado.raw.StorableCodecFactory;
import com.amazon.carbonado.raw.RawSupport;
import com.amazon.carbonado.raw.RawUtil;

import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.spi.IndexInfoImpl;
import com.amazon.carbonado.spi.LobEngine;
import com.amazon.carbonado.spi.TriggerManager;

import com.amazon.carbonado.txn.TransactionScope;

/**
 *
 * @author Brian S O'Neill
 */
abstract class BDBStorage<Txn, S extends Storable> implements Storage<S>, StorageAccess<S> {
    /** Constant indicating success */
    protected static final byte[] SUCCESS = new byte[0];

    /** Constant indicating an entry was not found */
    protected static final byte[] NOT_FOUND = new byte[0];

    /** Constant indicating an entry already exists */
    protected static final Object KEY_EXIST = new Object();

    private static final int DEFAULT_LOB_BLOCK_SIZE = 1000;

    final BDBRepository<Txn> mRepository;
    /** Reference to the type of storable */
    private final Class<S> mType;

    /** Does most of the work in generating storables, used for preparing and querying  */
    StorableCodec<S> mStorableCodec;

    /**
     * Reference to an instance of Proxy, defined in this class, which binds
     * the storables to our own implementation. Handed off to mStorableFactory.
     */
    private final RawSupport<S> mRawSupport;

    /** Primary key index is required, and is the only one supported. */
    private StorableIndex<S> mPrimaryKeyIndex;

    /** Reference to primary database. */
    private Object mPrimaryDatabase;

    /** Reference to query engine, defined later in this class */
    private QueryEngine<S> mQueryEngine;

    final TriggerManager<S> mTriggerManager;

    /**
     * Constructs a storage instance, but subclass must call open before it can
     * be used.
     *
     * @param repository repository this storage came from
     * @throws SupportException if storable type is not supported
     */
    protected BDBStorage(BDBRepository<Txn> repository, Class<S> type)
        throws SupportException
    {
        mRepository = repository;
        mType = type;
        mRawSupport = new Support(repository, this);
        mTriggerManager = new TriggerManager<S>();
        try {
            // Ask if any lobs via static method first, to prevent stack
            // overflow that occurs when creating BDBStorage instances for
            // metatypes. These metatypes cannot support Lobs.
            if (LobEngine.hasLobs(type)) {
                Trigger<S> lobTrigger = repository.getLobEngine()
                    .getSupportTrigger(type, DEFAULT_LOB_BLOCK_SIZE);
                addTrigger(lobTrigger);
            }
        } catch (SupportException e) {
            throw e;
        } catch (RepositoryException e) {
            throw new SupportException(e);
        }
    }

    public Class<S> getStorableType() {
        return mType;
    }

    public S prepare() {
        return mStorableCodec.instantiate();
    }

    public Query<S> query() throws FetchException {
        return mQueryEngine.query();
    }

    public Query<S> query(String filter) throws FetchException {
        return mQueryEngine.query(filter);
    }

    public Query<S> query(Filter<S> filter) throws FetchException {
        return mQueryEngine.query(filter);
    }

    /**
     * @since 1.2
     */
    public void truncate() throws PersistException {
        if (mTriggerManager.getDeleteTrigger() != null || mRepository.mSingleFileName != null) {
            final int batchSize = 100;

            while (true) {
                Transaction txn = mRepository.enterTransaction(IsolationLevel.READ_COMMITTED);
                txn.setForUpdate(true);
                try {
                    Cursor<S> cursor = query().fetch();
                    if (!cursor.hasNext()) {
                        return;
                    }
                    int count = 0;
                    do {
                        cursor.next().tryDelete();
                    } while (count++ < batchSize && cursor.hasNext());
                    txn.commit();
                } catch (FetchException e) {
                    throw e.toPersistException();
                } finally {
                    txn.exit();
                }
            }
        }

        TransactionScope<Txn> scope = localTransactionScope();

        // Lock out shutdown task.
        scope.getLock().lock();
        try {
            try {
                db_truncate(scope.getTxn());
            } catch (Exception e) {
                throw toPersistException(e);
            }
        } finally {
            scope.getLock().unlock();
        }
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.addTrigger(trigger);
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.removeTrigger(trigger);
    }

    public IndexInfo[] getIndexInfo() {
        StorableIndex<S> pkIndex = mPrimaryKeyIndex;

        if (pkIndex == null) {
            return new IndexInfo[0];
        }

        int i = pkIndex.getPropertyCount();
        String[] propertyNames = new String[i];
        Direction[] directions = new Direction[i];
        while (--i >= 0) {
            propertyNames[i] = pkIndex.getProperty(i).getName();
            directions[i] = pkIndex.getPropertyDirection(i);
        }

        return new IndexInfo[] {
            new IndexInfoImpl(getStorableType().getName(), true, true, propertyNames, directions)
        };
    }

    public QueryExecutorFactory<S> getQueryExecutorFactory() {
        return mQueryEngine;
    }

    public Collection<StorableIndex<S>> getAllIndexes() {
        return Collections.singletonList(mPrimaryKeyIndex);
    }

    public Storage<S> storageDelegate(StorableIndex<S> index) {
        // We're the grunt and don't delegate.
        return null;
    }

    public SortBuffer<S> createSortBuffer() {
        return new MergeSortBuffer<S>();
    }

    public long countAll() throws FetchException {
        // Return -1 to indicate default algorithm should be used.
        return -1;
    }

    public Cursor<S> fetchAll() throws FetchException {
        return fetchSubset(null, null,
                           BoundaryType.OPEN, null,
                           BoundaryType.OPEN, null,
                           false, false);
    }

    public Cursor<S> fetchOne(StorableIndex<S> index,
                              Object[] identityValues)
        throws FetchException
    {
        byte[] key = mStorableCodec.encodePrimaryKey(identityValues);
        byte[] value = mRawSupport.tryLoad(null, key);
        if (value == null) {
            return EmptyCursor.the();
        }
        return new SingletonCursor<S>(instantiate(key, value));
    }

    public Query<?> indexEntryQuery(StorableIndex<S> index) {
        return null;
    }

    public Cursor<S> fetchFromIndexEntryQuery(StorableIndex<S> index, Query<?> indexEntryQuery) {
        // This method should never be called since null was returned by indexEntryQuery.
        throw new UnsupportedOperationException();
    }

    public Cursor<S> fetchSubset(StorableIndex<S> index,
                                 Object[] identityValues,
                                 BoundaryType rangeStartBoundary,
                                 Object rangeStartValue,
                                 BoundaryType rangeEndBoundary,
                                 Object rangeEndValue,
                                 boolean reverseRange,
                                 boolean reverseOrder)
        throws FetchException
    {
        TransactionScope<Txn> scope = localTransactionScope();

        if (reverseRange) {
            {
                BoundaryType temp = rangeStartBoundary;
                rangeStartBoundary = rangeEndBoundary;
                rangeEndBoundary = temp;
            }

            {
                Object temp = rangeStartValue;
                rangeStartValue = rangeEndValue;
                rangeEndValue = temp;
            }
        }

        // Lock out shutdown task.
        scope.getLock().lock();
        try {
            StorableCodec<S> codec = mStorableCodec;

            final byte[] identityKey;
            if (identityValues == null || identityValues.length == 0) {
                identityKey = codec.encodePrimaryKeyPrefix();
            } else {
                identityKey = codec.encodePrimaryKey(identityValues, 0, identityValues.length);
            }

            final byte[] startBound;
            if (rangeStartBoundary == BoundaryType.OPEN) {
                startBound = identityKey;
            } else {
                startBound = createBound(identityValues, identityKey, rangeStartValue, codec);
                if (!reverseOrder && rangeStartBoundary == BoundaryType.EXCLUSIVE) {
                    // If key is composite and partial, need to skip trailing
                    // unspecified keys by adding one and making inclusive.
                    if (!RawUtil.increment(startBound)) {
                        return EmptyCursor.the();
                    }
                    rangeStartBoundary = BoundaryType.INCLUSIVE;
                }
            }

            final byte[] endBound;
            if (rangeEndBoundary == BoundaryType.OPEN) {
                endBound = identityKey;
            } else {
                endBound = createBound(identityValues, identityKey, rangeEndValue, codec);
                if (reverseOrder && rangeEndBoundary == BoundaryType.EXCLUSIVE) {
                    // If key is composite and partial, need to skip trailing
                    // unspecified keys by subtracting one and making
                    // inclusive.
                    if (!RawUtil.decrement(endBound)) {
                        return EmptyCursor.the();
                    }
                    rangeEndBoundary = BoundaryType.INCLUSIVE;
                }
            }

            final boolean inclusiveStart = rangeStartBoundary != BoundaryType.EXCLUSIVE;
            final boolean inclusiveEnd = rangeEndBoundary != BoundaryType.EXCLUSIVE;

            try {
                BDBCursor<Txn, S> cursor = openCursor
                    (scope,
                     startBound, inclusiveStart,
                     endBound, inclusiveEnd,
                     mStorableCodec.getPrimaryKeyPrefixLength(),
                     reverseOrder,
                     getPrimaryDatabase());

                cursor.open();
                return cursor;
            } catch (Exception e) {
                throw toFetchException(e);
            }
        } finally {
            scope.getLock().unlock();
        }
    }

    private byte[] createBound(Object[] exactValues, byte[] exactKey, Object rangeValue,
                               StorableCodec<S> codec) {
        Object[] values = {rangeValue};
        if (exactValues == null || exactValues.length == 0) {
            return codec.encodePrimaryKey(values, 0, 1);
        }

        byte[] rangeKey = codec.encodePrimaryKey
            (values, exactValues.length, exactValues.length + 1);
        byte[] bound = new byte[exactKey.length + rangeKey.length];
        System.arraycopy(exactKey, 0, bound, 0, exactKey.length);
        System.arraycopy(rangeKey, 0, bound, exactKey.length, rangeKey.length);
        return bound;
    }

    protected BDBRepository getRepository() {
        return mRepository;
    }

    /**
     * @param readOnly when true, this method will not attempt to reconcile
     * differences between the current index set and the desired index set.
     */
    protected void open(boolean readOnly) throws RepositoryException {
        open(readOnly, null, true);
    }

    protected void open(boolean readOnly, Txn openTxn, boolean installTriggers)
        throws RepositoryException
    {
        StorableInfo<S> info = StorableIntrospector.examine(getStorableType());
        StorableCodecFactory codecFactory = mRepository.getStorableCodecFactory();
        final Layout layout = getLayout(codecFactory);

        // Open primary database.
        Object primaryDatabase;

        String databaseName = codecFactory.getStorageName(getStorableType());
        if (databaseName == null) {
            databaseName = getStorableType().getName();
        }

        // Primary info may be null for StoredDatabaseInfo itself.
        StoredDatabaseInfo primaryInfo;
        boolean isPrimaryEmpty;

        try {
            TransactionScope<Txn> scope = mRepository.localTransactionScope();
            // Lock out shutdown task.
            scope.getLock().lock();
            try {
                primaryDatabase = env_openPrimaryDatabase(openTxn, databaseName);
                primaryInfo = registerPrimaryDatabase(readOnly, layout);
                isPrimaryEmpty = db_isEmpty(null, primaryDatabase, scope.isForUpdate());
            } finally {
                scope.getLock().unlock();
            }
        } catch (Exception e) {
            throw toRepositoryException(e);
        }

        StorableIndex<S> desiredPkIndex;
        {
            // In order to select the best index for the primary key, allow all
            // indexes to be considered.
            StorableIndexSet<S> indexSet = new StorableIndexSet<S>();
            indexSet.addIndexes(info);
            indexSet.addAlternateKeys(info);
            indexSet.addPrimaryKey(info);

            indexSet.reduce(Direction.ASCENDING);

            desiredPkIndex = indexSet.findPrimaryKeyIndex(info);
        }

        StorableIndex<S> pkIndex;

        if (!isPrimaryEmpty && primaryInfo != null
            && primaryInfo.getIndexNameDescriptor() != null)
        {
            // Entries already exist, so primary key format is locked in.
            try {
                pkIndex = verifyPrimaryKey(info, desiredPkIndex,
                                           primaryInfo.getIndexNameDescriptor(),
                                           primaryInfo.getIndexTypeDescriptor());
            } catch (SupportException e) {
                try {
                    db_close(primaryDatabase);
                } catch (Exception e2) {
                    // Don't care.
                }
                throw e;
            }
        } else {
            pkIndex = desiredPkIndex;

            if (primaryInfo != null) {
                if (!pkIndex.getNameDescriptor().equals(primaryInfo.getIndexNameDescriptor()) ||
                    !pkIndex.getTypeDescriptor().equals(primaryInfo.getIndexTypeDescriptor())) {

                    primaryInfo.setIndexNameDescriptor(pkIndex.getNameDescriptor());
                    primaryInfo.setIndexTypeDescriptor(pkIndex.getTypeDescriptor());

                    if (!readOnly) {
                        Repository repo = mRepository.getRootRepository();
                        try {
                            Transaction txn =
                                repo.enterTopTransaction(IsolationLevel.READ_COMMITTED);
                            try {
                                primaryInfo.update();
                                txn.commit();
                            } finally {
                                txn.exit();
                            }
                        } catch (PersistException e) {
                            if (e instanceof PersistDeadlockException ||
                                e instanceof PersistTimeoutException)
                            {
                                // Might be caused by coarse locks. Switch to
                                // nested transaction to share the locks.
                                Transaction txn =
                                    repo.enterTransaction(IsolationLevel.READ_COMMITTED);
                                try {
                                    primaryInfo.update();
                                    txn.commit();
                                } finally {
                                    txn.exit();
                                }
                            } else {
                                throw e;
                            }
                        }
                    }
                }
            }
        }

        // Indicate that primary key is clustered, which can affect query analysis.
        pkIndex = pkIndex.clustered(true);

        try {
            mStorableCodec = codecFactory
                .createCodec(getStorableType(), pkIndex, mRepository.isMaster(), layout,
                             mRawSupport);
        } catch (SupportException e) {
            // We've opened the database prematurely, since type isn't
            // supported by encoding strategy. Close it down and unregister.
            try {
                db_close(primaryDatabase);
            } catch (Exception e2) {
                // Don't care.
            }
            try {
                unregisterDatabase(readOnly, getStorableType().getName());
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }

        mPrimaryKeyIndex = mStorableCodec.getPrimaryKeyIndex();
        mPrimaryDatabase = primaryDatabase;

        mQueryEngine = new QueryEngine<S>(getStorableType(), mRepository);

        if (installTriggers) {
            // Don't install automatic triggers until we're completely ready.
            mTriggerManager.addTriggers(getStorableType(), mRepository.mTriggerFactories);
        }
    }

    protected S instantiate(byte[] key, byte[] value) throws FetchException {
        return mStorableCodec.instantiate(key, value);
    }

    protected CompactionCapability.Result<S> compact() throws RepositoryException {
        byte[] start = mStorableCodec.encodePrimaryKeyPrefix();
        if (start != null && start.length == 0) {
            start = null;
        }

        byte[] end;
        if (start == null) {
            end = null;
        } else {
            end = start.clone();
            if (!RawUtil.increment(end)) {
                end = null;
            }
        }

        try {
            Txn txn = mRepository.localTransactionScope().getTxn();
            return db_compact(txn, mPrimaryDatabase, start, end);
        } catch (Exception e) {
            throw mRepository.toRepositoryException(e);
        }
    }

    /**
     * @return true if record with given key exists
     */
    protected abstract boolean db_exists(Txn txn, byte[] key, boolean rmw) throws Exception;

    /**
     * @return NOT_FOUND, any byte[], or null (if empty result)
     */
    protected abstract byte[] db_get(Txn txn, byte[] key, boolean rmw) throws Exception;

    /**
     * @return SUCCESS, KEY_EXIST, or NOT_FOUND otherwise
     */
    protected abstract Object db_putNoOverwrite(Txn txn, byte[] key, byte[] value)
        throws Exception;

    /**
     * @return true if successful
     */
    protected abstract boolean db_put(Txn txn, byte[] key, byte[] value)
        throws Exception;

    /**
     * @return true if successful
     */
    protected abstract boolean db_delete(Txn txn, byte[] key)
        throws Exception;

    protected abstract void db_truncate(Txn txn) throws Exception;

    /**
     * @return true if database has no entries.
     */
    protected abstract boolean db_isEmpty(Txn txn, Object database, boolean rmw) throws Exception;

    protected CompactionCapability.Result<S> db_compact
        (Txn txn, Object database, byte[] start, byte[] end)
        throws Exception
    {
        throw new UnsupportedOperationException();
    }

    protected abstract void db_close(Object database) throws Exception;

    /**
     * Implementation should call runDatabasePrepareForOpeningHook on database
     * before opening.
     */
    protected abstract Object env_openPrimaryDatabase(Txn txn, String name) throws Exception;

    protected void runDatabasePrepareForOpeningHook(Object database) throws RepositoryException {
        mRepository.runDatabasePrepareForOpeningHook(database);
    }

    protected abstract void env_removeDatabase(Txn txn, String databaseName) throws Exception;

    /**
     * @param txn optional transaction to commit when cursor is closed
     * @param scope
     * @param startBound specify the starting key for the cursor, or null if first
     * @param inclusiveStart true if start bound is inclusive
     * @param endBound specify the ending key for the cursor, or null if last
     * @param inclusiveEnd true if end bound is inclusive
     * @param maxPrefix maximum expected common initial bytes in start and end bound
     * @param reverse when true, iteration is reversed
     * @param database database to use
     */
    protected abstract BDBCursor<Txn, S> openCursor
        (TransactionScope<Txn> scope,
         byte[] startBound, boolean inclusiveStart,
         byte[] endBound, boolean inclusiveEnd,
         int maxPrefix,
         boolean reverse,
         Object database)
        throws Exception;

    FetchException toFetchException(Throwable e) {
        return mRepository.toFetchException(e);
    }

    PersistException toPersistException(Throwable e) {
        return mRepository.toPersistException(e);
    }

    RepositoryException toRepositoryException(Throwable e) {
        return mRepository.toRepositoryException(e);
    }

    TransactionScope<Txn> localTransactionScope() {
        return mRepository.localTransactionScope();
    }

    /**
     * Caller must hold transaction lock. May throw FetchException if storage
     * is closed.
     */
    Object getPrimaryDatabase() throws FetchException {
        Object database = mPrimaryDatabase;
        if (database == null) {
            checkClosed();
            throw new IllegalStateException("BDBStorage not opened");
        }
        return database;
    }

    Blob getBlob(S storable, String name, long locator) throws FetchException {
        try {
            return mRepository.getLobEngine().getBlobValue(locator);
        } catch (RepositoryException e) {
            throw e.toFetchException();
        }
    }

    long getLocator(Blob blob) throws PersistException {
        try {
            return mRepository.getLobEngine().getLocator(blob);
        } catch (ClassCastException e) {
            throw new PersistException(e);
        } catch (RepositoryException e) {
            throw e.toPersistException();
        }
    }

    Clob getClob(S storable, String name, long locator) throws FetchException {
        try {
            return mRepository.getLobEngine().getClobValue(locator);
        } catch (RepositoryException e) {
            throw e.toFetchException();
        }
    }

    long getLocator(Clob clob) throws PersistException {
        try {
            return mRepository.getLobEngine().getLocator(clob);
        } catch (ClassCastException e) {
            throw new PersistException(e);
        } catch (RepositoryException e) {
            throw e.toPersistException();
        }
    }

    /**
     * If open, returns normally. If shutting down, blocks forever. Otherwise,
     * if closed, throws FetchException. Method blocks forever on shutdown to
     * prevent threads from starting work that will likely fail along the way.
     */
    void checkClosed() throws FetchException {
        TransactionScope<Txn> scope = localTransactionScope();

        // Lock out shutdown task.
        scope.getLock().lock();
        try {
            if (mPrimaryDatabase == null) {
                // If shuting down, this will force us to block forever.
                try {
                    scope.getTxn();
                } catch (Exception e) {
                    // Don't care.
                }
                // Okay, not shutting down, so throw exception.
                throw new FetchException("Repository closed");
            }
        } finally {
            scope.getLock().unlock();
        }
    }

    void close() throws Exception {
        TransactionScope<Txn> scope = mRepository.localTransactionScope();
        scope.getLock().lock();
        try {
            if (mPrimaryDatabase != null) {
                db_close(mPrimaryDatabase);
                mPrimaryDatabase = null;
            }
        } finally {
            scope.getLock().unlock();
        }
    }

    Layout getLayout(StorableCodecFactory codecFactory) throws RepositoryException {
        if (Unevolvable.class.isAssignableFrom(getStorableType())) {
            // Don't record generation for storables marked as unevolvable.
            return null;
        }

        LayoutFactory factory;
        try {
            factory = mRepository.getLayoutFactory();
        } catch (SupportException e) {
            // Metadata repository does not support layout storables, so it
            // cannot support generations.
            return null;
        }

        Class<S> type = getStorableType();
        return factory.layoutFor(type, codecFactory.getLayoutOptions(type));
    }

    /**
     * Note: returned StoredDatabaseInfo does not have name and type
     * descriptors saved yet.
     *
     * @return null if type cannot be registered
     */
    private StoredDatabaseInfo registerPrimaryDatabase(boolean readOnly, Layout layout)
        throws Exception
    {
        if (getStorableType() == StoredDatabaseInfo.class) {
            // Can't register itself in itself.
            return null;
        }

        Repository repo = mRepository.getRootRepository();

        StoredDatabaseInfo info;
        try {
            info = repo.storageFor(StoredDatabaseInfo.class).prepare();
        } catch (SupportException e) {
            return null;
        }
        info.setDatabaseName(getStorableType().getName());

        // Try to insert metadata up to three times.
        boolean top = true;
        for (int retryCount = 3;;) {
            try {
                Transaction txn;
                if (top) {
                    txn = repo.enterTopTransaction(IsolationLevel.READ_COMMITTED);
                } else {
                    txn = repo.enterTransaction(IsolationLevel.READ_COMMITTED);
                }

                txn.setForUpdate(true);
                try {
                    if (!info.tryLoad()) {
                        if (layout == null) {
                            info.setEvolutionStrategy(StoredDatabaseInfo.EVOLUTION_NONE);
                        } else {
                            info.setEvolutionStrategy(StoredDatabaseInfo.EVOLUTION_STANDARD);
                        }
                        info.setCreationTimestamp(System.currentTimeMillis());
                        info.setVersionNumber(0);
                        if (!readOnly) {
                            info.insert();
                        }
                    }
                    txn.commit();
                } finally {
                    txn.exit();
                }
                break;
            } catch (UniqueConstraintException e) {
                // This might be caused by a transient replication error. Retry
                // a few times before throwing exception. Wait up to a second
                // before each retry.
                retryCount = e.backoff(e, retryCount, 1000);
            } catch (FetchException e) {
                if (e instanceof FetchDeadlockException || e instanceof FetchTimeoutException) {
                    // Might be caused by coarse locks. Switch to nested
                    // transaction to share the locks.
                    if (top) {
                        top = false;
                        retryCount = e.backoff(e, retryCount, 100);
                        continue;
                    }
                }
                throw e;
            } catch (PersistException e) {
                if (e instanceof PersistDeadlockException || e instanceof PersistTimeoutException){
                    // Might be caused by coarse locks. Switch to nested
                    // transaction to share the locks.
                    if (top) {
                        top = false;
                        retryCount = e.backoff(e, retryCount, 100);
                        continue;
                    }
                }
                throw e;
            }
        }

        return info;
    }

    private void unregisterDatabase(boolean readOnly, String name) throws RepositoryException {
        if (getStorableType() == StoredDatabaseInfo.class) {
            // Can't unregister when register wasn't allowed.
            return;
        }
        if (!readOnly) {
            Repository repo = mRepository.getRootRepository();

            StoredDatabaseInfo info;
            try {
                info = repo.storageFor(StoredDatabaseInfo.class).prepare();
            } catch (SupportException e) {
                return;
            }
            info.setDatabaseName(name);

            Transaction txn = repo.enterTopTransaction(IsolationLevel.READ_COMMITTED);
            try {
                info.delete();
                txn.commit();
            } finally {
                txn.exit();
            }
        }
    }

    private StorableIndex<S> verifyPrimaryKey(StorableInfo<S> info,
                                              StorableIndex<S> desiredPkIndex,
                                              String nameDescriptor,
                                              String typeDescriptor)
        throws SupportException
    {
        StorableIndex<S> pkIndex;
        try {
            pkIndex = StorableIndex.parseNameDescriptor(nameDescriptor, info);
        } catch (IllegalArgumentException e) {
            throw new SupportException
                ("Existing primary key apparently refers to properties which " +
                 "no longer exist. Primary key cannot change if Storage<" +
                 info.getStorableType().getName() + "> is not empty. " +
                 "Primary key name descriptor: " + nameDescriptor + ", error: " +
                 e.getMessage());
        }

        if (!nameDescriptor.equals(desiredPkIndex.getNameDescriptor())) {
            throw new SupportException
                (buildIndexMismatchMessage(info, pkIndex, desiredPkIndex, null, false));
        }

        if (!typeDescriptor.equals(desiredPkIndex.getTypeDescriptor())) {
            throw new SupportException
                (buildIndexMismatchMessage(info, pkIndex, desiredPkIndex, typeDescriptor, true));
        }
    
        return pkIndex;
    }

    private String buildIndexMismatchMessage(StorableInfo<S> info,
                                             StorableIndex<S> pkIndex,
                                             StorableIndex<S> desiredPkIndex,
                                             String typeDescriptor,
                                             boolean showDesiredType)
    {
        StringBuilder message = new StringBuilder();
        message.append("Cannot change primary key if Storage<" + info.getStorableType().getName() +
                       "> is not empty. Primary key was ");
        appendIndexDecl(message, pkIndex, typeDescriptor, false);
        message.append(", but new specification is ");
        appendIndexDecl(message, desiredPkIndex, null, showDesiredType);
        return message.toString();
    }

    private void appendIndexDecl(StringBuilder buf, StorableIndex<S> index,
                                 String typeDescriptor, boolean showDesiredType)
    {
        buf.append('[');
        int count = index.getPropertyCount();

        TypeDesc[] types = null;
        boolean[] nullable = null;

        if (typeDescriptor != null) {
            types = new TypeDesc[count];
            nullable = new boolean[count];

            try {
                for (int i=0; i<count; i++) {
                    if (typeDescriptor.charAt(0) == 'N') {
                        typeDescriptor = typeDescriptor.substring(1);
                        nullable[i] = true;
                    }

                    String typeStr;

                    if (typeDescriptor.charAt(0) == 'L') {
                        int end = typeDescriptor.indexOf(';');
                        typeStr = typeDescriptor.substring(0, end + 1);
                        typeDescriptor = typeDescriptor.substring(end + 1);
                    } else {
                        typeStr = typeDescriptor.substring(0, 1);
                        typeDescriptor = typeDescriptor.substring(1);
                    }

                    types[i] = TypeDesc.forDescriptor(typeStr);
                }
            } catch (IndexOutOfBoundsException e) {
            }
        }

        for (int i=0; i<count; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            if (types != null) {
                if (nullable[i]) {
                    buf.append("@Nullable ");
                }
                buf.append(types[i].getFullName());
                buf.append(' ');
            } else if (showDesiredType) {
                if (index.getProperty(i).isNullable()) {
                    buf.append("@Nullable ");
                }
                buf.append(TypeDesc.forClass(index.getProperty(i).getType()).getFullName());
                buf.append(' ');
            }
            buf.append(index.getPropertyDirection(i).toCharacter());
            buf.append(index.getProperty(i).getName());
        }

        buf.append(']');
    }

    // Note: BDBStorage could just implement the RawSupport interface, but
    // then these hidden methods would be public. A simple cast of Storage to
    // RawSupport would expose them.
    private class Support implements RawSupport<S> {
        private final BDBRepository<Txn> mRepository;
        private final BDBStorage<Txn, S> mStorage;
        private Map<String, ? extends StorableProperty<S>> mProperties;

        Support(BDBRepository<Txn> repo, BDBStorage<Txn, S> storage) {
            mRepository = repo;
            mStorage = storage;
        }

        public Repository getRootRepository() {
            return mRepository.getRootRepository();
        }

        public boolean isPropertySupported(String name) {
            if (name == null) {
                return false;
            }
            if (mProperties == null) {
                mProperties = StorableIntrospector
                    .examine(mStorage.getStorableType()).getAllProperties();
            }
            return mProperties.containsKey(name);
        }

        public byte[] tryLoad(S storable, byte[] key) throws FetchException {
            TransactionScope<Txn> scope = mStorage.localTransactionScope();
            byte[] result;
            // Lock out shutdown task.
            scope.getLock().lock();
            try {
                try {
                    result = mStorage.db_get(scope.getTxn(), key, scope.isForUpdate());
                } catch (Throwable e) {
                    throw mStorage.toFetchException(e);
                }
            } finally {
                scope.getLock().unlock();
            }
            if (result == NOT_FOUND) {
                return null;
            }
            if (result == null) {
                result = SUCCESS;
            }
            return result;
        }

        public boolean tryInsert(S storable, byte[] key, byte[] value) throws PersistException {
            TransactionScope<Txn> scope = mStorage.localTransactionScope();
            Object result;
            // Lock out shutdown task.
            scope.getLock().lock();
            try {
                try {
                    result = mStorage.db_putNoOverwrite(scope.getTxn(), key, value);
                } catch (Throwable e) {
                    throw mStorage.toPersistException(e);
                }
            } finally {
                scope.getLock().unlock();
            }
            if (result == KEY_EXIST) {
                return false;
            }
            if (result != SUCCESS) {
                throw new PersistException("Failed");
            }
            return true;
        }

        public void store(S storable, byte[] key, byte[] value) throws PersistException {
            TransactionScope<Txn> scope = mStorage.localTransactionScope();
            // Lock out shutdown task.
            scope.getLock().lock();
            try {
                try {
                    if (!mStorage.db_put(scope.getTxn(), key, value)) {
                        throw new PersistException("Failed");
                    }
                } catch (Throwable e) {
                    throw mStorage.toPersistException(e);
                }
            } finally {
                scope.getLock().unlock();
            }
        }

        public boolean tryDelete(S storable, byte[] key) throws PersistException {
            TransactionScope<Txn> scope = mStorage.localTransactionScope();
            // Lock out shutdown task.
            scope.getLock().lock();
            try {
                try {
                    return mStorage.db_delete(scope.getTxn(), key);
                } catch (Throwable e) {
                    throw mStorage.toPersistException(e);
                }
            } finally {
                scope.getLock().unlock();
            }
        }

        public Blob getBlob(S storable, String name, long locator) throws FetchException {
            return mStorage.getBlob(storable, name, locator);
        }

        public long getLocator(Blob blob) throws PersistException {
            return mStorage.getLocator(blob);
        }

        public Clob getClob(S storable, String name, long locator) throws FetchException {
            return mStorage.getClob(storable, name, locator);
        }

        public long getLocator(Clob clob) throws PersistException {
            return mStorage.getLocator(clob);
        }

        public void decode(S dest, int generation, byte[] data) throws CorruptEncodingException {
            mStorableCodec.decode(dest, generation, data);
        }

        public SequenceValueProducer getSequenceValueProducer(String name)
            throws PersistException
        {
            try {
                return mStorage.mRepository.getSequenceValueProducer(name);
            } catch (RepositoryException e) {
                throw e.toPersistException();
            }
        }

        public Trigger<? super S> getInsertTrigger() {
            return mStorage.mTriggerManager.getInsertTrigger();
        }

        public Trigger<? super S> getUpdateTrigger() {
            return mStorage.mTriggerManager.getUpdateTrigger();
        }

        public Trigger<? super S> getDeleteTrigger() {
            return mStorage.mTriggerManager.getDeleteTrigger();
        }

        public Trigger<? super S> getLoadTrigger() {
            return mStorage.mTriggerManager.getLoadTrigger();
        }

        public void locallyDisableLoadTrigger() {
            mStorage.mTriggerManager.locallyDisableLoad();
        }

        public void locallyEnableLoadTrigger() {
            mStorage.mTriggerManager.locallyEnableLoad();
        }
    }
}
