/*
 * Copyright 2006-2012 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.indexed;

import java.lang.reflect.UndeclaredThrowableException;

import java.util.Comparator;
import java.util.Iterator;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchTimeoutException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistTimeoutException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableKey;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.cursor.MergeSortBuffer;

import com.amazon.carbonado.spi.RepairExecutor;

import com.amazon.carbonado.synthetic.SyntheticStorableReferenceAccess;

import com.amazon.carbonado.util.Throttle;

/**
 * Encapsulates info and operations for a single index.
 *
 * @author Brian S O'Neill
 */
class ManagedIndex<S extends Storable> implements IndexEntryAccessor<S> {
    private static final int BUILD_SORT_BUFFER_SIZE = 65536;
    private static final int BUILD_INFO_DELAY_MILLIS = 5000;
    static final int BUILD_BATCH_SIZE = 1000;
    static final int BUILD_THROTTLE_WINDOW = BUILD_BATCH_SIZE * 10;
    static final int BUILD_THROTTLE_SLEEP_PRECISION = 10;

    private static final int BUILD_TXN_TIMEOUT_MILLIS;

    static {
        int timeout = 100;
        String prop = System.getProperty
            ("com.amazon.carbonado.repo.indexed.BUILD_TXN_TIMEOUT_MILLIS");
        if (prop != null) {
            timeout = Integer.parseInt(prop);
        }
        BUILD_TXN_TIMEOUT_MILLIS = timeout;
    }

    private static String[] naturalOrdering(Class<? extends Storable> type) {
        StorableKey<?> pk = StorableIntrospector.examine(type).getPrimaryKey();
        String[] naturalOrdering = new String[pk.getProperties().size()];
        int i=0;
        for (OrderedProperty<?> prop : pk.getProperties()) {
            String orderBy;
            if (prop.getDirection() == Direction.DESCENDING) {
                orderBy = prop.toString();
            } else {
                orderBy = prop.getChainedProperty().toString();
            }
            naturalOrdering[i++] = orderBy;
        }
        return naturalOrdering;
    }

    private final IndexedRepository mRepository;
    private final Storage<S> mMasterStorage;
    private final StorableIndex mIndex;
    private final SyntheticStorableReferenceAccess<S> mAccessor;
    private final Storage<?> mIndexEntryStorage;

    private Query<?> mSingleMatchQuery;

    ManagedIndex(IndexedRepository repository,
                 Storage<S> masterStorage,
                 StorableIndex<S> index,
                 SyntheticStorableReferenceAccess<S> accessor,
                 Storage<?> indexEntryStorage)
        throws SupportException
    {
        mRepository = repository;
        mMasterStorage = masterStorage;
        mIndex = index;
        mAccessor = accessor;
        mIndexEntryStorage = indexEntryStorage;
    }

    public String getName() {
        return mIndex.getNameDescriptor();
    }

    public String[] getPropertyNames() {
        int i = mIndex.getPropertyCount();
        String[] names = new String[i];
        while (--i >= 0) {
            names[i] = mIndex.getProperty(i).getName();
        }
        return names;
    }

    public Direction[] getPropertyDirections() {
        int i = mIndex.getPropertyCount();
        Direction[] directions = new Direction[i];
        while (--i >= 0) {
            directions[i] = mIndex.getPropertyDirection(i);
        }
        return directions;
    }

    public boolean isUnique() {
        return mIndex.isUnique();
    }

    public boolean isClustered() {
        return false;
    }

    public StorableIndex getIndex() {
        return mIndex;
    }

    // Required by IndexEntryAccessor interface.
    public Storage<?> getIndexEntryStorage() {
        return mIndexEntryStorage;
    }

    // Required by IndexEntryAccessor interface.
    public void copyToMasterPrimaryKey(Storable indexEntry, S master) throws FetchException {
        mAccessor.copyToMasterPrimaryKey(indexEntry, master);
    }

    // Required by IndexEntryAccessor interface.
    public void copyFromMaster(Storable indexEntry, S master) throws FetchException {
        mAccessor.copyFromMaster(indexEntry, master);
    }

    // Required by IndexEntryAccessor interface.
    public boolean isConsistent(Storable indexEntry, S master) throws FetchException {
        return mAccessor.isConsistent(indexEntry, master);
    }

    // Required by IndexEntryAccessor interface.
    public void repair(double desiredSpeed) throws RepositoryException {
        buildIndex(desiredSpeed);
    }

    // Required by IndexEntryAccessor interface.
    public Comparator<? extends Storable> getComparator() {
        return mAccessor.getComparator();
    }

    Cursor<S> fetchOne(IndexedStorage storage, Object[] identityValues)
        throws FetchException
    {
        return fetchOne(storage, identityValues, null);
    }

    Cursor<S> fetchOne(IndexedStorage storage, Object[] identityValues,
                       Query.Controller controller)
        throws FetchException
    {
        Query<?> query = mSingleMatchQuery;

        if (query == null) {
            StorableIndex index = mIndex;
            Filter filter = Filter.getOpenFilter(mIndexEntryStorage.getStorableType());
            for (int i=0; i<index.getPropertyCount(); i++) {
                filter = filter.and(index.getProperty(i).getName(), RelOp.EQ);
            }
            mSingleMatchQuery = query = mIndexEntryStorage.query(filter);
        }

        return fetchFromIndexEntryQuery(storage, query.withValues(identityValues), controller);
    }

    Cursor<S> fetchFromIndexEntryQuery(IndexedStorage storage, Query<?> indexEntryQuery)
        throws FetchException
    {
        return fetchFromIndexEntryQuery(storage, indexEntryQuery, null);
    }

    Cursor<S> fetchFromIndexEntryQuery(IndexedStorage storage, Query<?> indexEntryQuery,
                                       Query.Controller controller)
        throws FetchException
    {
        return new IndexedCursor<S>(indexEntryQuery.fetch(controller), storage, mAccessor);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("IndexInfo ");
        try {
            mIndex.appendTo(b);
        } catch (java.io.IOException e) {
            // Not gonna happen.
        }
        return b.toString();
    }

    /** Assumes caller is in a transaction */
    boolean deleteIndexEntry(S userStorable) throws PersistException {
        try {
            return makeIndexEntry(userStorable).tryDelete();
        } catch (PersistException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IllegalArgumentException) {
                // Can be caused by a corrupt master record, which is
                // attempting do assign an illegal value to the index. There's
                // no way to find the old index entry to delete.
                return false;
            }
            throw e;
        }
    }

    /** Assumes caller is in a transaction */
    boolean insertIndexEntry(S userStorable) throws PersistException {
        return insertIndexEntry(userStorable, makeIndexEntry(userStorable));
    }

    /** Assumes caller is in a transaction */
    boolean updateIndexEntry(S userStorable, S oldUserStorable) throws PersistException {
        Storable newIndexEntry = makeIndexEntry(userStorable);

        if (oldUserStorable != null) deleteOldEntry: {
            Storable oldIndexEntry;
            try {
                oldIndexEntry = makeIndexEntry(oldUserStorable);
            } catch (PersistException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IllegalArgumentException) {
                    // Can be caused by a corrupt master record, which is
                    // attempting do assign an illegal value to the index. There's
                    // no way to find the old index entry to delete.
                    break deleteOldEntry;
                }
                throw e;
            }

            if (oldIndexEntry.equalPrimaryKeys(newIndexEntry)) {
                // Index entry didn't change, so nothing to do. If the index
                // entry has a version, it will lag behind the master's version
                // until the index entry changes, at which point the version
                // will again match the master.
                return true;
            }

            oldIndexEntry.tryDelete();
        }

        return insertIndexEntry(userStorable, newIndexEntry);
    }

    /**
     * Build the entire index, repairing as it goes.
     *
     * @param repo used to enter transactions
     */
    void buildIndex(double desiredSpeed) throws RepositoryException {
        final MergeSortBuffer buffer;
        final Comparator c;

        final Log log = LogFactory.getLog(IndexedStorage.class);

        final Query<S> masterQuery;
        {
            // Need to explicitly order master query by primary key in order
            // for fetchAfter to work correctly in case corrupt records are
            // encountered.
            masterQuery = mMasterStorage.query()
                .orderBy(naturalOrdering(mMasterStorage.getStorableType()));
        }

        // Quick check to see if any records exist in master.
        {
            Transaction txn = mRepository.enterTopTransaction(IsolationLevel.READ_COMMITTED);
            try {
                if (!masterQuery.exists()) {
                    if (mIndexEntryStorage.query().exists()) {
                        txn.exit();
                        mIndexEntryStorage.truncate();
                    }
                    return;
                }
            } finally {
                txn.exit();
            }
        }

        // Enter top transaction with isolation level of none to make sure
        // preload operation does not run in a long nested transaction.
        Transaction txn = mRepository.enterTopTransaction(IsolationLevel.NONE);
        try {
            Cursor<S> cursor = masterQuery.fetch();
            try {
                if (log.isInfoEnabled()) {
                    StringBuilder b = new StringBuilder();
                    b.append("Preparing index on ");
                    b.append(mMasterStorage.getStorableType().getName());
                    b.append(": ");
                    try {
                        mIndex.appendTo(b);
                    } catch (java.io.IOException e) {
                        // Not gonna happen.
                    }
                    log.info(b.toString());
                }

                // Preload and sort all index entries for improved performance.

                buffer = new MergeSortBuffer(mIndexEntryStorage, null, BUILD_SORT_BUFFER_SIZE);
                c = getComparator();
                buffer.prepare(c);

                long nextReportTime = System.currentTimeMillis() + BUILD_INFO_DELAY_MILLIS;

                // These variables are used when corrupt records are encountered.
                S lastUserStorable = null;
                int skippedCount = 0;

                while (cursor.hasNext()) {
                    S userStorable;
                    try {
                        userStorable = cursor.next();
                        skippedCount = 0;
                    } catch (CorruptEncodingException e) {
                        log.warn("Omitting corrupt record from index: " + e.toString());

                        // Exception forces cursor to close. Close again to be sure.
                        cursor.close();

                        if (lastUserStorable == null) {
                            cursor = masterQuery.fetch();
                        } else {
                            cursor = masterQuery.fetchAfter(lastUserStorable);
                        }

                        cursor.skipNext(++skippedCount);
                        continue;
                    }

                    buffer.add(makeIndexEntry(userStorable));

                    if (log.isInfoEnabled()) {
                        long now = System.currentTimeMillis();
                        if (now >= nextReportTime) {
                            log.info("Prepared " + buffer.size() + " index entries");
                            nextReportTime = now + BUILD_INFO_DELAY_MILLIS;
                        }
                    }

                    lastUserStorable = userStorable;
                }

                // No need to commit transaction because no changes should have been made.
            } finally {
                cursor.close();
            }
        } finally {
            txn.exit();
        }

        // This is not expected to take long, since MergeSortBuffer sorts as
        // needed. This just finishes off what was not written to a file.
        buffer.sort();

        if (isUnique()) {
            // If index is unique, scan buffer and check for duplicates
            // _before_ inserting index entries. If there are duplicates,
            // fail, since unique index cannot be built.

            if (log.isInfoEnabled()) {
                log.info("Verifying index");
            }

            Object last = null;
            for (Object obj : buffer) {
                if (last != null) {
                    if (c.compare(last, obj) == 0) {
                        buffer.close();
                        throw new UniqueConstraintException
                            ("Cannot build unique index because duplicates exist: "
                             + this + ", " + last + " == " + obj);
                    }
                }
                last = obj;
            }
        }

        final int bufferSize = buffer.size();

        if (log.isInfoEnabled()) {
            log.info("Begin build of " + bufferSize + " index entries");
        }

        // Need this index entry query for deleting bogus entries.
        final Query indexEntryQuery = mIndexEntryStorage.query()
            .orderBy(naturalOrdering(mIndexEntryStorage.getStorableType()));

        Throttle throttle = desiredSpeed < 1.0 ? new Throttle(BUILD_THROTTLE_WINDOW) : null;

        long totalInserted = 0;
        long totalUpdated = 0;
        long totalDeleted = 0;
        long totalProgress = 0;

        txn = enterBuildTxn();
        try {
            Cursor<? extends Storable> indexEntryCursor = indexEntryQuery.fetch();
            Storable existingIndexEntry = null;

            if (!indexEntryCursor.hasNext()) {
                indexEntryCursor.close();
                // Don't try opening again.
                indexEntryCursor = null;
            }

            boolean retry = false;
            Storable indexEntry = null;
            Storable lastIndexEntry = null;

            long nextReportTime = System.currentTimeMillis() + BUILD_INFO_DELAY_MILLIS;

            Iterator it = buffer.iterator();
            bufferIterate: while (true) {
                if (!retry) {
                    Object obj;
                    if (it.hasNext()) {
                        obj = it.next();
                    } else if (indexEntryCursor != null && indexEntryCursor.hasNext()) {
                        obj = null;
                    } else {
                        break;
                    }

                    indexEntry = (Storable) obj;
                }

                try {
                    if (indexEntry != null) {
                        if (indexEntry.tryInsert()) {
                            totalInserted++;
                        } else {
                            // Couldn't insert because an index entry already exists.
                            Storable existing = indexEntry.copy();
                            boolean doUpdate = false;
                            if (!existing.tryLoad()) {
                                doUpdate = true;
                            } else if (!existing.equalProperties(indexEntry)) {
                                // If only the version differs, leave existing entry alone.
                                indexEntry.copyVersionProperty(existing);
                                doUpdate = !existing.equalProperties(indexEntry);
                            }
                            if (doUpdate) {
                                indexEntry.tryDelete();
                                indexEntry.tryInsert();
                                totalUpdated++;
                            }
                        }
                    }

                    if (indexEntryCursor != null) while (true) {
                        if (existingIndexEntry == null) {
                            if (indexEntryCursor.hasNext()) {
                                existingIndexEntry = indexEntryCursor.next();
                            } else {
                                indexEntryCursor.close();
                                // Don't try opening again.
                                indexEntryCursor = null;
                                break;
                            }
                        }

                        int compare = c.compare(existingIndexEntry, indexEntry);

                        if (compare == 0) {
                            // Existing entry cursor matches so allow cursor to advance.
                            existingIndexEntry = null;
                            break;
                        } else if (compare > 0) {
                            // Existing index entry is ahead so check later.
                            break;
                        } else {
                            // Existing index entry might be bogus. Check again
                            // in case master record changed.
                            doDelete: {
                                S master = mMasterStorage.prepare();
                                copyToMasterPrimaryKey(existingIndexEntry, master);
                                if (master.tryLoad()) {
                                    Storable temp = makeIndexEntry(master);
                                    existingIndexEntry.copyVersionProperty(temp);
                                    if (existingIndexEntry.equalProperties(temp)) {
                                        break doDelete;
                                    }
                                }

                                existingIndexEntry.tryDelete();
                                totalDeleted++;

                                if (totalDeleted % BUILD_BATCH_SIZE == 0) {
                                    txn.commit();
                                    txn.exit();

                                    nextReportTime = logProgress
                                        (nextReportTime, log, totalProgress, bufferSize,
                                         totalInserted, totalUpdated, totalDeleted);

                                    txn = enterBuildTxn();

                                    indexEntryCursor.close();
                                    indexEntryCursor = indexEntryQuery
                                        .fetchAfter(existingIndexEntry);

                                    if (!indexEntryCursor.hasNext()) {
                                        indexEntryCursor.close();
                                        // Don't try opening again.
                                        indexEntryCursor = null;
                                        break;
                                    }
                                }
                            }

                            existingIndexEntry = null;

                            throttle(throttle, desiredSpeed);
                        }
                    }

                    if (indexEntry != null) {
                        totalProgress++;
                    }

                    lastIndexEntry = indexEntry;
                    retry = false;
                } catch (RepositoryException e) {
                    if (e instanceof FetchTimeoutException ||
                        e instanceof PersistTimeoutException)
                    {
                        log.warn("Lock conflict during index repair; will retry: " +
                                 indexEntry + ", " + e);
                        // This re-uses the last index entry to repair and forces
                        // the current transaction to commit.
                        retry = true;
                    } else {
                        throw e;
                    }
                }

                if (retry || (totalProgress % BUILD_BATCH_SIZE == 0)) {
                    txn.commit();
                    txn.exit();

                    nextReportTime = logProgress(nextReportTime, log, totalProgress, bufferSize,
                                                 totalInserted, totalUpdated, totalDeleted);

                    txn = enterBuildTxn();

                    if (indexEntryCursor != null) {
                        indexEntryCursor.close();
                        existingIndexEntry = null;

                        if (indexEntry == null || lastIndexEntry == null) {
                            indexEntryCursor = indexEntryQuery.fetch();
                        } else if (!retry) {
                            indexEntryCursor = indexEntryQuery.fetchAfter(indexEntry);
                        } else {
                            // Re-fetch starting at the same spot.
                            indexEntryCursor = indexEntryQuery.fetchAfter(lastIndexEntry);
                        }
                    }
                }

                throttle(throttle, desiredSpeed);
            }

            txn.commit();
        } finally {
            txn.exit();
            buffer.close();
        }

        if (log.isInfoEnabled()) {
            log.info("Finished building " + totalProgress + " index entries " +
                     progressSubMessgage(totalInserted, totalUpdated, totalDeleted));
        }
    }

    private Transaction enterBuildTxn() {
        Transaction txn = mRepository.enterTopTransaction(IsolationLevel.READ_COMMITTED);
        txn.setForUpdate(true);
        txn.setDesiredLockTimeout(BUILD_TXN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        return txn;
    }

    private static void throttle(Throttle throttle, double desiredSpeed)
        throws RepositoryException
    {
        if (throttle != null) {
            try {
                throttle.throttle(desiredSpeed, BUILD_THROTTLE_SLEEP_PRECISION);
            } catch (InterruptedException e) {
                throw new RepositoryException("Index build interrupted");
            }
        }
    }

    private long logProgress(long nextReportTime, Log log,
                             long totalProgress, int bufferSize,
                             long totalInserted, long totalUpdated, long totalDeleted)
    {
        long now = System.currentTimeMillis();
        if (now >= nextReportTime) {
            if (log.isInfoEnabled()) {
                String format = "Index build progress: %.3f%% " +
                    progressSubMessgage(totalInserted, totalUpdated, totalDeleted);
                double percent = 100.0 * totalProgress / bufferSize;
                log.info(String.format(format, percent));
            }
            nextReportTime = now + BUILD_INFO_DELAY_MILLIS;
        }
        return nextReportTime;
    }

    private String progressSubMessgage(long totalInserted, long totalUpdated, long totalDeleted) {
        StringBuilder b = new StringBuilder();
        b.append('(');

        if (totalInserted > 0) {
            b.append(totalInserted);
            b.append(" inserted");
        }
        if (totalUpdated > 0) {
            if (b.length() > 1) {
                b.append(", ");
            }
            b.append(totalUpdated);
            b.append(" updated");
        }
        if (totalDeleted > 0) {
            if (b.length() > 1) {
                b.append(", ");
            }
            b.append(totalDeleted);
            b.append(" deleted");
        }

        if (b.length() == 1) {
            b.append("no changes made");
        }

        b.append(')');

        return b.toString();
    }

    private Storable makeIndexEntry(S userStorable) throws PersistException {
        try {
            Storable indexEntry = mIndexEntryStorage.prepare();
            mAccessor.copyFromMaster(indexEntry, userStorable);
            return indexEntry;
        } catch (UndeclaredThrowableException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PersistException) {
                throw (PersistException) cause;
            }
            throw new PersistException(cause);
        } catch (Exception e) {
            if (e instanceof PersistException) {
                throw (PersistException) e;
            }
            throw new PersistException(e);
        }
    }

    /** Assumes caller is in a transaction */
    private boolean insertIndexEntry(final S userStorable, final Storable indexEntry)
        throws PersistException
    {
        if (indexEntry.tryInsert()) {
            return true;
        }

        // If index entry already exists, then index might be corrupt.
        try {
            Storable freshEntry = mIndexEntryStorage.prepare();
            mAccessor.copyFromMaster(freshEntry, userStorable);
            freshEntry.load();
            indexEntry.copyVersionProperty(freshEntry);
            if (freshEntry.equals(indexEntry)) {
                // Existing entry is fine.
                return true;
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }

        return false;
    }
}
