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

package com.amazon.carbonado.repo.indexed;

import java.lang.reflect.UndeclaredThrowableException;

import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
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
import com.amazon.carbonado.info.StorableIndex;

import com.amazon.carbonado.cursor.MergeSortBuffer;

import com.amazon.carbonado.spi.RepairExecutor;

import com.amazon.carbonado.qe.BoundaryType;

import com.amazon.carbonado.util.Throttle;

/**
 * Encapsulates info and operations for a single index.
 *
 * @author Brian S O'Neill
 */
class ManagedIndex<S extends Storable> implements IndexEntryAccessor<S> {
    private static final int POPULATE_SORT_BUFFER_SIZE = 65536;
    private static final int POPULATE_INFO_DELAY_MILLIS = 5000;
    static final int POPULATE_BATCH_SIZE = 128;
    static final int POPULATE_THROTTLE_WINDOW = POPULATE_BATCH_SIZE * 10;
    static final int POPULATE_THROTTLE_SLEEP_PRECISION = 10;

    private final StorableIndex mIndex;
    private final IndexEntryGenerator<S> mGenerator;
    private final Storage<?> mIndexEntryStorage;

    private Query<?> mSingleMatchQuery;

    ManagedIndex(StorableIndex<S> index,
                 IndexEntryGenerator<S> generator,
                 Storage<?> indexEntryStorage)
        throws SupportException
    {
        mIndex = index;
        mGenerator = generator;
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
    public void copyToMasterPrimaryKey(Storable indexEntry, S master) {
        mGenerator.copyToMasterPrimaryKey(indexEntry, master);
    }

    // Required by IndexEntryAccessor interface.
    public void copyFromMaster(Storable indexEntry, S master) {
        mGenerator.copyFromMaster(indexEntry, master);
    }

    // Required by IndexEntryAccessor interface.
    public boolean isConsistent(Storable indexEntry, S master) {
        return mGenerator.isConsistent(indexEntry, master);
    }

    // Required by IndexEntryAccessor interface.
    public Comparator<? extends Storable> getComparator() {
        return mGenerator.getComparator();
    }

    Cursor<S> fetchOne(IndexedStorage storage, Object[] identityValues)
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

        return fetchFromIndexEntryQuery(storage, query.withValues(identityValues));
    }

    Cursor<S> fetchFromIndexEntryQuery(IndexedStorage storage, Query<?> indexEntryQuery)
        throws FetchException
    {
        return new IndexedCursor<S>(indexEntryQuery.fetch(), storage, mGenerator);
    }

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
        return makeIndexEntry(userStorable).tryDelete();
    }

    /** Assumes caller is in a transaction */
    boolean insertIndexEntry(S userStorable) throws PersistException {
        return insertIndexEntry(userStorable, makeIndexEntry(userStorable));
    }

    /** Assumes caller is in a transaction */
    boolean updateIndexEntry(S userStorable, S oldUserStorable) throws PersistException {
        Storable newIndexEntry = makeIndexEntry(userStorable);

        if (oldUserStorable != null) {
            Storable oldIndexEntry = makeIndexEntry(oldUserStorable);
            if (oldIndexEntry.equalPrimaryKeys(newIndexEntry)) {
                // Index entry didn't change, so nothing to do. If the
                // index entry has a version, it will lag behind the
                // master's version until the index entry changes, at which
                // point the version will again match the master.
                return true;
            }

            oldIndexEntry.tryDelete();
        }

        return insertIndexEntry(userStorable, newIndexEntry);
    }

    /**
     * Populates the entire index, repairing as it goes.
     *
     * @param repo used to enter transactions
     */
    void populateIndex(Repository repo, Storage<S> masterStorage, double desiredSpeed)
        throws RepositoryException
    {
        MergeSortBuffer buffer;
        Comparator c;

        Log log = LogFactory.getLog(IndexedStorage.class);

        // Enter top transaction with isolation level of none to make sure
        // preload operation does not run in a long nested transaction.
        Transaction txn = repo.enterTopTransaction(IsolationLevel.NONE);
        try {
            Cursor<S> cursor = masterStorage.query().fetch();
            try {
                if (!cursor.hasNext()) {
                    // Nothing exists in master, so nothing to populate.
                    return;
                }

                if (log.isInfoEnabled()) {
                    StringBuilder b = new StringBuilder();
                    b.append("Populating index on ");
                    b.append(masterStorage.getStorableType().getName());
                    b.append(": ");
                    try {
                        mIndex.appendTo(b);
                    } catch (java.io.IOException e) {
                        // Not gonna happen.
                    }
                    log.info(b.toString());
                }

                // Preload and sort all index entries for improved performance.

                buffer = new MergeSortBuffer(mIndexEntryStorage, null, POPULATE_SORT_BUFFER_SIZE);
                c = mGenerator.getComparator();
                buffer.prepare(c);

                long nextReportTime = System.currentTimeMillis() + POPULATE_INFO_DELAY_MILLIS;
                while (cursor.hasNext()) {
                    buffer.add(makeIndexEntry(cursor.next()));

                    if (log.isInfoEnabled()) {
                        long now = System.currentTimeMillis();
                        if (now >= nextReportTime) {
                            log.info("Prepared " + buffer.size() + " new index entries");
                            nextReportTime = now + POPULATE_INFO_DELAY_MILLIS;
                        }
                    }
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
                log.info("Verifying unique index");
            }

            Object last = null;
            for (Object obj : buffer) {
                if (last != null) {
                    if (c.compare(last, obj) == 0) {
                        buffer.close();
                        throw new UniqueConstraintException
                            ("Cannot build unique index because duplicates exist: "
                             + this);
                    }
                }
                last = obj;
            }
        }

        final int bufferSize = buffer.size();

        if (log.isInfoEnabled()) {
            log.info("Begin insert of " + bufferSize + " new index entries");
        }

        Throttle throttle = desiredSpeed < 1.0 ? new Throttle(POPULATE_THROTTLE_WINDOW) : null;

        int totalInserted = 0;

        txn = repo.enterTopTransaction(IsolationLevel.READ_COMMITTED);
        try {
            for (Object obj : buffer) {
                Storable indexEntry = (Storable) obj;
                if (!indexEntry.tryInsert()) {
                    // Couldn't insert because an index entry already exists.
                    Storable existing = indexEntry.copy();
                    if (existing.tryLoad()) {
                        if (!existing.equalProperties(indexEntry)) {
                            // Existing entry differs, so update it.
                            indexEntry.copyUnequalProperties(existing);
                            existing.tryUpdate();
                            indexEntry = existing;
                        }
                    } else {
                        // Couldn't find existing entry for some reason, so
                        // repair by brute force.
                        indexEntry.tryDelete();
                        indexEntry.tryInsert();
                    }
                }

                totalInserted++;
                if (totalInserted % POPULATE_BATCH_SIZE == 0) {
                    txn.commit();
                    txn.exit();

                    if (log.isInfoEnabled()) {
                        String format = "Committed %d new index entries (%.3f%%)";
                        double percent = 100.0 * totalInserted / bufferSize;
                        log.info(String.format(format, totalInserted, percent));
                    }

                    txn = repo.enterTopTransaction(IsolationLevel.READ_COMMITTED);
                }

                if (throttle != null) {
                    try {
                        throttle.throttle(desiredSpeed, POPULATE_THROTTLE_SLEEP_PRECISION);
                    } catch (InterruptedException e) {
                        throw new RepositoryException("Index populate interrupted");
                    }
                }
            }

            txn.commit();
        } finally {
            txn.exit();
            buffer.close();
        }

        if (log.isInfoEnabled()) {
            log.info("Finished inserting " + totalInserted + " new index entries");
        }
    }

    private Storable makeIndexEntry(S userStorable) throws PersistException {
        try {
            Storable indexEntry = mIndexEntryStorage.prepare();
            mGenerator.copyFromMaster(indexEntry, userStorable);
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
        {
            Storable freshEntry = mIndexEntryStorage.prepare();
            mGenerator.copyFromMaster(freshEntry, userStorable);
            indexEntry.copyVersionProperty(freshEntry);
            if (freshEntry.equals(indexEntry)) {
                // Existing entry is exactly what we expect. Return false
                // exception if alternate key constraint, since this is
                // user error.
                return !isUnique();
            }
        }

        // Run the repair outside a transaction.

        RepairExecutor.execute(new Runnable() {
            public void run() {
                try {
                    // Blow it away entry and re-insert. Don't simply update
                    // the entry, since record version number may prevent
                    // update.

                    // Since we may be running outside transaction now, user
                    // storable may have changed. Reload to get latest data.

                    S freshUserStorable = (S) userStorable.copy();
                    if (!freshUserStorable.tryLoad()) {
                        // Gone now, nothing we can do. Assume index entry
                        // was properly deleted.
                        return;
                    }

                    Storable freshEntry = mIndexEntryStorage.prepare();
                    mGenerator.copyFromMaster(freshEntry, freshUserStorable);

                    // Blow it away entry and re-insert. Don't simply update
                    // the entry, since record version number may prevent
                    // update.
                    freshEntry.tryDelete();
                    freshEntry.tryInsert();
                } catch (FetchException fe) {
                    LogFactory.getLog(IndexedStorage.class).warn
                        ("Unable to check if repair is required: " +
                         userStorable.toStringKeyOnly(), fe);
                } catch (PersistException pe) {
                    LogFactory.getLog(IndexedStorage.class).error
                        ("Unable to repair index entry for " +
                         userStorable.toStringKeyOnly(), pe);
                }
            }
        });

        return true;
    }
}
