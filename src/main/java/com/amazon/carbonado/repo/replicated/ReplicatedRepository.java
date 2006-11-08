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
package com.amazon.carbonado.repo.replicated;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.cojen.util.BeanComparator;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.UnsupportedTypeException;

import com.amazon.carbonado.capability.Capability;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.capability.ResyncCapability;
import com.amazon.carbonado.capability.ShutdownCapability;
import com.amazon.carbonado.capability.StorableInfoCapability;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.spi.StorageCollection;
import com.amazon.carbonado.spi.TransactionPair;

import com.amazon.carbonado.util.Throttle;

/**
 * A ReplicatedRepository binds two repositories together.  One will be used
 * for reading (the replica), and the other will be used for writing; changes
 * to the master repository will be copied to the replica.
 *
 * @author Don Schneider
 */
class ReplicatedRepository
    implements Repository,
               ResyncCapability,
               ShutdownCapability,
               StorableInfoCapability
{
    // Maximum number of resync updates to replica per transaction.
    private static final int RESYNC_BATCH_SIZE = 10;

    // Commit resync replica transaction early if this many records
    // scanned. Otherwise, write locks may be held for a very long time.
    private static final int RESYNC_WATERMARK = 100;

    /**
     * Utility method to select the natural ordering of a storage, by looking
     * for a clustered index on the primary key. Returns null if no clustered
     * index was found.
     *
     * TODO: Try to incorporate this into standard storage interface somehow.
     */
    private static String[] selectNaturalOrder(Repository repo, Class<? extends Storable> type)
        throws RepositoryException
    {
        IndexInfoCapability capability = repo.getCapability(IndexInfoCapability.class);
        if (capability == null) {
            return null;
        }
        IndexInfo info = null;
        for (IndexInfo candidate : capability.getIndexInfo(type)) {
            if (candidate.isClustered()) {
                info = candidate;
                break;
            }
        }
        if (info == null) {
            return null;
        }

        // Verify index is part of primary key.
        Set<String> pkSet = StorableIntrospector.examine(type).getPrimaryKeyProperties().keySet();

        String[] propNames = info.getPropertyNames();
        for (String prop : propNames) {
            if (!pkSet.contains(prop)) {
                return null;
            }
        }

        String[] orderBy = new String[pkSet.size()];

        Direction[] directions = info.getPropertyDirections();

        // Clone to remove elements.
        pkSet = new LinkedHashSet<String>(pkSet);

        int i;
        for (i=0; i<propNames.length; i++) {
            orderBy[i] = ((directions[i] == Direction.DESCENDING) ? "-" : "+") + propNames[i];
            pkSet.remove(propNames[i]);
        }

        // Append any remaining pk properties, to ensure complete ordering.
        if (pkSet.size() > 0) {
            for (String prop : pkSet) {
                orderBy[i++] = prop;
            }
        }

        return orderBy;
    }

    private String mName;
    private Repository mReplicaRepository;
    private Repository mMasterRepository;

    private final StorageCollection mStorages;

    ReplicatedRepository(String aName,
                         Repository aReplicaRepository,
                         Repository aMasterRepository) {
        mName = aName;
        mReplicaRepository = aReplicaRepository;
        mMasterRepository = aMasterRepository;

        mStorages = new StorageCollection() {
            protected <S extends Storable> Storage<S> createStorage(Class<S> type)
                throws SupportException, RepositoryException
            {
                Storage<S> replicaStorage = mReplicaRepository.storageFor(type);

                try {
                    return new ReplicatedStorage<S>(ReplicatedRepository.this, replicaStorage);
                } catch (UnsupportedTypeException e) {
                    // Okay, no master.
                    return replicaStorage;
                }
            }
        };
    }

    public String getName() {
        return mName;
    }

    Repository getReplicaRepository() {
        return mReplicaRepository;
    }

    Repository getMasterRepository() {
        return mMasterRepository;
    }

    public <S extends Storable> Storage<S> storageFor(Class<S> type)
        throws MalformedTypeException, SupportException, RepositoryException
    {
        return mStorages.storageFor(type);
    }

    public Transaction enterTransaction() {
        return new TransactionPair(mMasterRepository.enterTransaction(),
                                   mReplicaRepository.enterTransaction());
    }

    public Transaction enterTransaction(IsolationLevel level) {
        return new TransactionPair(mMasterRepository.enterTransaction(level),
                                   mReplicaRepository.enterTransaction(level));
    }

    public Transaction enterTopTransaction(IsolationLevel level) {
        return new TransactionPair(mMasterRepository.enterTopTransaction(level),
                                   mReplicaRepository.enterTopTransaction(level));
    }

    public IsolationLevel getTransactionIsolationLevel() {
        IsolationLevel replicaLevel = mReplicaRepository.getTransactionIsolationLevel();
        if (replicaLevel == null) {
            return null;
        }
        IsolationLevel masterLevel = mMasterRepository.getTransactionIsolationLevel();
        if (masterLevel == null) {
            return null;
        }
        return replicaLevel.lowestCommon(masterLevel);
    }

    @SuppressWarnings("unchecked")
    public <C extends Capability> C getCapability(Class<C> capabilityType) {
        if (capabilityType.isInstance(this)) {
            if (ShutdownCapability.class.isAssignableFrom(capabilityType)) {
                if (mReplicaRepository.getCapability(capabilityType) == null &&
                    mMasterRepository.getCapability(capabilityType) == null) {

                    return null;
                }
            }
            return (C) this;
        }

        C cap = mMasterRepository.getCapability(capabilityType);
        if (cap == null) {
            cap = mReplicaRepository.getCapability(capabilityType);
        }

        return cap;
    }

    public void close() {
        mReplicaRepository.close();
        mMasterRepository.close();
    }

    public String[] getUserStorableTypeNames() throws RepositoryException {
        StorableInfoCapability replicaCap =
            mReplicaRepository.getCapability(StorableInfoCapability.class);
        StorableInfoCapability masterCap =
            mMasterRepository.getCapability(StorableInfoCapability.class);

        if (replicaCap == null) {
            if (masterCap == null) {
                return new String[0];
            }
            return masterCap.getUserStorableTypeNames();
        } else if (masterCap == null) {
            return replicaCap.getUserStorableTypeNames();
        }

        // Merge the two sets together.
        Set<String> names = new LinkedHashSet<String>();
        for (String name : replicaCap.getUserStorableTypeNames()) {
            names.add(name);
        }
        for (String name : masterCap.getUserStorableTypeNames()) {
            names.add(name);
        }

        return names.toArray(new String[names.size()]);
    }

    public boolean isSupported(Class<Storable> type) {
        StorableInfoCapability replicaCap =
            mReplicaRepository.getCapability(StorableInfoCapability.class);
        StorableInfoCapability masterCap =
            mMasterRepository.getCapability(StorableInfoCapability.class);

        return (masterCap == null || masterCap.isSupported(type))
            && (replicaCap == null || replicaCap.isSupported(type));
    }

    public boolean isPropertySupported(Class<Storable> type, String name) {
        StorableInfoCapability replicaCap =
            mReplicaRepository.getCapability(StorableInfoCapability.class);
        StorableInfoCapability masterCap =
            mMasterRepository.getCapability(StorableInfoCapability.class);

        return (masterCap == null || masterCap.isPropertySupported(type, name))
            && (replicaCap == null || replicaCap.isPropertySupported(type, name));
    }

    public boolean isAutoShutdownEnabled() {
        ShutdownCapability cap = mReplicaRepository.getCapability(ShutdownCapability.class);
        if (cap != null && cap.isAutoShutdownEnabled()) {
            return true;
        }
        cap = mMasterRepository.getCapability(ShutdownCapability.class);
        if (cap != null && cap.isAutoShutdownEnabled()) {
            return true;
        }
        return false;
    }

    public void setAutoShutdownEnabled(boolean enabled) {
        ShutdownCapability cap = mReplicaRepository.getCapability(ShutdownCapability.class);
        if (cap != null) {
            cap.setAutoShutdownEnabled(enabled);
        }
        cap = mMasterRepository.getCapability(ShutdownCapability.class);
        if (cap != null) {
            cap.setAutoShutdownEnabled(enabled);
        }
    }

    public void shutdown() {
        ShutdownCapability cap = mReplicaRepository.getCapability(ShutdownCapability.class);
        if (cap != null) {
            cap.shutdown();
        } else {
            mReplicaRepository.close();
        }
        cap = mMasterRepository.getCapability(ShutdownCapability.class);
        if (cap != null) {
            cap.shutdown();
        } else {
            mMasterRepository.close();
        }
    }

    /**
     * Repairs replicated storables by synchronizing the replica repository
     * against the master repository.
     *
     * @param type type of storable to re-sync
     * @param desiredSpeed throttling parameter - 1.0 = full speed, 0.5 = half
     * speed, 0.1 = one-tenth speed, etc
     * @param filter optional query filter to limit which objects get re-sync'ed
     * @param filterValues filter values for optional filter
     */
    public <S extends Storable> void resync(Class<S> type,
                                            double desiredSpeed,
                                            String filter,
                                            Object... filterValues)
        throws RepositoryException
    {
        ReplicationTrigger<S> trigger;
        if (storageFor(type) instanceof ReplicatedStorage) {
            trigger = ((ReplicatedStorage) storageFor(type)).getTrigger();
        } else {
            throw new UnsupportedTypeException(type);
        }

        Storage<S> replicaStorage, masterStorage;
        replicaStorage = mReplicaRepository.storageFor(type);
        masterStorage = mMasterRepository.storageFor(type);

        Query<S> replicaQuery, masterQuery;
        if (filter == null) {
            replicaQuery = replicaStorage.query();
            masterQuery = masterStorage.query();
        } else {
            replicaQuery = replicaStorage.query(filter).withValues(filterValues);
            masterQuery = masterStorage.query(filter).withValues(filterValues);
        }

        // Order both queries the same so that they can be run in parallel.
        // Favor natural order of replica, since cursors may be opened and
        // re-opened on it.
        String[] orderBy = selectNaturalOrder(mReplicaRepository, type);
        if (orderBy == null) {
            orderBy = selectNaturalOrder(mMasterRepository, type);
            if (orderBy == null) {
                Set<String> pkSet =
                    StorableIntrospector.examine(type).getPrimaryKeyProperties().keySet();
                orderBy = pkSet.toArray(new String[0]);
            }
        }

        BeanComparator bc = BeanComparator.forClass(type);
        for (String order : orderBy) {
            bc = bc.orderBy(order);
            bc = bc.caseSensitive();
        }

        replicaQuery = replicaQuery.orderBy(orderBy);
        masterQuery = masterQuery.orderBy(orderBy);

        Throttle throttle;
        if (desiredSpeed >= 1.0) {
            throttle = null;
        } else {
            if (desiredSpeed < 0.0) {
                desiredSpeed = 0.0;
            }
            // 50 samples
            throttle = new Throttle(50);
        }

        Transaction replicaTxn = mReplicaRepository.enterTransaction();
        try {
            replicaTxn.setForUpdate(true);

            resync(trigger,
                   replicaStorage, replicaQuery,
                   masterStorage, masterQuery,
                   throttle, desiredSpeed,
                   bc, replicaTxn);

            replicaTxn.commit();
        } finally {
            replicaTxn.exit();
        }
    }

    @SuppressWarnings("unchecked")
    private <S extends Storable> void resync(ReplicationTrigger<S> trigger,
                                             Storage<S> replicaStorage, Query<S> replicaQuery,
                                             Storage<S> masterStorage, Query<S> masterQuery,
                                             Throttle throttle, double desiredSpeed,
                                             Comparator comparator, Transaction replicaTxn)
        throws RepositoryException
    {
        final Log log = LogFactory.getLog(ReplicatedRepository.class);

        Cursor<S> replicaCursor = null;
        Cursor<S> masterCursor = null;

        try {
            replicaCursor = replicaQuery.fetch();
            masterCursor = masterQuery.fetch();

            S lastReplicaEntry = null;
            S lastMasterEntry = null;
            S replicaEntry = null;
            S masterEntry = null;
            
            int count = 0, txnCount = 0;
            while (true) {
                if (throttle != null) {
                    try {
                        // 100 millisecond clock precision
                        throttle.throttle(desiredSpeed, 100);
                    } catch (InterruptedException e) {
                        throw new FetchInterruptedException(e);
                    }
                }

                if (replicaEntry == null && replicaCursor != null) {
                    long skippedCount = 0;
                    while (replicaCursor.hasNext()) {
                        try {
                            replicaEntry = replicaCursor.next();
                            if (skippedCount > 0) {
                                if (skippedCount == 1) {
                                    log.warn("Skipped corrupt replica entry before this one: " +
                                             replicaEntry);
                                } else {
                                    log.warn("Skipped " + skippedCount +
                                             " corrupt replica entries before this one: " +
                                             replicaEntry);
                                }
                            }
                            break;
                        } catch (CorruptEncodingException e) {
                            // Exception forces cursor to close. Close again to be sure.
                            replicaCursor.close();
                            replicaCursor = null;

                            skipCorruption: {
                                Storable withKey = e.getStorableWithPrimaryKey();

                                if (withKey != null &&
                                    withKey.storableType() == replicaStorage.getStorableType())
                                {
                                    // Delete corrupt replica entry.
                                    try {
                                        trigger.deleteReplica(withKey);
                                        log.info("Deleted corrupt replica entry: " +
                                                 withKey.toStringKeyOnly(), e);
                                        break skipCorruption;
                                    } catch (PersistException e2) {
                                        log.warn("Unable to delete corrupt replica entry: " +
                                                 withKey.toStringKeyOnly(), e2);
                                    }
                                }

                                // Just skip it.
                                try {
                                    skippedCount += replicaCursor.skipNext(1);
                                    log.info("Skipped corrupt replica entry", e);
                                } catch (FetchException e2) {
                                    log.error("Unable to skip past corrupt replica entry", e2);
                                    throw e;
                                }
                            }

                            // Re-open (if we can)
                            if (lastReplicaEntry == null) {
                                break;
                            }
                            replicaCursor = replicaQuery.fetchAfter(lastReplicaEntry);
                        }
                    }
                }
                
                if (count++ >= RESYNC_WATERMARK || txnCount >= RESYNC_BATCH_SIZE) {
                    replicaTxn.commit();
                    if (replicaCursor != null) {
                        // Cursor should auto-close after txn commit, but force
                        // a close anyhow. Cursor is re-opened when it is
                        // allowed to advance.
                        replicaCursor.close();
                        replicaCursor = null;
                    }
                    count = 0;
                    txnCount = 0;
                }

                if (masterEntry == null && masterCursor.hasNext()) {
                    masterEntry = masterCursor.next();
                }

                Runnable resyncTask = null;

                // Comparator should treat null as high.
                int compare = comparator.compare(replicaEntry, masterEntry);

                if (compare < 0) {
                    // Bogus record exists only in replica so delete it.
                    resyncTask = prepareResyncTask(trigger, replicaEntry, null);
                    // Allow replica to advance.
                    if (replicaCursor == null) {
                        replicaCursor = replicaQuery.fetchAfter(replicaEntry);
                    }
                    lastReplicaEntry = replicaEntry;
                    replicaEntry = null;
                } else if (compare > 0) {
                    // Replica cursor is missing an entry so copy it.
                    resyncTask = prepareResyncTask(trigger, null, masterEntry);
                    // Allow master to advance.
                    lastMasterEntry = masterEntry;
                    masterEntry = null;
                } else {
                    if (replicaEntry == null && masterEntry == null) {
                        // Both cursors exhausted -- resync is complete.
                        break;
                    }

                    if (!replicaEntry.equalProperties(masterEntry)) {
                        // Replica is stale.
                        resyncTask = prepareResyncTask(trigger, replicaEntry, masterEntry);
                    }

                    // Entries are synchronized so allow both cursors to advance.
                    if (replicaCursor == null) {
                        replicaCursor = replicaQuery.fetchAfter(replicaEntry);
                    }
                    lastReplicaEntry = replicaEntry;
                    lastMasterEntry = masterEntry;
                    replicaEntry = null;
                    masterEntry = null;
                }

                if (resyncTask != null) {
                    txnCount++;
                    resyncTask.run();
                }
            }
        } finally {
            try {
                if (masterCursor != null) {
                    masterCursor.close();
                }
            } finally {
                if (replicaCursor != null) {
                    replicaCursor.close();
                }
            }
        }
    }

    private <S extends Storable> Runnable prepareResyncTask(final ReplicationTrigger<S> trigger,
                                                            final S replicaEntry,
                                                            final S masterEntry)
        throws RepositoryException
    {
        if (replicaEntry == null && masterEntry == null) {
            // If both are null, then there's nothing to do, is there?
            // Note: Caller shouldn't have passed double nulls to
            // prepareResyncTask in the first place.
            return null;
        }

        Runnable task = new Runnable() {
            public void run() {
                try {
                    trigger.resyncEntries(replicaEntry, masterEntry);
                } catch (Exception e) {
                    LogFactory.getLog(ReplicatedRepository.class).error(null, e);
                }
            }
        };

        return task;
    }
}
