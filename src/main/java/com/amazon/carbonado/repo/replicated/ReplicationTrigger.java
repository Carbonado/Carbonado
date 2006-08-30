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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.FetchDeadlockException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.OptimisticLockException;
import com.amazon.carbonado.PersistDeadlockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.spi.RepairExecutor;

/**
 * All inserts/updates/deletes are first committed to the master storage, then
 * duplicated and committed to the replica.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
class ReplicationTrigger<S extends Storable> extends Trigger<S> {
    private final Repository mRepository;
    private final Storage<S> mReplicaStorage;
    private final Storage<S> mMasterStorage;

    private final ThreadLocal<AtomicInteger> mDisabled = new ThreadLocal<AtomicInteger>();

    ReplicationTrigger(Repository repository,
                       Storage<S> replicaStorage,
                       Storage<S> masterStorage)
    {
        mRepository = repository;
        mReplicaStorage = replicaStorage;
        mMasterStorage = masterStorage;
    }

    @Override
    public Object beforeInsert(S replica) throws PersistException {
        return beforeInsert(replica, false);
    }

    @Override
    public Object beforeTryInsert(S replica) throws PersistException {
        return beforeInsert(replica, true);
    }

    private Object beforeInsert(S replica, boolean forTry) throws PersistException {
        if (isReplicationDisabled()) {
            return null;
        }

        final S master = mMasterStorage.prepare();
        replica.copyAllProperties(master);

        try {
            if (forTry) {
                if (!master.tryInsert()) {
                    throw abortTry();
                }
            } else {
                master.insert();
            }
        } catch (UniqueConstraintException e) {
            // This may be caused by an inconsistency between replica and
            // master. Here's one scenerio: user called tryLoad and saw the
            // entry does not exist. So instead of calling update, he/she calls
            // insert. If the master entry exists, then there is an
            // inconsistency. The code below checks for this specific kind of
            // error and repairs it by inserting a record in the replica.

            // Here's another scenerio: Unique constraint was caused by an
            // inconsistency with the values of the alternate keys. User
            // expected alternate keys to have unique values, as indicated by
            // replica.

            repair(replica);

            // Throw exception since we don't know what the user's intentions
            // really are.
            throw e;
        }

        // Master may have applied sequences to unitialized primary keys, so
        // copy primary keys to replica. Mark properties as dirty to allow
        // primary key to be changed.
        replica.markPropertiesDirty();

        // Copy all properties in order to trigger constraints that
        // master should have resolved.
        master.copyAllProperties(replica);

        return null;
    }

    @Override
    public Object beforeUpdate(S replica) throws PersistException {
        return beforeUpdate(replica, false);
    }

    @Override
    public Object beforeTryUpdate(S replica) throws PersistException {
        return beforeUpdate(replica, true);
    }

    private Object beforeUpdate(S replica, boolean forTry) throws PersistException {
        if (isReplicationDisabled()) {
            return null;
        }

        final S master = mMasterStorage.prepare();
        replica.copyPrimaryKeyProperties(master);

        if (!replica.hasDirtyProperties()) {
            // Nothing to update, but must load from master anyhow, since
            // update must always perform a fresh load as a side-effect. We
            // cannot simply call update on the master, since it may need a
            // version property to be set. Setting the version has the
            // side-effect of making the storable look dirty, so the master
            // will perform an update. This in turn causes the version to
            // increase for no reason.
            try {
                if (forTry) {
                    if (!master.tryLoad()) {
                        // Master record does not exist. To ensure consistency,
                        // delete record from replica.
                        deleteReplica(replica);
                        throw abortTry();
                    }
                } else {
                    try {
                        master.load();
                    } catch (FetchNoneException e) {
                        // Master record does not exist. To ensure consistency,
                        // delete record from replica.
                        deleteReplica(replica);
                        throw e;
                    }
                }
            } catch (FetchException e) {
                throw e.toPersistException
                    ("Could not load master object for update: " + master.toStringKeyOnly());
            }
        } else {
            replica.copyVersionProperty(master);
            replica.copyDirtyProperties(master);

            try {
                if (forTry) {
                    if (!master.tryUpdate()) {
                        // Master record does not exist. To ensure consistency,
                        // delete record from replica.
                        deleteReplica(replica);
                        throw abortTry();
                    }
                } else {
                    try {
                        master.update();
                    } catch (PersistNoneException e) {
                        // Master record does not exist. To ensure consistency,
                        // delete record from replica.
                        deleteReplica(replica);
                        throw e;
                    }
                }
            } catch (OptimisticLockException e) {
                // This may be caused by an inconsistency between replica and
                // master.

                repair(replica);

                // Throw original exception since we don't know what the user's
                // intentions really are.
                throw e;
            }
        }

        // Copy master properties back, since its repository may have
        // altered property values as a side effect.
        master.copyUnequalProperties(replica);

        return null;
    }

    @Override
    public Object beforeDelete(S replica) throws PersistException {
        if (isReplicationDisabled()) {
            return null;
        }

        S master = mMasterStorage.prepare();
        replica.copyPrimaryKeyProperties(master);

        // If this fails to delete anything, don't care. Any delete failure
        // will be detected when the replica is deleted. If there was an
        // inconsistency, it is resolved after the replica is deleted.
        master.tryDelete();

        return null;
    }

    /**
     * Re-sync the replica to the master. The primary keys of both entries are
     * assumed to match.
     *
     * @param replicaEntry current replica entry, or null if none
     * @param masterEntry current master entry, or null if none
     */
    void resyncEntries(S replicaEntry, S masterEntry) throws FetchException, PersistException {
        if (replicaEntry == null && masterEntry == null) {
            return;
        }

        Log log = LogFactory.getLog(ReplicatedRepository.class);

        setReplicationDisabled(true);
        try {
            Transaction txn = mRepository.enterTransaction();
            try {
                if (replicaEntry != null) {
                    if (masterEntry == null) {
                        log.info("Deleting bogus entry: " + replicaEntry);
                    }
                    replicaEntry.tryDelete();
                }
                if (masterEntry != null) {
                    Storable newReplicaEntry = mReplicaStorage.prepare();
                    if (replicaEntry == null) {
                        masterEntry.copyAllProperties(newReplicaEntry);
                        log.info("Adding missing entry: " + newReplicaEntry);
                    } else {
                        if (replicaEntry.equalProperties(masterEntry)) {
                            return;
                        }
                        // First copy from old replica to preserve values of
                        // any independent properties. Be sure not to copy
                        // nulls from old replica to new replica, in case new
                        // non-nullable properties have been added. This is why
                        // copyUnequalProperties is called instead of
                        // copyAllProperties.
                        replicaEntry.copyUnequalProperties(newReplicaEntry);
                        // Calling copyAllProperties will skip unsupported
                        // independent properties in master, thus preserving
                        // old independent property values.
                        masterEntry.copyAllProperties(newReplicaEntry);
                        log.info("Replacing stale entry with: " + newReplicaEntry);
                    }
                    if (!newReplicaEntry.tryInsert()) {
                        // Try to correct bizarre corruption.
                        newReplicaEntry.tryDelete();
                        newReplicaEntry.tryInsert();
                    }
                }
                txn.commit();
            } finally {
                txn.exit();
            }
        } finally {
            setReplicationDisabled(false);
        }
    }

    /**
     * Runs a repair in a background thread. This is done for two reasons: It
     * allows repair to not be hindered by locks acquired by transactions and
     * repairs don't get rolled back when culprit exception is thrown. Culprit
     * may be UniqueConstraintException or OptimisticLockException.
     */
    private void repair(S replica) throws PersistException {
        replica = (S) replica.copy();
        S master = mMasterStorage.prepare();
        replica.copyPrimaryKeyProperties(master);

        try {
            if (replica.tryLoad()) {
                if (master.tryLoad()) {
                    if (replica.equalProperties(master)) {
                        // Both are equal -- no repair needed.
                        return;
                    }
                }
            } else if (!master.tryLoad()) {
                // Both are missing -- no repair needed.
                return;
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }

        final S finalReplica = replica;
        final S finalMaster = master;

        RepairExecutor.execute(new Runnable() {
            public void run() {
                try {
                    Transaction txn = mRepository.enterTransaction();
                    try {
                        txn.setForUpdate(true);
                        if (finalReplica.tryLoad()) {
                            if (finalMaster.tryLoad()) {
                                resyncEntries(finalReplica, finalMaster);
                            } else {
                                resyncEntries(finalReplica, null);
                            }
                        } else if (finalMaster.tryLoad()) {
                            resyncEntries(null, finalMaster);
                        }
                        txn.commit();
                    } finally {
                        txn.exit();
                    }
                } catch (FetchException fe) {
                    Log log = LogFactory.getLog(ReplicatedRepository.class);
                    log.warn("Unable to check if repair is required for " +
                             finalReplica.toStringKeyOnly(), fe);
                } catch (PersistException pe) {
                    Log log = LogFactory.getLog(ReplicatedRepository.class);
                    log.error("Unable to repair entry " +
                              finalReplica.toStringKeyOnly(), pe);
                }
            }
        });
    }

    /**
     * Deletes the replica entry with replication disabled.
     */
    private void deleteReplica(S replica) throws PersistException {
        // Disable replication to prevent trigger from being invoked by
        // deleting replica.
        setReplicationDisabled(true);
        try {
            replica.tryDelete();
        } finally {
            setReplicationDisabled(false);
        }
    }

    /**
     * Returns true if replication is disabled for the current thread.
     */
    private boolean isReplicationDisabled() {
        // Count indicates how many times disabled (nested)
        AtomicInteger i = mDisabled.get();
        return i != null && i.get() > 0;
    }

    /**
     * By default, replication is enabled for the current thread. Pass true to
     * disable during re-sync operations.
     */
    private void setReplicationDisabled(boolean disabled) {
        // Using a count allows this method call to be nested. Based on the
        // current implementation, it should never be nested, so this extra
        // work is just a safeguard.
        AtomicInteger i = mDisabled.get();
        if (disabled) {
            if (i == null) {
                i = new AtomicInteger(1);
                mDisabled.set(i);
            } else {
                i.incrementAndGet();
            }
        } else {
            if (i != null) {
                i.decrementAndGet();
            }
        }
    }
}
