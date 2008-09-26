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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.OptimisticLockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.capability.ResyncCapability;

import com.amazon.carbonado.spi.RepairExecutor;
import com.amazon.carbonado.spi.TriggerManager;

import com.amazon.carbonado.util.ThrowUnchecked;

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

    private final TriggerManager<S> mTriggerManager;

    ReplicationTrigger(Repository repository,
                       Storage<S> replicaStorage,
                       Storage<S> masterStorage)
    {
        mRepository = repository;
        mReplicaStorage = replicaStorage;
        mMasterStorage = masterStorage;

        // Use TriggerManager to locally disable trigger execution during
        // resync and repairs.
        mTriggerManager = new TriggerManager<S>();
        mTriggerManager.addTrigger(this);

        BlobReplicationTrigger<S> blobTrigger = BlobReplicationTrigger.create(masterStorage);
        if (blobTrigger != null) {
            mTriggerManager.addTrigger(blobTrigger);
        }

        ClobReplicationTrigger<S> clobTrigger = ClobReplicationTrigger.create(masterStorage);
        if (clobTrigger != null) {
            mTriggerManager.addTrigger(clobTrigger);
        }

        replicaStorage.addTrigger(mTriggerManager);
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
        final S master = mMasterStorage.prepare();
        replica.copyPrimaryKeyProperties(master);
        replica.copyVersionProperty(master);
        replica.copyDirtyProperties(master);

        try {
            if (forTry) {
                if (!master.tryUpdate()) {
                    // Master record does not exist. To ensure consistency,
                    // delete record from replica.
                    if (tryDeleteReplica(replica)) {
                        // Replica was inconsistent, but caller might be in a
                        // transaction and rollback the repair. Run repair
                        // again in separate thread to ensure it sticks.
                        repair(replica);
                    }
                    throw abortTry();
                }
            } else {
                try {
                    master.update();
                } catch (PersistNoneException e) {
                    // Master record does not exist. To ensure consistency,
                    // delete record from replica.
                    if (tryDeleteReplica(replica)) {
                        // Replica was inconsistent, but caller might be in a
                        // transaction and rollback the repair. Run repair
                        // again in separate thread to ensure it sticks.
                        repair(replica);
                    }
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

        // Copy master properties back, since its repository may have
        // altered property values as a side effect.
        master.copyUnequalProperties(replica);

        return null;
    }

    @Override
    public Object beforeDelete(S replica) throws PersistException {
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
     * @param listener optional
     * @param replicaEntry current replica entry, or null if none
     * @param masterEntry current master entry, or null if none
     */
    void resyncEntries(ResyncCapability.Listener<? super S> listener,
                       S replicaEntry, S masterEntry)
        throws FetchException, PersistException
    {
        if (replicaEntry == null && masterEntry == null) {
            return;
        }

        Log log = LogFactory.getLog(ReplicatedRepository.class);

        setReplicationDisabled();
        try {
            Transaction txn = mRepository.enterTransaction();
            try {
                final S newReplicaEntry;
                if (replicaEntry == null) {
                    newReplicaEntry = mReplicaStorage.prepare();
                    masterEntry.copyAllProperties(newReplicaEntry);
                    log.info("Inserting missing replica entry: " + newReplicaEntry);
                } else if (masterEntry != null) {
                    if (replicaEntry.equalProperties(masterEntry)) {
                        return;
                    }
                    newReplicaEntry = mReplicaStorage.prepare();
                    transferToReplicaEntry(replicaEntry, masterEntry, newReplicaEntry);
                    log.info("Updating stale replica entry with: " + newReplicaEntry);
                } else {
                    newReplicaEntry = null;
                    log.info("Deleting bogus replica entry: " + replicaEntry);
                }

                final Object state;
                if (listener == null) {
                    state = null;
                } else {
                    if (replicaEntry == null) {
                        state = listener.beforeInsert(newReplicaEntry);
                    } else if (masterEntry != null) {
                        state = listener.beforeUpdate(replicaEntry, newReplicaEntry);
                    } else {
                        state = listener.beforeDelete(replicaEntry);
                    }
                }

                try {
                    // Delete old entry.
                    if (replicaEntry != null) {
                        try {
                            replicaEntry.tryDelete();
                        } catch (PersistException e) {
                            log.error("Unable to delete replica entry: " + replicaEntry, e);
                            if (masterEntry != null) {
                                // Try to update instead.
                                log.info("Updating corrupt replica entry with: " +
                                         newReplicaEntry);
                                try {
                                    newReplicaEntry.update();
                                    // This disables the insert step, which is not needed now.
                                    masterEntry = null;
                                } catch (PersistException e2) {
                                    log.error("Unable to update replica entry: " +
                                              replicaEntry, e2);
                                    resyncFailed(listener, replicaEntry, masterEntry,
                                                 newReplicaEntry, state);
                                    return;
                                }
                            }
                        }
                    }

                    // Insert new entry.
                    if (masterEntry != null && newReplicaEntry != null) {
                        if (!newReplicaEntry.tryInsert()) {
                            // Try to correct bizarre corruption.
                            newReplicaEntry.tryDelete();
                            newReplicaEntry.tryInsert();
                        }
                    }

                    if (listener != null) {
                        if (replicaEntry == null) {
                            listener.afterInsert(newReplicaEntry, state);
                        } else if (masterEntry != null) {
                            listener.afterUpdate(newReplicaEntry, state);
                        } else {
                            listener.afterDelete(replicaEntry, state);
                        }
                    }

                    txn.commit();
                } catch (Throwable e) {
                    resyncFailed(listener, replicaEntry, masterEntry, newReplicaEntry, state);
                    ThrowUnchecked.fire(e);
                }
            } finally {
                txn.exit();
            }
        } finally {
            setReplicationEnabled();
        }
    }

    private void resyncFailed(ResyncCapability.Listener<? super S> listener,
                              S replicaEntry, S masterEntry,
                              S newReplicaEntry, Object state)
    {
        if (listener != null) {
            try {
                if (replicaEntry == null) {
                    listener.failedInsert(newReplicaEntry, state);
                } else if (masterEntry != null) {
                    listener.failedUpdate(newReplicaEntry, state);
                } else {
                    listener.failedDelete(replicaEntry, state);
                }
            } catch (Throwable e2) {
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e2);
            }
        }
    }

    private void transferToReplicaEntry(S replicaEntry, S masterEntry, S newReplicaEntry) {
        // First copy from old replica to preserve values of any independent
        // properties. Be sure not to copy nulls from old replica to new
        // replica, in case new non-nullable properties have been added. This
        // is why copyUnequalProperties is called instead of copyAllProperties.
        replicaEntry.copyUnequalProperties(newReplicaEntry);
        // Calling copyAllProperties will skip unsupported independent
        // properties in master, thus preserving old independent property values.
        masterEntry.copyAllProperties(newReplicaEntry);
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
        // Must copy more than just primary key properties to master since
        // replica object might only have alternate keys.
        replica.copyAllProperties(master);

        try {
            if (replica.tryLoad()) {
                if (master.tryLoad()) {
                    if (replica.equalProperties(master)) {
                        // Both are equal -- no repair needed.
                        return;
                    }
                }
            } else {
                if (!master.tryLoad()) {
                    // Both are missing -- no repair needed.
                    return;
                }
            }
        } catch (IllegalStateException e) {
            // Can be caused by not fully defining the primary key on the
            // replica, but an alternate key is. The insert will fail anyhow,
            // so don't try to repair.
            return;
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
                                resyncEntries(null, finalReplica, finalMaster);
                            } else {
                                resyncEntries(null, finalReplica, null);
                            }
                        } else if (finalMaster.tryLoad()) {
                            resyncEntries(null, null, finalMaster);
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

    boolean addTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.addTrigger(trigger);
    }

    boolean removeTrigger(Trigger<? super S> trigger) {
        return mTriggerManager.removeTrigger(trigger);
    }

    /**
     * Deletes the replica entry with replication disabled.
     */
    boolean tryDeleteReplica(Storable replica) throws PersistException {
        // Prevent trigger from being invoked by deleting replica.
        TriggerManager tm = mTriggerManager;
        tm.locallyDisableDelete();
        try {
            return replica.tryDelete();
        } finally {
            tm.locallyEnableDelete();
        }
    }

    /**
     * Deletes the replica entry with replication disabled.
     */
    void deleteReplica(Storable replica) throws PersistException {
        // Prevent trigger from being invoked by deleting replica.
        TriggerManager tm = mTriggerManager;
        tm.locallyDisableDelete();
        try {
            replica.delete();
        } finally {
            tm.locallyEnableDelete();
        }
    }

    void setReplicationDisabled() {
        // This method disables not only this trigger, but all triggers added
        // to manager.
        TriggerManager tm = mTriggerManager;
        tm.locallyDisableInsert();
        tm.locallyDisableUpdate();
        tm.locallyDisableDelete();
        tm.locallyDisableLoad();
    }

    void setReplicationEnabled() {
        TriggerManager tm = mTriggerManager;
        tm.locallyEnableInsert();
        tm.locallyEnableUpdate();
        tm.locallyEnableDelete();
        tm.locallyEnableLoad();
    }
}
