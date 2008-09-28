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

package com.amazon.carbonado.capability;

import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Trigger;

/**
 * Capability of replicating repositories for re-synchronizing to the master
 * repository. A re-sync operation can be used to fill up a fresh replication
 * repository or to repair inconsistencies.
 *
 * @author Brian S O'Neill
 */
public interface ResyncCapability extends Capability {
    /**
     * Re-synchronizes replicated storables against the master repository.
     *
     * @param type type of storable to re-sync
     * @param desiredSpeed throttling parameter - 1.0 = full speed, 0.5 = half
     * speed, 0.1 = one-tenth speed, etc
     * @param filter optional query filter to limit which objects get re-sync'ed
     * @param filterValues filter values for optional filter
     */
    <S extends Storable> void resync(Class<S> type,
                                     double desiredSpeed,
                                     String filter,
                                     Object... filterValues)
        throws RepositoryException;

    /**
     * Re-synchronizes replicated storables against the master repository.
     *
     * @param type type of storable to re-sync
     * @param listener optional listener which gets notified as storables are re-sync'd
     * @param desiredSpeed throttling parameter - 1.0 = full speed, 0.5 = half
     * speed, 0.1 = one-tenth speed, etc
     * @param filter optional query filter to limit which objects get re-sync'ed
     * @param filterValues filter values for optional filter
     * @since 1.2
     */
    <S extends Storable> void resync(Class<S> type,
                                     Listener<? super S> listener,
                                     double desiredSpeed,
                                     String filter,
                                     Object... filterValues)
        throws RepositoryException;

    /**
     * Returns the immediate master Repository, for manual comparison. Direct
     * updates to the master will likely create inconsistencies.
     */
    Repository getMasterRepository();

    /**
     * Trigger which is invoked as storables get re-sync'd. Callbacks are
     * invoked in the scope of the resync transaction. If any unchecked
     * exception is thrown, the immediate changes are rolled back and the
     * entire repository resync operation is aborted.
     *
     * <p>The listener implementation should return quickly from the callback
     * methods, to avoid lingering transactions. If the listener is used to
     * invoke special repair operations, they should be placed into a task
     * queue. A separate thread can then perform the repairs outside the resync
     * transaction.
     */
    public static class Listener<S> extends Trigger<S> {
        /**
         * Called before a sync'd storable is to be inserted. Changes can be
         * made to the storable at this point, possibly to define independent
         * properties.
         *
         * @param newStorable sync'd storable before being inserted
         * @return arbitrary state object, passed to afterInsert or failedInsert method
         */
        @Override
        public Object beforeInsert(S newStorable) throws PersistException {
            return null;
        }

        /**
         * Called right after a sync'd storable has been successfully inserted.
         *
         * @param newStorable sync'd storable after being inserted
         * @param state object returned by beforeInsert method
         */
        @Override
        public void afterInsert(S newStorable, Object state) throws PersistException {
        }

        /**
         * Called when an insert operation failed because an exception was
         * thrown. The main purpose of this method is to allow any necessary
         * clean-up to occur on the optional state object.
         *
         * @param newStorable sync'd storable which failed to be inserted
         * @param state object returned by beforeInsert method, but it may be null
         */
        @Override
        public void failedInsert(S newStorable, Object state) {
        }

        /**
         * Called before a sync'd storable is to be updated. Changes can be
         * made to the storable at this point, possibly to update independent
         * properties.
         *
         * @param newStorable sync'd storable before being updated
         * @return arbitrary state object, passed to afterUpdate or failedUpdate method
         */
        @Override
        public Object beforeUpdate(S newStorable) throws PersistException {
            return null;
        }

        /**
         * Overloaded version of beforeUpdate method which is passed the
         * storable in it's out-of-sync and sync'd states. Changes can be made
         * to the storable at this point, possibly to update independent
         * properties.
         *
         * <p>The default implementation calls the single argument beforeUpdate
         * method, only passing the newly sync'd storable.
         *
         * @param oldStorable storable prior to being sync'd
         * @param newStorable sync'd storable before being updated
         */
        public Object beforeUpdate(S oldStorable, S newStorable) throws PersistException {
            return beforeUpdate(newStorable);
        }

        /**
         * Called right after a sync'd storable has been successfully updated.
         *
         * @param newStorable sync'd storable after being updated
         * @param state optional object returned by beforeUpdate method
         */
        @Override
        public void afterUpdate(S newStorable, Object state) throws PersistException {
        }

        /**
         * Called when an update operation failed because an exception was
         * thrown. The main purpose of this method is to allow any necessary
         * clean-up to occur on the optional state object.
         *
         * @param newStorable sync'd storable which failed to be updated
         * @param state object returned by beforeUpdate method, but it may be null
         */
        @Override
        public void failedUpdate(S newStorable, Object state) {
        }

        /**
         * Called before a bogus storable is to be deleted.
         *
         * @param oldStorable bogus storable before being deleted
         * @return arbitrary state object, passed to afterDelete or failedDelete method
         */
        @Override
        public Object beforeDelete(S oldStorable) throws PersistException {
            return null;
        }

        /**
         * Called right after a bogus storable has been successfully deleted.
         *
         * @param oldStorable bogus storable after being deleted
         * @param state optional object returned by beforeDelete method
         */
        @Override
        public void afterDelete(S oldStorable, Object state) throws PersistException {
        }

        /**
         * Called when a delete operation failed because an exception was
         * thrown. The main purpose of this method is to allow any necessary
         * clean-up to occur on the optional state object.
         *
         * @param oldStorable bogus storable which failed to be deleted
         * @param state object returned by beforeDelete method, but it may be null
         */
        @Override
        public void failedDelete(S oldStorable, Object state) {
        }
    }
}
