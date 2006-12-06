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

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

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
     * Returns the immediate master Repository, for manual comparison. Direct
     * updates to the master will likely create inconsistencies.
     */
    Repository getMasterRepository();
}
