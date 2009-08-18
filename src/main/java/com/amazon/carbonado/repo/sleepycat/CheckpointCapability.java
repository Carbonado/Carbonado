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

import com.amazon.carbonado.PersistDeniedException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.capability.Capability;

/**
 * Capability to control BDB checkpointing.
 *
 * @author Brian S O'Neill
 * @see HotBackupCapability
 */
public interface CheckpointCapability extends Capability {
    /**
     * Suspend the checkpointer until the suspension time has expired or until
     * manually resumed. If a checkpoint is in progress, this method will block
     * until it is finished. If checkpointing is disabled, calling this method
     * has no effect.
     *
     * <p>Calling this method repeatedly resets the suspension time. Each
     * invocation of suspendCheckpointer is like a lease renewal or heartbeat.
     *
     * @param suspensionTime minimum length of suspension, in milliseconds,
     * unless checkpointer is manually resumed
     */
    void suspendCheckpointer(long suspensionTime);

    /**
     * Resumes the checkpointer if it was suspended. If checkpointing is
     * disabled or if not suspended, calling this method has no effect.
     */
    void resumeCheckpointer();

    /**
     * Forces a checkpoint to run now, even if checkpointer is suspended or
     * disabled. If a checkpoint is in progress, then this method will block
     * until it is finished, and then run another checkpoint. This method does
     * not return until the requested checkpoint has finished.
     *
     * @throws PersistDeniedException if disabled during a hot backup
     */
    void forceCheckpoint() throws PersistException;
}
