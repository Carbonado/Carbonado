/*
 * Copyright 2009 Amazon Technologies, Inc. or its affiliates.
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

import java.io.File;

import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.capability.Capability;

/**
 * Capability for performing a backup of an active BDB environment. To restore
 * from a hot backup, it is <b>critical</b> that a full recovery be
 * performed. BDB-JE does not require this, however. Pass true to {@link
 * BDBRepositoryBuilder#setRunFullRecovery(boolean)} to enable.
 *
 * @author Brian S O'Neill
 * @since 1.2.1
 */
public interface HotBackupCapability extends Capability {
    /**
     * Starts the backup by disabling log file deletion. Be sure to call
     * endBackup when done to resume log file cleanup. Concurrent backups are
     * supported.
     */
    Backup startBackup() throws RepositoryException;

    public static interface Backup {
        /**
         * End the backup and resume log file cleanup.
         */
        void endBackup() throws RepositoryException;

        /**
         * Returns all the files to be copied, in the exact order in which they
         * must be copied.
         *
         * @return ordered array of absolute files
         * @throws IllegalStateException if backup has ended
         */
        File[] getFiles() throws RepositoryException;
    }
}
