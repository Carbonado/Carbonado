/*
 * Copyright 2009-2010 Amazon Technologies, Inc. or its affiliates.
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
 * <p>
 * If incremental backups are performed it is required that
 * log file removal is disabled in the underlying database.
 *
 * @author Brian S O'Neill
 * @author Olga Kuznetsova
 * @since 1.2.1
 */
public interface HotBackupCapability extends Capability {
///TODO:Have log file deletion be queued after all backups are completed
    /**
     * Starts the backup by disabling log file deletion. Be sure to call
     * endBackup when done to resume log file cleanup. Concurrent backups are
     * supported.
     *
     * <p>
     * To perform incremental backups use the builder option of setLogInMemory(false) 
     * so that old log files are not deleted. Log files can be deleted in the
     * future before starting a new backup (see method below).
     *
     * @throws IllegalStateException if log files are being removed (setLogInMemory(true))
     */
    Backup startBackup() throws RepositoryException;

    /**
     * Starts the backup by disabling log file deletion. Be sure to call 
     * endBackup when done to resume log file cleanup. Concurrent backups are supported.
     *
     * <p>
     * Caution should be observed when deleting old log files by force as log files as they may be required
     * for future incremental backups (if concurrent backups are running). 
     * If any concurrent backups are occurring, log fail deletion will fail.
     *
     * @param deleteOldLogFiles deletes log files that are no longer in use and have been backed up. False by default.
     * @throws IllegalStateException if log files are being removed (setLogInMemory(true))
     */
    Backup startBackup(boolean deleteOldLogFiles) throws RepositoryException;

    /**
     * Starts an incremental backup. Log files that are newer than the lastLogNumber will be copied
     * during the backup. Should only be run after performing a full backup.
     *
     * @param lastLogNumber number of the last log file that was copied in a previous backup.
     * @throws IllegalArgumentException if lastLogNumber is negative
     * @throws IllegalStateException if log files are being removed (setLogInMemory(true))
     */
    Backup startIncrementalBackup(long lastLogNumber) throws RepositoryException;

    /**
     * Starts an incremental backup. Log files that are newer than the lastLogNumber will be copied
     * during the backup. Can only be run after performing a full backup.
     *
     * <p>
     * Caution should be observed when deleting old log files by force as log files as they may be required
     * for future incremental backups (if concurrent backups are running). 
     * If any concurrent backups are occurring, log fail deletion will fail.
     *
     * @param lastLogNumber number of the last log file that was copied in a previous backup.
     * @param deleteOldLogFiles deletes log files that are no longer in use and have been backed up. False by default.
     * @throws IllegalArgumentException if lastLogNumber is negative
     * @throws IllegalStateException if log files are being removed (setLogInMemory(true))
     */
    Backup startIncrementalBackup(long lastLogNumber, boolean deleteOldLogFiles) throws RepositoryException;

    public static interface Backup {
        /**
	 * Resume normal operation.
         */
        void endBackup() throws RepositoryException;

        /**
         * Returns all the files to be copied, in the exact order in which they
         * must be copied.
	 *
	 * <p>
	 * These files must be copied prior to calling endBackup().
         *
         * @return ordered array of absolute files
         * @throws IllegalStateException if backup has ended
         */
        File[] getFiles() throws RepositoryException;

	/**
	 * Can be called after a backup has been performed to find the last log file
	 * that has been backed up.
	 *
	 * @return the file number of the last file in the current backup set.
	 * This number is required to perform incremental backups.
	 */
	long getLastLogNumber() throws RepositoryException;
    }
}
