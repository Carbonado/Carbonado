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
 * Capability for performing a backup of an active BDB environment. If {@link
 * BDBRepositoryBuilder#setLogInMemory(boolean) in-memory logging} is enabled,
 * backups cannot be performed. To restore from a hot backup, it is
 * <b>critical</b> that a full recovery be performed.  Pass true to {@link
 * BDBRepositoryBuilder#setRunFullRecovery(boolean) setRunFullRecovery} to
 * enable. {@link BDBProduct#JE BDB-JE} does not require this, however.
 * 
 * <p>To support incremental backups against the {@link BDBProduct#DB native
 * BDB product}, old log files must be kept. Pass true to {@link
 * BDBRepositoryBuilder#setKeepOldLogFiles(boolean) setKeepOldLogFiles}.
 *
 * @author Brian S O'Neill
 * @author Olga Kuznetsova
 * @since 1.2.1
 */
public interface HotBackupCapability extends Capability {
    /**
     * Starts the backup by disabling log file deletion. Be sure to call
     * endBackup when done to resume log file cleanup. Concurrent backups are
     * supported.
     *
     * @throws IllegalStateException if configuration doesn't support backups
     */
    Backup startBackup() throws RepositoryException;

    /**
     * Starts the backup by disabling log file deletion. Be sure to call 
     * endBackup when done to resume log file cleanup. Concurrent backups are supported.
     *
     * <p>Caution should be observed when deleting old log files by force, if
     * an external process is also performing backups. If a concurrent backup
     * is issued by this repository instance, log file deletion is suppressed.
     *
     * @param deleteOldLogFiles deletes log files that are no longer in use and
     * have been backed up. False by default.
     * @throws IllegalStateException if configuration doesn't support backups
     */
    Backup startBackup(boolean deleteOldLogFiles) throws RepositoryException;

    /**
     * Starts an incremental backup. Log files that are newer than the
     * lastLogNumber will be copied during the backup. Should only be run after
     * performing a full backup.
     *
     * @param lastLogNumber number of the last log file that was copied in a previous backup
     * @throws IllegalArgumentException if lastLogNumber is negative
     * @throws IllegalStateException if configuration doesn't support backups
     */
    Backup startIncrementalBackup(long lastLogNumber) throws RepositoryException;

    /**
     * Starts an incremental backup. Log files that are newer than the lastLogNumber will be copied
     * during the backup. Can only be run after performing a full backup.
     *
     * <p>Caution should be observed when deleting old log files by force, if
     * an external process is also performing backups. If a concurrent backup
     * is issued by this repository instance, log file deletion is suppressed.
     *
     * @param lastLogNumber number of the last log file that was copied in a previous backup.
     * @param deleteOldLogFiles deletes log files that are no longer in use and
     * have been backed up. False by default.
     * @throws IllegalArgumentException if lastLogNumber is negative
     * @throws IllegalStateException if configuration doesn't support backups
     */
    Backup startIncrementalBackup(long lastLogNumber, boolean deleteOldLogFiles)
        throws RepositoryException;

    public static interface Backup {
        /**
         * Resume normal operation.
         */
        void endBackup() throws RepositoryException;

        /**
         * @deprecated use getDataFiles and getLogFiles
         */
        @Deprecated
        File[] getFiles() throws RepositoryException;

        /**
         * Returns all the data files to be copied. After these files are
         * durably copied, call {@link #getLogFiles()} and copy the log files
         * which were created while the data files were copied.
         *
         * @return array of data files, which might be empty
         */
        File[] getDataFiles() throws RepositoryException;

        /**
         * Returns all the transaction log files to be copied, in the exact
         * order in which they must be copied. After these files are durably
         * copied, call {@link #endBackup()}.
         *
         * @return array of transaction log files, never empty
         */
        File[] getLogFiles() throws RepositoryException;

        /**
         * Can be called after a backup has been performed to find the last log file
         * that has been backed up.
         *
         * @return the file number of the last file in the current backup set.
         * This number is required to perform incremental backups.
         * @throws IllegalStateException if {@link #getFiles()} was not called
         */
        long getLastLogNumber() throws RepositoryException;
    }
}
