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

package com.amazon.carbonado.cursor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Used internally by {@link MergeSortBuffer}.
 *
 * @author Brian S O'Neill
 */
class WorkFilePool {
    private static final String cTempDir = System.getProperty("java.io.tmpdir");

    private static final ConcurrentMap<String, WorkFilePool> cPools =
        new ConcurrentHashMap<String, WorkFilePool>();

    /**
     * @param tempDir directory to store temp files for merging, or null for default
     */
    static WorkFilePool getInstance(String tempDir) {
        // This method uses ConcurrentMap features to eliminate the need to
        // ever synchronize access, since this method may be called frequently.

        if (tempDir == null) {
            tempDir = cTempDir;
        }

        WorkFilePool pool = cPools.get(tempDir);

        if (pool != null) {
            return pool;
        }

        String canonical;
        try {
            canonical = new File(tempDir).getCanonicalPath();
        } catch (IOException e) {
            canonical = new File(tempDir).getAbsolutePath();
        }

        if (!canonical.equals(tempDir)) {
            pool = getInstance(canonical);
            cPools.putIfAbsent(tempDir, pool);
            return pool;
        }

        pool = new WorkFilePool(new File(tempDir));

        WorkFilePool existing = cPools.putIfAbsent(tempDir, pool);

        if (existing == null) {
            // New pool is the winner, so finish off initialization. Pool can
            // be used concurrently by another thread without shutdown hook.
            pool.addShutdownHook(tempDir);
        } else {
            pool = existing;
        }

        return pool;
    }

    private final File mTempDir;

    // Work files not currently being used by any MergeSortBuffer.
    private final List<RandomAccessFile> mWorkFilePool;
    // Instances of MergeSortBuffer, to be notified on shutdown that they
    // should close their work files.
    private final Set<MergeSortBuffer<?>> mWorkFileUsers;

    private Thread mShutdownHook;

    /**
     * @param tempDir directory to store temp files for merging, or null for default
     */
    private WorkFilePool(File tempDir) {
        mTempDir = tempDir;
        mWorkFilePool = new ArrayList<RandomAccessFile>();
        mWorkFileUsers = new HashSet<MergeSortBuffer<?>>();
    }

    RandomAccessFile acquireWorkFile(MergeSortBuffer<?> buffer) throws IOException {
        synchronized (mWorkFileUsers) {
            mWorkFileUsers.add(buffer);
        }
        synchronized (mWorkFilePool) {
            if (mWorkFilePool.size() > 0) {
                return mWorkFilePool.remove(mWorkFilePool.size() - 1);
            }
        }
        File file = File.createTempFile("carbonado-mergesort-", null, mTempDir);
        file.deleteOnExit();
        return new RandomAccessFile(file, "rw");
    }

    void releaseWorkFiles(List<RandomAccessFile> files) {
        synchronized (mWorkFilePool) {
            for (RandomAccessFile raf : files) {
                try {
                    raf.seek(0);
                    // Return space to file system.
                    raf.setLength(0);
                    mWorkFilePool.add(raf);
                } catch (IOException e) {
                    // Work file is broken, discard it.
                    try {
                        raf.close();
                    } catch (IOException e2) {
                    }
                }
            }
        }
    }

    void unregisterWorkFileUser(MergeSortBuffer<?> buffer) {
        synchronized (mWorkFileUsers) {
            mWorkFileUsers.remove(buffer);
            // Only one wait condition, so okay to not call notifyAll.
            mWorkFileUsers.notify();
        }
    }

    private synchronized void addShutdownHook(String tempDir) {
        if (mShutdownHook != null) {
            return;
        }

        // Add shutdown hook to close work files so that they can be deleted.
        String threadName = "MergeSortBuffer shutdown (" + tempDir + ')';

        mShutdownHook = new Thread(threadName) {
            @Override
            public void run() {
                // Notify users of work files and wait for them to close.
                synchronized (mWorkFileUsers) {
                    for (MergeSortBuffer<?> buffer : mWorkFileUsers) {
                        buffer.stop();
                    }
                    final long timeout = 10000;
                    final long giveup = System.currentTimeMillis() + timeout;
                    try {
                        while (mWorkFileUsers.size() > 0) {
                            long now = System.currentTimeMillis();
                            if (now < giveup) {
                                mWorkFileUsers.wait(giveup - now);
                            } else {
                                break;
                            }
                        }
                    } catch (InterruptedException e) {
                    }
                }

                synchronized (mWorkFilePool) {
                    for (RandomAccessFile raf : mWorkFilePool) {
                        try {
                            raf.close();
                        } catch (IOException e) {
                        }
                    }
                    mWorkFilePool.clear();
                }
            }
        };

        Runtime.getRuntime().addShutdownHook(mShutdownHook);
    }
}
