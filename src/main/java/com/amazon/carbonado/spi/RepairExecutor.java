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

package com.amazon.carbonado.spi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A convenience class for repositories to run dynamic repairs in separate
 * threads. When a repository detects a consistency error during a user
 * operation, it should not perform the repair in the same thread.
 *
 * <p>If the repair was initiated by an exception, but the original exception
 * is re-thrown, a transaction exit will rollback the repair! Executing the
 * repair in a separate thread allows it to wait until the transaction has
 * exited.
 *
 * <p>Other kinds of inconsistencies might be detected during cursor
 * iteration. The repair will need to acquire write locks, but the open cursor
 * might not allow that, resulting in deadlock. Executing the repair in a
 * separate thread allows it to wait until the cursor has released locks.
 *
 * <p>This class keeps thread-local references to single-threaded executors. In
 * other words, each user thread has at most one associated repair thread. Each
 * repair thread has a fixed size queue, and they exit when they are idle. If
 * the queue is full, newly added repair tasks are silently discarded.
 *
 * <p>The following system properties are supported:
 *
 * <ul>
 * <li>com.amazon.carbonado.spi.RepairExecutor.keepAliveSeconds (default is 10)
 * <li>com.amazon.carbonado.spi.RepairExecutor.queueSize (default is 10000)
 * </ul>
 *
 * @author Brian S O'Neill
 */
public class RepairExecutor {
    static final ThreadLocal<RepairExecutor> cExecutor;

    static {
        final int keepAliveSeconds = Integer.getInteger
            ("com.amazon.carbonado.spi.RepairExecutor.keepAliveSeconds", 10);
        final int queueSize = Integer.getInteger
            ("com.amazon.carbonado.spi.RepairExecutor.queueSize", 10000);

        cExecutor = new ThreadLocal<RepairExecutor>() {
            @Override
            protected RepairExecutor initialValue() {
                return new RepairExecutor(keepAliveSeconds, queueSize);
            }
        };
    }

    public static void execute(Runnable repair) {
        cExecutor.get().executeIt(repair);
    }

    /**
     * Waits for repairs that were executed from the current thread to finish.
     *
     * @return true if all repairs are finished
     */
    public static boolean waitForRepairsToFinish(long timeoutMillis) throws InterruptedException {
        return cExecutor.get().waitToFinish(timeoutMillis);
    }

    private final int mKeepAliveSeconds;
    private BlockingQueue<Runnable> mQueue;
    private Worker mWorker;
    private boolean mIdle = true;

    private RepairExecutor(int keepAliveSeconds, int queueSize) {
        mKeepAliveSeconds = keepAliveSeconds;
        mQueue = new LinkedBlockingQueue<Runnable>(queueSize);
    }

    private synchronized void executeIt(Runnable repair) {
        mQueue.offer(repair);
        if (mWorker == null) {
            mWorker = new Worker();
            mWorker.start();
        }
    }

    private synchronized boolean waitToFinish(long timeoutMillis) throws InterruptedException {
        if (mIdle && mQueue.size() == 0) {
            return true;
        }

        if (mWorker == null) {
            // The worker should never be null if the queue has elements.
            mWorker = new Worker();
            mWorker.start();
        }

        if (timeoutMillis != 0) {
            if (timeoutMillis < 0) {
                while (!mIdle || mQueue.size() > 0) {
                    wait();
                }
            } else {
                long start = System.currentTimeMillis();
                while (timeoutMillis > 0 && (!mIdle || mQueue.size() > 0)) {
                    wait(timeoutMillis);
                    long now = System.currentTimeMillis();
                    timeoutMillis -= (now - start);
                    start = now;
                }
            }
        }

        return mQueue.size() == 0;
    }

    Runnable dequeue() throws InterruptedException {
        while (true) {
            synchronized (this) {
                mIdle = true;
                // Only one wait condition, so okay to not call notifyAll.
                notify();
            }
            Runnable task = mQueue.poll(mKeepAliveSeconds, TimeUnit.SECONDS);
            synchronized (this) {
                if (task != null) {
                    mIdle = false;
                    return task;
                }
                if (mQueue.size() == 0) {
                    // Only one wait condition, so okay to not call notifyAll.
                    notify();
                    mWorker = null;
                    return null;
                }
            }
        }
    }

    private class Worker extends Thread {
        Worker() {
            setDaemon(true);
            setName(Thread.currentThread().getName() + " (repository repair)");
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Runnable task = dequeue();
                    if (task == null) {
                        break;
                    }
                    task.run();
                } catch (InterruptedException e) {
                    break;
                } catch (ThreadDeath e) {
                    break;
                } catch (Throwable e) {
                    try {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    } catch (ThreadDeath e2) {
                        break;
                    } catch (Throwable e2) {
                        // Ignore exceptions thrown while reporting exceptions.
                    }
                }
            }
        }
    }
}
