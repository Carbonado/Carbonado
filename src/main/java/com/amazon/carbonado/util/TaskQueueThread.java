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

package com.amazon.carbonado.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Simple generic queue for running tasks from a single thread. Be sure to call
 * shutdown or interrupt when done using the thread, or else it will never exit.
 *
 * @author Brian S O'Neill
 */
public class TaskQueueThread extends Thread implements Executor {
    private static final int
        STATE_NOT_STARTED = 0,
        STATE_RUNNING = 1,
        STATE_SHOULD_STOP = 2,
        STATE_STOPPED = 3;

    private static final Runnable STOP_TASK = new Runnable() {public void run() {}};

    private final BlockingQueue<Runnable> mQueue;
    private final long mTimeoutMillis;

    private int mState = STATE_NOT_STARTED;

    /**
     * @param name name to give this thread
     * @param queueSize fixed size of queue
     */
    public TaskQueueThread(String name, int queueSize) {
        this(name, queueSize, 0);
    }

    /**
     * @param name name to give this thread
     * @param queueSize fixed size of queue
     * @param timeoutMillis default maximum time to wait for queue to have an available slot
     */
    public TaskQueueThread(String name, int queueSize, long timeoutMillis) {
        super(name);
        mQueue = new ArrayBlockingQueue<Runnable>(queueSize, true);
        mTimeoutMillis = timeoutMillis;
    }

    /**
     * Enqueue a task to run.
     *
     * @param task task to enqueue
     * @throws RejectedExecutionException if wait interrupted, timeout expires,
     * or shutdown has been called
     */
    public void execute(Runnable task) throws RejectedExecutionException {
        execute(task, mTimeoutMillis);
    }

    /**
     * Enqueue a task to run.
     *
     * @param task task to enqueue
     * @param timeoutMillis maximum time to wait for queue to have an available slot
     * @throws RejectedExecutionException if wait interrupted, timeout expires,
     * or shutdown has been called
     */
    public void execute(Runnable task, long timeoutMillis) throws RejectedExecutionException {
        if (task == null) {
            throw new NullPointerException("Cannot accept null task");
        }
        synchronized (this) {
            if (mState != STATE_RUNNING && mState != STATE_NOT_STARTED) {
                throw new RejectedExecutionException("Task queue is shutdown");
            }
        }
        try {
            if (!mQueue.offer(task, timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new RejectedExecutionException("Unable to enqueue task after waiting " +
                                                     timeoutMillis + " milliseconds");
            }
        } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
        }
    }

    /**
     * Indicate that this task queue thread should finish running its enqueued
     * tasks and then exit. Enqueueing new tasks will result in a
     * RejectedExecutionException being thrown. Join on this thread to wait for
     * it to exit.
     */
    public synchronized void shutdown() {
        if (mState == STATE_STOPPED) {
            return;
        }
        if (mState == STATE_NOT_STARTED) {
            mState = STATE_STOPPED;
            return;
        }
        mState = STATE_SHOULD_STOP;
        // Inject stop task into the queue so it knows to stop, in case we're blocked.
        mQueue.offer(STOP_TASK);
    }

    @Override
    public void run() {
        synchronized (this) {
            if (mState == STATE_SHOULD_STOP || mState == STATE_STOPPED) {
                return;
            }
            if (mState == STATE_RUNNING) {
                throw new IllegalStateException("Already running");
            }
            mState = STATE_RUNNING;
        }

        try {
            while (true) {
                boolean isStopping;
                synchronized (this) {
                    isStopping = mState != STATE_RUNNING;
                }

                Runnable task;
                if (isStopping) {
                    // Poll the queue so this thread doesn't block when it
                    // should be stopping.
                    task = mQueue.poll();
                } else {
                    try {
                        task = mQueue.take();
                    } catch (InterruptedException e) {
                        break;
                    }
                }

                if (task == null || task == STOP_TASK) {
                    // Marker to indicate we should stop.
                    break;
                }

                try {
                    task.run();
                } catch (ThreadDeath e) {
                    throw e;
                } catch (Throwable e) {
                    try {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    } catch (Throwable e2) {
                        // If there is an exception reporting the exception, throw the original.
                        ThrowUnchecked.fire(e);
                    }
                }
            }
        } finally {
            synchronized (this) {
                mState = STATE_STOPPED;
            }
        }
    }
}
