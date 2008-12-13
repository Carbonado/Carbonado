/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.map;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import java.util.concurrent.locks.LockSupport;

/**
 * Partially reentrant, upgradable read/write lock. Up to 1,073,741,824 read
 * locks can be held. Upgraders and writers may re-enter the lock up to
 * 2,147,483,648 times. Attempts by readers to re-enter the lock is not
 * detected and is deadlock prone, unless locker already holds an upgrade or
 * write lock. Subclasses can support full reentrancy by overriding the
 * protected read lock adjust and hold check methods.
 *
 * <p>This lock implementation differs from the usual Java lock with respect to
 * lock ownership. Locks are not owned by threads, but by arbitrary locker
 * objects. A thread which attempts to acquire an upgrade or write lock twice
 * with different locker objects will deadlock on the second attempt.
 *
 * <p>As is typical of read/write lock implementations, a read lock blocks
 * waiting writers, but it doesn't block other readers. A write lock is
 * exclusive and can be held by at most one locker. Attempting to acquire a
 * write lock while a read lock is held by the same locker is inherently
 * deadlock prone, and some read/write lock implementations will always
 * deadlock.
 *
 * <p>An upgrade lock allows a read lock to be safely upgraded to a write
 * lock. Instead of acquiring a read lock, an upgrade lock is acquired. This
 * acts like a shared read lock in that readers are not blocked, but it also
 * acts like an exclusive write lock -- writers and upgraders are blocked. With
 * an upgrade lock held, the locker may acquire a write lock without deadlock.
 *
 * <pre>
 * Locks held    Locks safely           Locks acquirable
 * by owner      acquirable by owner    by other lockers
 * --------------------------------------------------
 *   - - -         R U W                  R U W
 *   R - -         - - -                  R U -
 *   R U -         - U -                  R - -
 *   - U -         R U W                  R - -
 *   - U W         R U W                  - - -
 *   R U W         R U W                  - - -
 *   R - W         R U W                  - - -
 *   - - W         R U W                  - - -
 * </pre>
 *
 * @author Brian S O'Neill
 * @param <L> Locker type
 */
class UpgradableLock<L> {
    // Design note: This class borrows heavily from AbstractQueuedSynchronizer.
    // Consult that class for understanding the locking mechanics.

    private static enum Result {
        /** Lock acquisition failed */
        FAILED,
        /** Lock has just been acquired by locker and can be safely released later */
        ACQUIRED,
        /** Lock is already owned by locker and should not be released more than once */
        OWNED
    }

    private static final AtomicReferenceFieldUpdater<UpgradableLock, Node> cRWHeadRef =
        AtomicReferenceFieldUpdater.newUpdater
        (UpgradableLock.class, Node.class, "mRWHead");

    private static final AtomicReferenceFieldUpdater<UpgradableLock, Node> cRWTailRef =
        AtomicReferenceFieldUpdater.newUpdater
        (UpgradableLock.class, Node.class, "mRWTail");

    private static final AtomicReferenceFieldUpdater<UpgradableLock, Node> cUHeadRef =
        AtomicReferenceFieldUpdater.newUpdater
        (UpgradableLock.class, Node.class, "mUHead");

    private static final AtomicReferenceFieldUpdater<UpgradableLock, Node> cUTailRef =
        AtomicReferenceFieldUpdater.newUpdater
        (UpgradableLock.class, Node.class, "mUTail");

    private static final AtomicIntegerFieldUpdater<UpgradableLock> cStateRef =
        AtomicIntegerFieldUpdater.newUpdater
        (UpgradableLock.class, "mState");

    // State mask bits for held locks. Read lock count is stored in lower 30 bits of state.
    private static final int LOCK_STATE_UPGRADE = 0x40000000;
    // Write state must be this value in order for quick sign check to work.
    private static final int LOCK_STATE_WRITE = 0x80000000;

    private static final int LOCK_STATE_MASK = LOCK_STATE_UPGRADE | LOCK_STATE_WRITE;

    /**
     * The number of nanoseconds for which it is faster to spin
     * rather than to use timed park. A rough estimate suffices
     * to improve responsiveness with very short timeouts.
     */
    private static final long SPIN_FOR_TIMEOUT_THRESHOLD = 1000L;

    // Head of read-write queue.
    private transient volatile Node mRWHead;

    // Tail of read-write queue.
    private transient volatile Node mRWTail;

    // Head of write upgrade queue.
    private transient volatile Node mUHead;

    // Tail of write upgrade queue.
    private transient volatile Node mUTail;

    private transient volatile int mState;

    // Owner holds an upgradable lock and possibly a write lock too.
    private transient L mOwner;

    // Counts number of times that owner has entered an upgradable or write lock.
    private transient int mUpgradeCount;
    private transient int mWriteCount;

    public UpgradableLock() {
    }

    /**
     * Acquire a shared read lock, possibly blocking indefinitely.
     *
     * @param locker object which might be write or upgrade lock owner
     */
    public final void lockForRead(L locker) {
        if (!tryLockForRead(locker)) {
            lockForReadQueued(locker, addReadWaiter());
        }
    }

    /**
     * Acquire a shared read lock, possibly blocking until interrupted.
     *
     * @param locker object which might be write or upgrade lock owner
     */
    public final void lockForReadInterruptibly(L locker) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        if (!tryLockForRead(locker)) {
            lockForReadQueuedInterruptibly(locker, addReadWaiter());
        }
    }

    /**
     * Attempt to immediately acquire a shared read lock.
     *
     * @param locker object which might be write or upgrade lock owner
     * @return true if acquired
     */
    public final boolean tryLockForRead(L locker) {
        int state = mState;
        if (state >= 0) { // no write lock is held
            if (isReadWriteFirst() || isReadLockHeld(locker)) {
                do {
                    if (incrementReadLocks(state)) {
                        adjustReadLockCount(locker, 1);
                        return true;
                    }
                    // keep looping on CAS failure if a reader or upgrader mucked with the state
                } while ((state = mState) >= 0);
            }
        } else if (mOwner == locker) {
            // keep looping on CAS failure if a reader or upgrader mucked with the state
            while (!incrementReadLocks(state)) {
                state = mState;
            }
            adjustReadLockCount(locker, 1);
            return true;
        }
        return false;
    }

    /**
     * Attempt to acquire a shared read lock, waiting a maximum amount of
     * time.
     *
     * @param locker object which might be write or upgrade lock owner
     * @return true if acquired
     */
    public final boolean tryLockForRead(L locker, long timeout, TimeUnit unit)
        throws InterruptedException
    {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        if (!tryLockForRead(locker)) {
            return lockForReadQueuedInterruptibly(locker, addReadWaiter(), unit.toNanos(timeout));
        }
        return true;
    }

    /**
     * Release a previously acquired read lock.
     */
    public final void unlockFromRead(L locker) {
        adjustReadLockCount(locker, -1);

        int readLocks;
        while ((readLocks = decrementReadLocks(mState)) < 0) {}

        if (readLocks == 0) {
            Node h = mRWHead;
            if (h != null && h.mWaitStatus != 0) {
                unparkReadWriteSuccessor(h);
            }
        }
    }

    /**
     * Acquire an upgrade lock, possibly blocking indefinitely.
     *
     * @param locker object trying to become lock owner
     * @return true if acquired
     */
    public final boolean lockForUpgrade(L locker) {
        return lockForUpgrade_(locker) != Result.FAILED;
    }

    /**
     * Acquire an upgrade lock, possibly blocking indefinitely.
     *
     * @param locker object trying to become lock owner
     * @return ACQUIRED or OWNED
     */
    private final Result lockForUpgrade_(L locker) {
        Result result;
        if ((result = tryLockForUpgrade_(locker)) == Result.FAILED) {
            result = lockForUpgradeQueued(locker, addUpgradeWaiter());
        }
        return result;
    }

    /**
     * Acquire an upgrade lock, possibly blocking until interrupted.
     *
     * @param locker object trying to become lock owner
     * @return true if acquired
     */
    public final boolean lockForUpgradeInterruptibly(L locker) throws InterruptedException {
        return lockForUpgradeInterruptibly_(locker) != Result.FAILED;
    }

    /**
     * Acquire an upgrade lock, possibly blocking until interrupted.
     *
     * @param locker object trying to become lock owner
     * @return ACQUIRED or OWNED
     */
    private final Result lockForUpgradeInterruptibly_(L locker) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Result result;
        if ((result = tryLockForUpgrade_(locker)) == Result.FAILED) {
            result = lockForUpgradeQueuedInterruptibly(locker, addUpgradeWaiter());
        }
        return result;
    }

    /**
     * Attempt to immediately acquire an upgrade lock.
     *
     * @param locker object trying to become lock owner
     * @return true if acquired
     */
    public final boolean tryLockForUpgrade(L locker) {
        return tryLockForUpgrade_(locker) != Result.FAILED;
    }

    /**
     * Attempt to immediately acquire an upgrade lock.
     *
     * @param locker object trying to become lock owner
     * @return FAILED, ACQUIRED or OWNED
     */
    private final Result tryLockForUpgrade_(L locker) {
        int state = mState;
        if ((state & LOCK_STATE_MASK) == 0) { // no write or upgrade lock is held
            if (isUpgradeFirst()) {
                do {
                    if (setUpgradeLock(state)) {
                        mOwner = locker;
                        incrementUpgradeCount();
                        return Result.ACQUIRED;
                    }
                    // keep looping on CAS failure if a reader mucked with the state
                } while (((state = mState) & LOCK_STATE_MASK) == 0);
            }
        } else if (mOwner == locker) {
            incrementUpgradeCount();
            return Result.OWNED;
        }
        return Result.FAILED;
    }

    /**
     * Attempt to acquire an upgrade lock, waiting a maximum amount of time.
     *
     * @param locker object trying to become lock owner
     * @return true if acquired
     */
    public final boolean tryLockForUpgrade(L locker, long timeout, TimeUnit unit)
        throws InterruptedException
    {
        return tryLockForUpgrade_(locker, timeout, unit) != Result.FAILED;
    }

    /**
     * Attempt to acquire an upgrade lock, waiting a maximum amount of time.
     *
     * @param locker object trying to become lock owner
     * @return FAILED, ACQUIRED or OWNED
     */
    private final Result tryLockForUpgrade_(L locker, long timeout, TimeUnit unit)
        throws InterruptedException
    {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        Result result;
        if ((result = tryLockForUpgrade_(locker)) == Result.FAILED) {
            result = lockForUpgradeQueuedInterruptibly(locker, addUpgradeWaiter(),
                                                       unit.toNanos(timeout));
        }
        return result;
    }

    /**
     * Release a previously acquired upgrade lock.
     */
    public final void unlockFromUpgrade(L locker) {
        int upgradeCount = mUpgradeCount - 1;
        if (upgradeCount < 0) {
            throw new IllegalMonitorStateException("Too many upgrade locks released");
        }
        if (upgradeCount == 0 && mWriteCount > 0) {
            // Don't release last upgrade lock and switch write lock to
            // automatic upgrade mode.
            clearUpgradeLock(mState);
            return;
        }
        mUpgradeCount = upgradeCount;
        if (upgradeCount > 0) {
            return;
        }

        mOwner = null;

        // keep looping on CAS failure if reader mucked with state
        while (!clearUpgradeLock(mState)) {}

        Node h = mUHead;
        if (h != null && h.mWaitStatus != 0) {
            unparkUpgradeSuccessor(h);
        }
    }

    /**
     * Acquire an exclusive write lock, possibly blocking indefinitely.
     *
     * @param locker object trying to become lock owner
     */
    public final void lockForWrite(L locker) {
        if (!tryLockForWrite(locker)) {
            Result upgradeResult = lockForUpgrade_(locker);
            if (!tryLockForWrite(locker)) {
                lockForWriteQueued(locker, addWriteWaiter());
            }
            if (upgradeResult == Result.ACQUIRED) {
                // clear upgrade state bit to indicate automatic upgrade
                while (!clearUpgradeLock(mState)) {}
            } else {
                // undo automatic upgrade count increment
                mUpgradeCount--;
            }
        }
    }

    /**
     * Acquire an exclusive write lock, possibly blocking until interrupted.
     *
     * @param locker object trying to become lock owner
     */
    public final void lockForWriteInterruptibly(L locker) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        if (!tryLockForWrite(locker)) {
            Result upgradeResult = lockForUpgradeInterruptibly_(locker);
            if (!tryLockForWrite(locker)) {
                lockForWriteQueuedInterruptibly(locker, addWriteWaiter());
            }
            if (upgradeResult == Result.ACQUIRED) {
                // clear upgrade state bit to indicate automatic upgrade
                while (!clearUpgradeLock(mState)) {}
            } else {
                // undo automatic upgrade count increment
                mUpgradeCount--;
            }
        }
    }

    /**
     * Attempt to immediately acquire an exclusive lock.
     *
     * @param locker object trying to become lock owner
     * @return true if acquired
     */
    public final boolean tryLockForWrite(L locker) {
        int state = mState;
        if (state == 0) {
            // no locks are held
            if (isUpgradeOrReadWriteFirst() && setWriteLock(state)) {
                // keep upgrade state bit clear to indicate automatic upgrade
                mOwner = locker;
                incrementUpgradeCount();
                incrementWriteCount();
                return true;
            }
        } else if (state == LOCK_STATE_UPGRADE) {
            // only upgrade lock is held; upgrade to full write lock
            if (mOwner == locker && setWriteLock(state)) {
                incrementWriteCount();
                return true;
            }
        } else if (state < 0) {
            // write lock is held, and upgrade lock might be held too
            if (mOwner == locker) {
                incrementWriteCount();
                return true;
            }
        }
        return false;
    }

    /**
     * Attempt to acquire an exclusive lock, waiting a maximum amount of time.
     *
     * @param locker object trying to become lock owner
     * @return true if acquired
     */
    public final boolean tryLockForWrite(L locker, long timeout, TimeUnit unit)
        throws InterruptedException
    {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        if (!tryLockForWrite(locker)) {
            long start = System.nanoTime();
            Result upgradeResult = tryLockForUpgrade_(locker, timeout, unit);
            if (upgradeResult == Result.FAILED) {
                return false;
            }
            if (!tryLockForWrite(locker)) {
                unlockFromUpgrade(locker);
                if ((timeout = unit.toNanos(timeout) - (System.nanoTime() - start)) <= 0) {
                    return false;
                }
                if (!lockForWriteQueuedInterruptibly(locker, addWriteWaiter(), timeout)) {
                    return false;
                }
            }
            if (upgradeResult == Result.ACQUIRED) {
                // clear upgrade state bit to indicate automatic upgrade
                while (!clearUpgradeLock(mState)) {}
            } else {
                // undo automatic upgrade count increment
                mUpgradeCount--;
            }
        }
        return true;
    }

    /**
     * Release a previously acquired write lock.
     */
    public final void unlockFromWrite(L locker) {
        int writeCount = mWriteCount - 1;
        if (writeCount < 0) {
            throw new IllegalMonitorStateException("Too many write locks released");
        }
        mWriteCount = writeCount;
        if (writeCount > 0) {
            return;
        }

        // copy original state to check if upgrade lock was automatic
        final int state = mState;

        // make sure upgrade lock is still held after releasing write lock
        mState = LOCK_STATE_UPGRADE;

        Node h = mRWHead;
        if (h != null && h.mWaitStatus != 0) {
            unparkReadWriteSuccessor(h);
        }

        if (state == LOCK_STATE_WRITE) {
            // upgrade owner was automatically set, so automatically clear it
            unlockFromUpgrade(locker);
        }
    }

    @Override
    public String toString() {
        int state = mState;
        int readLocks = state & ~LOCK_STATE_MASK;
        int upgradeLocks = mUpgradeCount;
        int writeLocks = mWriteCount;

        return super.toString()
            + "[Read locks = " + readLocks
            + ", Upgrade locks = " + upgradeLocks
            + ", Write locks = " + writeLocks
            + ", Owner = " + mOwner
            + ']';
    }

    /**
     * Add or subtract to the count of read locks held for the given
     * locker. Default implementation does nothing, and so read locks are not
     * reentrant.
     *
     * @throws IllegalMonitorStateException if count overflows or underflows
     */
    protected void adjustReadLockCount(L locker, int amount) {
    }

    /**
     * Default implementation does nothing and always returns false, and so
     * read locks are not reentrant. Overridden implementation may choose to
     * always returns true, in which case read lock requests can starve upgrade
     * and write lock requests.
     */
    protected boolean isReadLockHeld(L locker) {
        return false;
    }

    private Node enqForReadWrite(final Node node) {
        for (;;) {
            Node t = mRWTail;
            if (t == null) { // Must initialize
                Node h = new Node(); // Dummy header
                h.mNext = node;
                node.mPrev = h;
                if (cRWHeadRef.compareAndSet(this, null, h)) {
                    mRWTail = node;
                    return h;
                }
            } else {
                node.mPrev = t;
                if (cRWTailRef.compareAndSet(this, t, node)) {
                    t.mNext = node;
                    return t;
                }
            }
        }
    }

    private Node enqForUpgrade(final Node node) {
        for (;;) {
            Node t = mUTail;
            if (t == null) { // Must initialize
                Node h = new Node(); // Dummy header
                h.mNext = node;
                node.mPrev = h;
                if (cUHeadRef.compareAndSet(this, null, h)) {
                    mUTail = node;
                    return h;
                }
            } else {
                node.mPrev = t;
                if (cUTailRef.compareAndSet(this, t, node)) {
                    t.mNext = node;
                    return t;
                }
            }
        }
    }

    private Node addReadWaiter() {
        return addReadWriteWaiter(true);
    }

    private Node addWriteWaiter() {
        return addReadWriteWaiter(false);
    }

    private Node addReadWriteWaiter(boolean shared) {
        Node node = new Node(Thread.currentThread(), shared);
        // Try the fast path of enq; backup to full enq on failure
        Node pred = mRWTail;
        if (pred != null) {
            node.mPrev = pred;
            if (cRWTailRef.compareAndSet(this, pred, node)) {
                pred.mNext = node;
                return node;
            }
        }
        enqForReadWrite(node);
        return node;
    }

    private Node addUpgradeWaiter() {
        Node node = new Node(Thread.currentThread(), false);
        // Try the fast path of enq; backup to full enq on failure
        Node pred = mUTail;
        if (pred != null) {
            node.mPrev = pred;
            if (cUTailRef.compareAndSet(this, pred, node)) {
                pred.mNext = node;
                return node;
            }
        }
        enqForUpgrade(node);
        return node;
    }

    private void setReadWriteHead(Node node) {
        mRWHead = node;
        node.mThread = null;
        node.mPrev = null;
    }

    private void setUpgradeHead(Node node) {
        mUHead = node;
        node.mThread = null;
        node.mPrev = null;
    }

    private void unparkReadWriteSuccessor(Node node) {
        Node.cWaitStatusRef.compareAndSet(node, Node.SIGNAL, 0);

        Node s = node.mNext;
        if (s == null || s.mWaitStatus > 0) {
            s = null;
            for (Node t = mRWTail; t != null && t != node; t = t.mPrev) {
                if (t.mWaitStatus <= 0) {
                    s = t;
                }
            }
        }
        if (s != null) {
            LockSupport.unpark(s.mThread);
        }
    }

    private void unparkUpgradeSuccessor(Node node) {
        Node.cWaitStatusRef.compareAndSet(node, Node.SIGNAL, 0);

        Node s = node.mNext;
        if (s == null || s.mWaitStatus > 0) {
            s = null;
            for (Node t = mUTail; t != null && t != node; t = t.mPrev) {
                if (t.mWaitStatus <= 0) {
                    s = t;
                }
            }
        }
        if (s != null) {
            LockSupport.unpark(s.mThread);
        }
    }

    private void setReadWriteHeadAndPropagate(Node node) {
        setReadWriteHead(node);
        if (node.mWaitStatus != 0) {
            Node s = node.mNext;
            if (s == null || s.mShared) {
                unparkReadWriteSuccessor(node);
            }
        }
    }

    private void cancelAcquireReadWrite(Node node) {
        if (node != null) {
            node.mThread = null;
            node.mWaitStatus = Node.CANCELLED;
            unparkReadWriteSuccessor(node);
        }
    }

    private void cancelAcquireUpgrade(Node node) {
        if (node != null) {
            node.mThread = null;
            node.mWaitStatus = Node.CANCELLED;
            unparkUpgradeSuccessor(node);
        }
    }

    private static boolean shouldParkAfterFailedAcquire(Node pred, Node node) {
        int s = pred.mWaitStatus;
        if (s < 0) {
            return true;
        }
        if (s > 0) {
            node.mPrev = pred.mPrev;
        } else {
            Node.cWaitStatusRef.compareAndSet(pred, 0, Node.SIGNAL);
        }
        return false;
    }

    private static void selfInterrupt() {
        Thread.currentThread().interrupt();
    }

    private final boolean parkAndCheckInterrupt() {
        LockSupport.park(this);
        return Thread.interrupted();
    }

    /**
     * @return ACQUIRED or OWNED
     */
    private final Result lockForUpgradeQueued(L locker, final Node node) {
        try {
            boolean interrupted = false;
            for (;;) {
                final Node p = node.predecessor();
                Result result;
                if (p == mUHead && (result = tryLockForUpgrade_(locker)) != Result.FAILED) {
                    setUpgradeHead(node);
                    p.mNext = null; // help GC
                    if (interrupted) {
                        selfInterrupt();
                    }
                    return result;
                }
                if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt()) {
                    interrupted = true;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireUpgrade(node);
            throw e;
        }
    }

    /**
     * @return ACQUIRED or OWNED
     */
    private final Result lockForUpgradeQueuedInterruptibly(L locker, final Node node)
        throws InterruptedException
    {
        try {
            for (;;) {
                final Node p = node.predecessor();
                Result result;
                if (p == mUHead && (result = tryLockForUpgrade_(locker)) != Result.FAILED) {
                    setUpgradeHead(node);
                    p.mNext = null; // help GC
                    return result;
                }
                if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireUpgrade(node);
            throw e;
        }
        // Arrive here only if interrupted
        cancelAcquireUpgrade(node);
        throw new InterruptedException();
    }

    /**
     * @return FAILED, ACQUIRED or OWNED
     */
    private final Result lockForUpgradeQueuedInterruptibly(L locker, final Node node,
                                                           long nanosTimeout)
        throws InterruptedException
    {
        long lastTime = System.nanoTime();
        try {
            for (;;) {
                final Node p = node.predecessor();
                Result result;
                if (p == mUHead && (result = tryLockForUpgrade_(locker)) != Result.FAILED) {
                    setUpgradeHead(node);
                    p.mNext = null; // help GC
                    return result;
                }
                if (nanosTimeout <= 0) {
                    cancelAcquireUpgrade(node);
                    return Result.FAILED;
                }
                if (nanosTimeout > SPIN_FOR_TIMEOUT_THRESHOLD
                    && shouldParkAfterFailedAcquire(p, node))
                {
                    LockSupport.parkNanos(this, nanosTimeout);
                }
                long now = System.nanoTime();
                nanosTimeout -= now - lastTime;
                lastTime = now;
                if (Thread.interrupted()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireUpgrade(node);
            throw e;
        }
        // Arrive here only if interrupted
        cancelAcquireUpgrade(node);
        throw new InterruptedException();
    }

    private final void lockForReadQueued(L locker, final Node node) {
        try {
            boolean interrupted = false;
            for (;;) {
                final Node p = node.predecessor();
                if (p == mRWHead && tryLockForRead(locker)) {
                    setReadWriteHeadAndPropagate(node);
                    p.mNext = null; // help GC
                    if (interrupted) {
                        selfInterrupt();
                    }
                    return;
                }
                if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt()) {
                    interrupted = true;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireReadWrite(node);
            throw e;
        }
    }

    private final void lockForReadQueuedInterruptibly(L locker, final Node node)
        throws InterruptedException
    {
        try {
            for (;;) {
                final Node p = node.predecessor();
                if (p == mRWHead && tryLockForRead(locker)) {
                    setReadWriteHeadAndPropagate(node);
                    p.mNext = null; // help GC
                    return;
                }
                if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireReadWrite(node);
            throw e;
        }
        // Arrive here only if interrupted
        cancelAcquireReadWrite(node);
        throw new InterruptedException();
    }

    /**
     * @return true if acquired
     */
    private final boolean lockForReadQueuedInterruptibly(L locker, final Node node,
                                                         long nanosTimeout)
        throws InterruptedException
    {
        long lastTime = System.nanoTime();
        try {
            for (;;) {
                final Node p = node.predecessor();
                if (p == mRWHead && tryLockForRead(locker)) {
                    setReadWriteHeadAndPropagate(node);
                    p.mNext = null; // help GC
                    return true;
                }
                if (nanosTimeout <= 0) {
                    cancelAcquireReadWrite(node);
                    return false;
                }
                if (nanosTimeout > SPIN_FOR_TIMEOUT_THRESHOLD
                    && shouldParkAfterFailedAcquire(p, node))
                {
                    LockSupport.parkNanos(this, nanosTimeout);
                }
                long now = System.nanoTime();
                nanosTimeout -= now - lastTime;
                lastTime = now;
                if (Thread.interrupted()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireReadWrite(node);
            throw e;
        }
        // Arrive here only if interrupted
        cancelAcquireReadWrite(node);
        throw new InterruptedException();
    }

    private final void lockForWriteQueued(L locker, final Node node) {
        try {
            boolean interrupted = false;
            for (;;) {
                final Node p = node.predecessor();
                if (p == mRWHead && tryLockForWrite(locker)) {
                    setReadWriteHead(node);
                    p.mNext = null; // help GC
                    if (interrupted) {
                        selfInterrupt();
                    }
                    return;
                }
                if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt()) {
                    interrupted = true;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireReadWrite(node);
            throw e;
        }
    }

    private final void lockForWriteQueuedInterruptibly(L locker, final Node node)
        throws InterruptedException
    {
        try {
            for (;;) {
                final Node p = node.predecessor();
                if (p == mRWHead && tryLockForWrite(locker)) {
                    setReadWriteHead(node);
                    p.mNext = null; // help GC
                    return;
                }
                if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireReadWrite(node);
            throw e;
        }
        // Arrive here only if interrupted
        cancelAcquireReadWrite(node);
        throw new InterruptedException();
    }

    /**
     * @return true if acquired
     */
    private final boolean lockForWriteQueuedInterruptibly(L locker, final Node node,
                                                          long nanosTimeout)
        throws InterruptedException
    {
        long lastTime = System.nanoTime();
        try {
            for (;;) {
                final Node p = node.predecessor();
                if (p == mRWHead && tryLockForWrite(locker)) {
                    setReadWriteHead(node);
                    p.mNext = null; // help GC
                    return true;
                }
                if (nanosTimeout <= 0) {
                    cancelAcquireReadWrite(node);
                    return false;
                }
                if (nanosTimeout > SPIN_FOR_TIMEOUT_THRESHOLD
                    && shouldParkAfterFailedAcquire(p, node))
                {
                    LockSupport.parkNanos(this, nanosTimeout);
                }
                long now = System.nanoTime();
                nanosTimeout -= now - lastTime;
                lastTime = now;
                if (Thread.interrupted()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            cancelAcquireReadWrite(node);
            throw e;
        }
        // Arrive here only if interrupted
        cancelAcquireReadWrite(node);
        throw new InterruptedException();
    }

    private final boolean isReadWriteFirst() {
        Node h;
        if ((h = mRWHead) == null) {
            return true;
        }
        Thread current = Thread.currentThread();
        Node s;
        return ((s = h.mNext) != null && s.mThread == current) || fullIsReadWriteFirst(current);
    }

    private final boolean fullIsReadWriteFirst(Thread current) {
        Node h, s;
        Thread firstThread = null;
        if (((h = mRWHead) != null && (s = h.mNext) != null &&
             s.mPrev == mRWHead && (firstThread = s.mThread) != null))
        {
            return firstThread == current;
        }
        Node t = mRWTail;
        while (t != null && t != mRWHead) {
            Thread tt = t.mThread;
            if (tt != null) {
                firstThread = tt;
            }
            t = t.mPrev;
        }
        return firstThread == current || firstThread == null;
    }

    private final boolean isUpgradeFirst() {
        Node h;
        if ((h = mUHead) == null) {
            return true;
        }
        Thread current = Thread.currentThread();
        Node s;
        return ((s = h.mNext) != null && s.mThread == current) || fullIsUpgradeFirst(current);
    }

    private final boolean fullIsUpgradeFirst(Thread current) {
        Node h, s;
        Thread firstThread = null;
        if (((h = mUHead) != null && (s = h.mNext) != null &&
             s.mPrev == mUHead && (firstThread = s.mThread) != null))
        {
            return firstThread == current;
        }
        Node t = mUTail;
        while (t != null && t != mUHead) {
            Thread tt = t.mThread;
            if (tt != null) {
                firstThread = tt;
            }
            t = t.mPrev;
        }
        return firstThread == current || firstThread == null;
    }

    private final boolean isUpgradeOrReadWriteFirst() {
        Node uh, rwh;
        if ((uh = mUHead) == null || (rwh = mRWHead) == null) {
            return true;
        }
        Thread current = Thread.currentThread();
        Node us, rws;
        return ((us = uh.mNext) != null && us.mThread == current)
            || ((rws = rwh.mNext) != null && rws.mThread == current)
            || fullIsUpgradeFirst(current)
            || fullIsReadWriteFirst(current);
    }

    /**
     * @return false if state changed
     */
    private boolean incrementReadLocks(int state) {
        int readLocks = (state & ~LOCK_STATE_MASK) + 1;
        if (readLocks == LOCK_STATE_MASK) {
            throw new IllegalMonitorStateException("Maximum read lock count exceeded");
        }
        return cStateRef.compareAndSet(this, state, state & LOCK_STATE_MASK | readLocks);
    }

    /**
     * @return number of remaining read locks or negative if concurrent
     * modification prevented operation
     */
    private int decrementReadLocks(int state) {
        int readLocks = (state & ~LOCK_STATE_MASK) - 1;
        if (readLocks < 0) {
            throw new IllegalMonitorStateException("Too many read locks released");
        }
        if (cStateRef.compareAndSet(this, state, state & LOCK_STATE_MASK | readLocks)) {
            return readLocks;
        }
        return -1;
    }

    /**
     * @return false if concurrent modification prevented operation
     */
    private boolean setUpgradeLock(int state) {
        return cStateRef.compareAndSet(this, state, state | LOCK_STATE_UPGRADE);
    }

    /**
     * @return false if concurrent modification prevented operation
     */
    private boolean clearUpgradeLock(int state) {
        return cStateRef.compareAndSet(this, state, state & ~LOCK_STATE_UPGRADE);
    }

    private void incrementUpgradeCount() {
        int upgradeCount = mUpgradeCount + 1;
        if (upgradeCount < 0) {
            throw new IllegalMonitorStateException("Maximum upgrade lock count exceeded");
        }
        mUpgradeCount = upgradeCount;
    }

    /**
     * @return false if concurrent modification prevented operation
     */
    private boolean setWriteLock(int state) {
        return cStateRef.compareAndSet(this, state, state | LOCK_STATE_WRITE);
    }

    private void incrementWriteCount() {
        int writeCount = mWriteCount + 1;
        if (writeCount < 0) {
            throw new IllegalMonitorStateException("Maximum write lock count exceeded");
        }
        mWriteCount = writeCount;
    }

    /**
     * Used by unit tests.
     */
    boolean noLocksHeld() {
        return mState == 0 && mOwner == null && mUpgradeCount == 0 && mWriteCount == 0;
    }

    /**
     * Node class ripped off from AbstractQueuedSynchronizer and modified
     * slightly. Read the comments in that class for better understanding.
     */
    static final class Node {
        static final AtomicIntegerFieldUpdater<Node> cWaitStatusRef =
            AtomicIntegerFieldUpdater.newUpdater(Node.class, "mWaitStatus");

        static final int CANCELLED =  1;
        static final int SIGNAL    = -1;

        volatile int mWaitStatus;
        volatile Node mPrev;
        volatile Node mNext;
        volatile Thread mThread;

        final boolean mShared;

        // Used to establish initial head
        Node() {
            mShared = false;
        }

        Node(Thread thread, boolean shared) {
            mThread = thread;
            mShared = shared;
        }

        final Node predecessor() throws NullPointerException {
            Node p = mPrev;
            if (p == null) {
                throw new NullPointerException();
            } else {
                return p;
            }
        }
    }
}
