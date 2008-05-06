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

package com.amazon.carbonado.raw;

import java.util.NoSuchElementException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.cursor.AbstractCursor;

/**
 * Abstract Cursor implementation for a repository that manipulates raw bytes.
 *
 * @author Brian S O'Neill
 */
public abstract class RawCursor<S> extends AbstractCursor<S> {
    // States for mState.
    private static final byte
        UNINITIALIZED = 0,
        CLOSED = 1,
        TRY_NEXT = 2,
        HAS_NEXT = 3;

    /** Lock object, as passed into the constructor */
    protected final Lock mLock;

    private final byte[] mStartBound;
    private final boolean mInclusiveStart;
    private final byte[] mEndBound;
    private final boolean mInclusiveEnd;
    private final int mPrefixLength;
    private final boolean mReverse;

    private byte mState;

    /**
     * @param lock operations lock on this object
     * @param startBound specify the starting key for the cursor, or null if first
     * @param inclusiveStart true if start bound is inclusive
     * @param endBound specify the ending key for the cursor, or null if last
     * @param inclusiveEnd true if end bound is inclusive
     * @param maxPrefix maximum expected common initial bytes in start and end bound
     * @param reverse when true, iteration is reversed
     * @throws IllegalArgumentException if any bound is null but is not inclusive
     */
    protected RawCursor(Lock lock,
                        byte[] startBound, boolean inclusiveStart,
                        byte[] endBound, boolean inclusiveEnd,
                        int maxPrefix,
                        boolean reverse) {
        mLock = lock == null ? new ReentrantLock() : lock;

        if ((startBound == null && !inclusiveStart) || (endBound == null && !inclusiveEnd)) {
            throw new IllegalArgumentException();
        }

        mStartBound = startBound;
        mInclusiveStart = inclusiveStart;
        mEndBound = endBound;
        mInclusiveEnd = inclusiveEnd;
        mReverse = reverse;

        // Determine common prefix for start and end bound.
        if (maxPrefix <= 0 || startBound == null && endBound == null) {
            mPrefixLength = 0;
        } else {
            int len = Math.min(maxPrefix, 
                               Math.min(startBound == null ? 0 : startBound.length, 
                                        endBound == null ? 0 : endBound.length));
            int i;
            for (i=0; i<len; i++) {
                if (startBound[i] != endBound[i]) {
                    break;
                }
            }
            mPrefixLength = i;
        }
    }

    public void close() throws FetchException {
        mLock.lock();
        try {
            if (mState != CLOSED) {
                release();
                // Switch state to closed before committing transaction, to
                // prevent infinite recursion that results when transaction
                // exits. Exiting a transaction causes all cursors to close.
                mState = CLOSED;
            }
        } finally {
            mLock.unlock();
        }
    }

    public boolean hasNext() throws FetchException {
        mLock.lock();
        try {
            try {
                switch (mState) {
                case UNINITIALIZED:
                    if (mReverse ? toBoundedLast() : toBoundedFirst()) {
                        mState = HAS_NEXT;
                        return true;
                    } else {
                        mState = TRY_NEXT;
                    }
                    break;

                case CLOSED: default:
                    return false;

                case TRY_NEXT:
                    if (mReverse ? toBoundedPrevious() : toBoundedNext()) {
                        mState = HAS_NEXT;
                        return true;
                    }
                    break;

                case HAS_NEXT:
                    return true;
                }
            } catch (FetchException e) {
                // Auto-close in response to FetchException.
                try {
                    close();
                } catch (FetchException e2) {
                    // Ignore.
                }
                throw e;
            }

            // Reached the end naturally, so close.
            close();
        } finally {
            mLock.unlock();
        }

        return false;
    }

    public S next() throws FetchException, NoSuchElementException {
        mLock.lock();
        try {
            if (!hasNext()) {
                handleNoSuchElement();
                throw new NoSuchElementException();
            }
            try {
                S obj = instantiateCurrent();
                mState = TRY_NEXT;
                return obj;
            } catch (FetchException e) {
                // Auto-close in response to FetchException.
                try {
                    close();
                } catch (FetchException e2) {
                    // Ignore.
                }
                throw e;
            }
        } finally {
            mLock.unlock();
        }
    }

    @Override
    public int skipNext(int amount) throws FetchException {
        if (amount <= 0) {
            if (amount < 0) {
                throw new IllegalArgumentException("Cannot skip negative amount: " + amount);
            }
            return 0;
        }

        mLock.lock();
        try {
            int actual = 0;

            if (hasNext()) {
                try {
                    actual += mReverse ? toBoundedPrevious(amount) : toBoundedNext(amount);
                } catch (FetchException e) {
                    // Auto-close in response to FetchException.
                    try {
                        close();
                    } catch (FetchException e2) {
                        // Ignore.
                    }
                    throw e;
                }

                if (actual >= amount) {
                    return actual;
                }
                mState = TRY_NEXT;
                // Since state was HAS_NEXT and is forced into TRY_NEXT, actual
                // amount skipped is effectively one more.
                actual++;
            }

            // Reached the end naturally, so close.
            close();

            return actual;
        } finally {
            mLock.unlock();
        }
    }

    /**
     * Release any internal resources, called when closed.
     */
    protected abstract void release() throws FetchException;

    /**
     * Returns the contents of the current key being referenced, or null
     * otherwise. Caller is responsible for making a copy of the key. The array
     * must not be modified concurrently.
     *
     * <p>If cursor is not opened, null must be returned.
     *
     * @return currently referenced key bytes or null if no current
     * @throws IllegalStateException if key is disabled
     */
    protected abstract byte[] getCurrentKey() throws FetchException;

    /**
     * Returns the contents of the current value being referenced, or null
     * otherwise. Caller is responsible for making a copy of the value. The
     * array must not be modified concurrently.
     *
     * <p>If cursor is not opened, null must be returned.
     *
     * @return currently referenced value bytes or null if no current
     * @throws IllegalStateException if value is disabled
     */
    protected abstract byte[] getCurrentValue() throws FetchException;

    /**
     * An optimization hint which disables key and value acquisition. The
     * default implementation of this method does nothing.
     */
    protected void disableKeyAndValue() {
    }

    /**
     * An optimization hint which disables just value acquisition. The default
     * implementation of this method does nothing.
     */
    protected void disableValue() {
    }

    /**
     * Enable key and value acquisition again, after they have been
     * disabled. Calling this method forces the key and value to be
     * re-acquired, if they had been disabled. Key and value acquisition must
     * be enabled by default. The default implementation of this method does
     * nothing.
     */
    protected void enableKeyAndValue() throws FetchException {
    }

    /**
     * Returns a new Storable instance for the currently referenced entry.
     *
     * @return new Storable instance, never null
     * @throws IllegalStateException if no current entry to instantiate
     */
    protected abstract S instantiateCurrent() throws FetchException;

    /**
     * Move the cursor to the first available entry. If false is returned, the
     * cursor must be positioned before the first available entry.
     *
     * @return true if first entry exists and is now current
     * @throws IllegalStateException if cursor is not opened
     */
    protected abstract boolean toFirst() throws FetchException;

    /**
     * Move the cursor to the first available entry at or after the given
     * key. If false is returned, the cursor must be positioned before the
     * first available entry. Caller is responsible for preserving contents of
     * array.
     *
     * @param key key to search for
     * @return true if first entry exists and is now current
     * @throws IllegalStateException if cursor is not opened
     */
    protected abstract boolean toFirst(byte[] key) throws FetchException;

    /**
     * Move the cursor to the last available entry. If false is returned, the
     * cursor must be positioned after the last available entry.
     *
     * @return true if last entry exists and is now current
     * @throws IllegalStateException if cursor is not opened
     */
    protected abstract boolean toLast() throws FetchException;

    /**
     * Move the cursor to the last available entry at or before the given
     * key. If false is returned, the cursor must be positioned after the last
     * available entry. Caller is responsible for preserving contents of array.
     *
     * @param key key to search for
     * @return true if last entry exists and is now current
     * @throws IllegalStateException if cursor is not opened
     */
    protected abstract boolean toLast(byte[] key) throws FetchException;

    /**
     * Move the cursor to the next available entry, returning false if none. If
     * false is returned, the cursor must be positioned after the last
     * available entry.
     *
     * @return true if moved to next entry
     * @throws IllegalStateException if cursor is not opened
     */
    protected abstract boolean toNext() throws FetchException;

    /**
     * Move the cursor to the next available entry, incrementing by the amount
     * given. The actual amount incremented is returned. If the amount is less
     * then requested, the cursor must be positioned after the last available
     * entry. Subclasses may wish to override this method with a faster
     * implementation.
     *
     * <p>Calling to toNext(1) is equivalent to calling toNext().
     *
     * @param amount positive amount to advance
     * @return actual amount advanced
     * @throws IllegalStateException if cursor is not opened
     */
    protected int toNext(int amount) throws FetchException {
        if (amount <= 1) {
            return (amount <= 0) ? 0 : (toNext() ? 1 : 0);
        }

        int count = 0;

        disableKeyAndValue();
        try {
            while (amount > 0) {
                if (toNext()) {
                    count++;
                    amount--;
                } else {
                    break;
                }
            }
        } finally {
            enableKeyAndValue();
        }

        return count;
    }

    /**
     * Move the cursor to the next unique key, returning false if none. If
     * false is returned, the cursor must be positioned after the last
     * available entry. Subclasses may wish to override this method with a
     * faster implementation.
     *
     * @return true if moved to next unique key
     * @throws IllegalStateException if cursor is not opened
     */
    protected boolean toNextKey() throws FetchException {
        byte[] initialKey = getCurrentKey();
        if (initialKey == null) {
            return false;
        }

        disableValue();
        try {
            while (true) {
                if (toNext()) {
                    byte[] currentKey = getCurrentKey();
                    if (currentKey == null) {
                        return false;
                    }
                    if (compareKeysPartially(currentKey, initialKey) > 0) {
                        break;
                    }
                } else {
                    return false;
                }
            }
        } finally {
            enableKeyAndValue();
        }

        return true;
    }

    /**
     * Move the cursor to the previous available entry, returning false if
     * none. If false is returned, the cursor must be positioned before the
     * first available entry.
     *
     * @return true if moved to previous entry
     * @throws IllegalStateException if cursor is not opened
     */
    protected abstract boolean toPrevious() throws FetchException;

    /**
     * Move the cursor to the previous available entry, decrementing by the
     * amount given. The actual amount decremented is returned. If the amount
     * is less then requested, the cursor must be positioned before the first
     * available entry. Subclasses may wish to override this method with a
     * faster implementation.
     *
     * <p>Calling to toPrevious(1) is equivalent to calling toPrevious().
     *
     * @param amount positive amount to retreat
     * @return actual amount retreated
     * @throws IllegalStateException if cursor is not opened
     */
    protected int toPrevious(int amount) throws FetchException {
        if (amount <= 1) {
            return (amount <= 0) ? 0 : (toPrevious() ? 1 : 0);
        }

        int count = 0;

        disableKeyAndValue();
        try {
            while (amount > 0) {
                if (toPrevious()) {
                    count++;
                    amount--;
                } else {
                    break;
                }
            }
        } finally {
            enableKeyAndValue();
        }

        return count;
    }

    /**
     * Move the cursor to the previous unique key, returning false if none. If
     * false is returned, the cursor must be positioned before the first
     * available entry. Subclasses may wish to override this method with a
     * faster implementation.
     *
     * @return true if moved to previous unique key
     * @throws IllegalStateException if cursor is not opened
     */
    protected boolean toPreviousKey() throws FetchException {
        byte[] initialKey = getCurrentKey();
        if (initialKey == null) {
            return false;
        }

        disableValue();
        try {
            while (true) {
                if (toPrevious()) {
                    byte[] currentKey = getCurrentKey();
                    if (currentKey == null) {
                        return false;
                    }
                    if (compareKeysPartially(getCurrentKey(), initialKey) < 0) {
                        break;
                    }
                } else {
                    return false;
                }
            }
        } finally {
            enableKeyAndValue();
        }

        return true;
    }

    /**
     * Returns {@literal <0} if key1 is less, 0 if equal (at least partially),
     * {@literal >0} if key1 is greater.
     */
    protected int compareKeysPartially(byte[] key1, byte[] key2) {
        int length = Math.min(key1.length, key2.length);
        for (int i=0; i<length; i++) {
            int a1 = key1[i];
            int a2 = key2[i];
            if (a1 != a2) {
                return (a1 & 0xff) - (a2 & 0xff);
            }
        }
        return 0;
    }

    /**
     * Called right before throwing NoSuchElementException. Subclasses may
     * override to do special checks or throw a different exception.
     */
    protected void handleNoSuchElement() throws FetchException {
    }

    private boolean prefixMatches() throws FetchException {
        int prefixLen = mPrefixLength;
        if (prefixLen > 0) {
            byte[] prefix = mStartBound;
            byte[] key = getCurrentKey();
            if (key == null) {
                return false;
            }
            for (int i=0; i<prefixLen; i++) {
                if (prefix[i] != key[i]) {
                    return false;
                }
            }
        }
        return true;
    }

    // Calls toFirst, but considers start and end bounds.
    private boolean toBoundedFirst() throws FetchException {
        if (mStartBound == null) {
            if (!toFirst()) {
                return false;
            }
        } else {
            if (!toFirst(mStartBound.clone())) {
                return false;
            }
            if (!mInclusiveStart) {
                byte[] currentKey = getCurrentKey();
                if (currentKey == null) {
                    return false;
                }
                if (compareKeysPartially(mStartBound, currentKey) == 0) {
                    if (!toNextKey()) {
                        return false;
                    }
                }
            }
        }

        if (mEndBound != null) {
            byte[] currentKey = getCurrentKey();
            if (currentKey == null) {
                return false;
            }
            int result = compareKeysPartially(currentKey, mEndBound);
            if (result >= 0) {
                if (result > 0 || !mInclusiveEnd) {
                    return false;
                }
            }
        }

        return prefixMatches();
    }

    // Calls toLast, but considers start and end bounds. Caller is responsible
    // for preserving key.
    private boolean toBoundedLast() throws FetchException {
        if (mEndBound == null) {
            if (!toLast()) {
                return false;
            }
        } else {
            if (!toLast(mEndBound.clone())) {
                return false;
            }
            if (!mInclusiveEnd) {
                byte[] currentKey = getCurrentKey();
                if (currentKey == null) {
                    return false;
                }
                if (compareKeysPartially(mEndBound, currentKey) == 0) {
                    if (!toPreviousKey()) {
                        return false;
                    }
                }
            }
        }

        if (mStartBound != null) {
            byte[] currentKey = getCurrentKey();
            if (currentKey == null) {
                return false;
            }
            int result = compareKeysPartially(currentKey, mStartBound);
            if (result <= 0) {
                if (result < 0 || !mInclusiveStart) {
                    return false;
                }
            }
        }

        return prefixMatches();
    }

    // Calls toNext, but considers end bound.
    private boolean toBoundedNext() throws FetchException {
        if (!toNext()) {
            return false;
        }

        if (mEndBound != null) {
            byte[] currentKey = getCurrentKey();
            if (currentKey == null) {
                return false;
            }
            int result = compareKeysPartially(currentKey, mEndBound);
            if (result >= 0) {
                if (result > 0 || !mInclusiveEnd) {
                    return false;
                }
            }
        }

        return prefixMatches();
    }

    // Calls toNext, but considers end bound.
    private int toBoundedNext(int amount) throws FetchException {
        if (mEndBound == null) {
            return toNext(amount);
        }

        int count = 0;

        disableValue();
        try {
            while (amount > 0) {
                if (!toNext()) {
                    break;
                }

                byte[] currentKey = getCurrentKey();
                if (currentKey == null) {
                    break;
                }

                int result = compareKeysPartially(currentKey, mEndBound);
                if (result >= 0) {
                    if (result > 0 || !mInclusiveEnd) {
                        break;
                    }
                }

                if (!prefixMatches()) {
                    break;
                }

                count++;
                amount--;
            }
        } finally {
            enableKeyAndValue();
        }

        return count;
    }

    // Calls toPrevious, but considers start bound.
    private boolean toBoundedPrevious() throws FetchException {
        if (!toPrevious()) {
            return false;
        }

        if (mStartBound != null) {
            byte[] currentKey = getCurrentKey();
            if (currentKey == null) {
                return false;
            }
            int result = compareKeysPartially(currentKey, mStartBound);
            if (result <= 0) {
                if (result < 0 || !mInclusiveStart) {
                    // Too far now, reset to first.
                    toBoundedFirst();
                    return false;
                }
            }
        }

        return prefixMatches();
    }

    // Calls toPrevious, but considers start bound.
    private int toBoundedPrevious(int amount) throws FetchException {
        if (mStartBound == null) {
            return toPrevious(amount);
        }

        int count = 0;

        disableValue();
        try {
            while (amount > 0) {
                if (!toPrevious()) {
                    break;
                }

                byte[] currentKey = getCurrentKey();
                if (currentKey == null) {
                    break;
                }

                int result = compareKeysPartially(currentKey, mStartBound);
                if (result <= 0) {
                    if (result < 0 || !mInclusiveStart) {
                        // Too far now, reset to first.
                        toBoundedFirst();
                        break;
                    }
                }

                if (!prefixMatches()) {
                    break;
                }

                count++;
                amount--;
            }
        } finally {
            enableKeyAndValue();
        }

        return count;
    }
}
