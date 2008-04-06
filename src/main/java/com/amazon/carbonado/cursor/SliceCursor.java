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

package com.amazon.carbonado.cursor;

import java.util.NoSuchElementException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;

/**
 * Wraps another cursor and only produces a range of elements. The actual range
 * might be smaller if the source cursor doesn't have enough elements.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public abstract class SliceCursor<S> extends AbstractCursor<S> {
    /**
     * @param from zero-based element to start from, inclusive
     * @throws IllegalArgumentException if source is null or from is negative
     */
    public static <S> Cursor<S> slice(Cursor<S> source, long from) {
        if (source == null) {
            throw new IllegalArgumentException("Source is null");
        }
        if (from >= 0) {
            return from == 0 ? source : new Skip<S>(source, from);
        } else {
            throw new IllegalArgumentException("Slice from is negative: " + from);
        }
    }

    /**
     * @param from zero-based element to start from, inclusive
     * @param to zero-based element to end at, exclusive
     * @throws IllegalArgumentException if source is null, from is negative or
     * if from is more than to
     */
    public static <S> Cursor<S> slice(Cursor<S> source, long from, long to) {
        source = slice(source, from);
        long remaining = to - from;
        if (remaining < 0) {
            throw new IllegalArgumentException("Slice from is more than to: " + from + " > " + to);
        }
        return new Limit<S>(source, remaining);
    }

    final Cursor<S> mSource;

    SliceCursor(Cursor<S> source) {
        mSource = source;
    }

    public void close() throws FetchException {
        mSource.close();
    }

    private static class Skip<S> extends SliceCursor<S> {
        private volatile long mSkip;

        Skip(Cursor<S> source, long skip) {
            super(source);
            mSkip = skip;
        }

        public boolean hasNext() throws FetchException {
            doSkip();
            return mSource.hasNext();
        }

        public S next() throws FetchException {
            doSkip();
            return mSource.next();
        }

        @Override
        public int skipNext(int amount) throws FetchException {
            doSkip();
            return mSource.skipNext(amount);
        }

        private void doSkip() throws FetchException {
            if (mSkip > 0) {
                while (mSkip > Integer.MAX_VALUE) {
                    mSkip -= mSource.skipNext(Integer.MAX_VALUE);
                }
                mSource.skipNext((int) mSkip);
                mSkip = 0;
            }
        }
    }

    private static class Limit<S> extends SliceCursor<S> {
        private volatile long mRemaining;

        Limit(Cursor<S> source, long remaining) {
            super(source);
            mRemaining = remaining;
        }

        public boolean hasNext() throws FetchException {
            if (mSource.hasNext()) {
                if (mRemaining > 0) {
                    return true;
                }
                mSource.close();
            }
            return false;
        }

        public S next() throws FetchException {
            if (mRemaining <= 0) {
                throw new NoSuchElementException();
            }
            S next = mSource.next();
            if (--mRemaining <= 0) {
                mSource.close();
            }
            return next;
        }

        @Override
        public int skipNext(int amount) throws FetchException {
            if (mRemaining <= 0) {
                return 0;
            }
            if (amount > mRemaining) {
                amount = (int) mRemaining;
            }
            amount = mSource.skipNext(amount);
            if ((mRemaining -= amount) <= 0) {
                mSource.close();
            }
            return amount;
        }
    }
}
