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
 * Wraps another cursor and only produces a range of elements. The range might
 * be smaller if the source cursor doesn't have enough elements.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public class ViewCursor<S> extends AbstractCursor<S> {
    private final Cursor<S> mSource;

    private volatile long mFrom;
    private volatile long mRemaining;

    /**
     * @param from zero-based element to start from, inclusive
     * @param to zero-based element to end at, exclusive
     * @throws IllegalArgumentException if source is null, from is negative or
     * if from is more than to
     */
    public ViewCursor(Cursor<S> source, long from, long to) {
        if (source == null) {
            throw new IllegalArgumentException("Source is null");
        }
        if (from < 0) {
            throw new IllegalArgumentException("From is negative: " + from);
        }
        mSource = source;
        mFrom = from;
        mRemaining = to - from;
        if (mRemaining <= 0) {
            if (mRemaining < 0) {
                throw new IllegalArgumentException("From is more than to: " + from + " > " + to);
            }
            // Don't bother skipping.
            mFrom = 0;
        }
    }

    public void close() throws FetchException {
        mSource.close();
    }

    public boolean hasNext() throws FetchException {
        if (mFrom > 0) {
            while (mFrom > Integer.MAX_VALUE) {
                mFrom -= mSource.skipNext(Integer.MAX_VALUE);
            }
            mSource.skipNext((int) mFrom);
            mFrom = 0;
        }
        if (mSource.hasNext()) {
            if (mRemaining > 0) {
                return true;
            }
            mSource.close();
        }
        return false;
    }

    public S next() throws FetchException {
        if (!hasNext()) {
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
        if (!hasNext()) {
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
