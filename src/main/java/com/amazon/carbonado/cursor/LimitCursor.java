/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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
 * Wraps another cursor to limit the amount of elements.
 *
 * @author Brian S O'Neill
 * @see SkipCursor
 * @since 1.2
 */
public class LimitCursor<S> extends AbstractCursor<S> {
    private final Cursor<S> mSource;
    private volatile long mRemaining;

    /**
     * @param limit maximum amount of elements
     * @throws IllegalArgumentException if source is null or limit is negative
     */
    public LimitCursor(Cursor<S> source, long limit) {
        if (source == null) {
            throw new IllegalArgumentException("Source is null");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("Limit is negative: " + limit);
        }
        mSource = source;
        mRemaining = limit;
    }

    public boolean hasNext() throws FetchException {
        try {
            if (mSource.hasNext()) {
                if (mRemaining > 0) {
                    return true;
                }
                mSource.close();
            }
        } catch (NoSuchElementException e) {
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

    public void close() throws FetchException {
        mSource.close();
    }
}
