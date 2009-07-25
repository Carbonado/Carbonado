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
 * Wraps another cursor to skip an initial amount of elements.
 *
 * @author Brian S O'Neill
 * @see LimitCursor
 * @since 1.2
 */
public class SkipCursor<S> extends AbstractCursor<S> {
    private final Cursor<S> mSource;
    private volatile long mSkip;

    /**
     * @param skip initial amount of elements to skip
     * @throws IllegalArgumentException if source is null or skip is negative
     */
    public SkipCursor(Cursor<S> source, long skip) {
        if (source == null) {
            throw new IllegalArgumentException("Source is null");
        }
        if (skip < 0) {
            throw new IllegalArgumentException("Skip is negative: " + skip);
        }
        mSource = source;
        mSkip = skip;
    }

    public boolean hasNext() throws FetchException {
        try {
            doSkip();
            return mSource.hasNext();
        } catch (NoSuchElementException e) {
            return false;
        }
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

    public void close() throws FetchException {
        mSource.close();
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
