/*
 * Copyright 2012 Amazon Technologies, Inc. or its affiliates.
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

import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;

/**
 * Cursor implementation which fetches records in advance, in order to release locks.
 *
 * @author Brian S O'Neill
 */
public class FetchAheadCursor<S> extends AbstractCursor<S> {
    private final Cursor<S> mSource;
    private final int mFetchAhead;
    private final Queue<Object> mQueue;

    /**
     * @param fetchAhead how much to fetch ahead from source
     */
    public FetchAheadCursor(Cursor<S> source, int fetchAhead) {
        mSource = source;
        mFetchAhead = fetchAhead;
        mQueue = new ArrayDeque<Object>(fetchAhead + 1);
    }

    public void close() throws FetchException {
        mQueue.clear();
        mSource.close();
    }

    public boolean hasNext() throws FetchException {
        while (mQueue.size() <= mFetchAhead && mSource.hasNext()) {
            try {
                mQueue.add(mSource.next());
            } catch (FetchException e) {
                mQueue.add(e);
            }
        }
        return !mQueue.isEmpty();
    }

    public S next() throws FetchException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Object next = mQueue.remove();
        if (next instanceof FetchException) {
            throw (FetchException) next;
        }
        return (S) next;
    }
}
