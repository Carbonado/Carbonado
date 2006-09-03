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

package com.amazon.carbonado.qe;

import java.util.concurrent.locks.Lock;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.IteratorCursor;

/**
 * QueryExecutor which fully scans an iterable collection.
 *
 * @author Brian S O'Neill
 * @see IteratorCursor
 */
public class IterableQueryExecutor<S extends Storable> extends FullScanQueryExecutor<S> {
    private final Iterable<S> mIterable;
    private final Lock mLock;

    /**
     * @param type type of Storable
     * @param iterable collection to iterate over, or null for empty cursor
     * @throws IllegalArgumentException if type is null
     */
    public IterableQueryExecutor(Class<S> type, Iterable<S> iterable) {
        this(type, iterable, null);
    }

    /**
     * @param type type of Storable
     * @param iterable collection to iterate over, or null for empty cursor
     * @param lock optional lock to hold while cursor is open
     * @throws IllegalArgumentException if type is null
     */
    public IterableQueryExecutor(Class<S> type, Iterable<S> iterable, Lock lock) {
        super(type);
        mIterable = iterable;
        mLock = lock;
    }

    protected Cursor<S> fetch() {
        return new IteratorCursor<S>(mIterable, mLock);
    }
}
