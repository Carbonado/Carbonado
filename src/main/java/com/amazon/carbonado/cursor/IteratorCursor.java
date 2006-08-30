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

package com.amazon.carbonado.cursor;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Adapts an Iterator into a Cursor.
 *
 * @author Brian S O'Neill
 */
public class IteratorCursor<S> extends AbstractCursor<S> {
    private Iterator<S> mIterator;

    /**
     * @param iterable collection to iterate over, or null for empty cursor
     */
    public IteratorCursor(Iterable<S> iterable) {
        this(iterable == null ? (Iterator<S>) null : iterable.iterator());
    }

    /**
     * @param iterator iterator to wrap, or null for empty cursor
     */
    public IteratorCursor(Iterator<S> iterator) {
        mIterator = iterator;
    }

    public void close() {
        mIterator = null;
    }

    public boolean hasNext() {
        Iterator it = mIterator;
        return it != null && it.hasNext();
    }

    public S next() {
        Iterator<S> it = mIterator;
        if (it == null) {
            throw new NoSuchElementException();
        }
        return it.next();
    }
}
