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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import com.amazon.carbonado.Cursor;

/**
 * Special cursor implementation that is empty.
 *
 * @author Brian S O'Neill
 * @see SingletonCursor
 */
public class EmptyCursor<S> implements Cursor<S> {
    private static final Cursor EMPTY_CURSOR = new EmptyCursor();

    /**
     * Returns the empty cursor instance.
     */
    @SuppressWarnings("unchecked")
    public static <S> Cursor<S> the() {
        return EMPTY_CURSOR;
    }

    // Package-private, to be used by test suite
    EmptyCursor() {
    }

    /**
     * Does nothing.
     */
    public void close() {
    }

    /**
     * Always returns false.
     */
    public boolean hasNext() {
        return false;
    }

    /**
     * Always throws NoSuchElementException.
     */
    public S next() {
        throw new NoSuchElementException();
    }

    /**
     * Always returns 0.
     */
    public int skipNext(int amount) {
        return 0;
    }

    /**
     * Performs no copy and always returns 0.
     */
    public int copyInto(Collection<? super S> c) {
        return 0;
    }

    /**
     * Performs no copy and always returns 0.
     */
    public int copyInto(Collection<? super S> c, int limit) {
        return 0;
    }

    /**
     * Always returns an empty list.
     */
    public List<S> toList() {
        return Collections.emptyList();
    }

    /**
     * Always returns an empty list.
     */
    public List<S> toList(int limit) {
        return Collections.emptyList();
    }
}

