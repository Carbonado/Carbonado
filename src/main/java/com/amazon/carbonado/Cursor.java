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

package com.amazon.carbonado;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Represents the results of a {@link com.amazon.carbonado.Query}'s fetch
 * operation. Cursors must be closed promptly when no longer
 * needed. Failure to do so may result in excessive resource consumption or
 * deadlock. As a convenience, the close operation is automatically performed
 * when the end is reached or when an exception is thrown.
 *
 * <P>Note: because a Cursor manages resources, it is inapproprate to create a long-lived one and
 * pass it around in your code.  A cursor is expected to live close to the Query which vended
 * it.  To discourage inappropriate retention, the cursor does not implement methods (like
 * "getQuery" or "reset") which would make it more convenient to operate on in isolation.
 *
 * <P>Similarly, it is difficult to guarantee that the results of a cursor will
 * be the same in case of a "reset" or reverse iteration.  For this reason,
 * neither is supported; if you need to iterate the same set of objects twice,
 * simply retain the query object and reissue it. Be aware that the results may
 * not be identical, if any relevant objects are added to or removed the
 * repository in the interim. To guard against this, operate within a
 * serializable {@link IsolationLevel isolation level}.
 *
 * <p>Cursor instances are mutable and not guaranteed to be thread-safe. Only
 * one thread should ever operate on a cursor instance.
 *
 * @author Brian S O'Neill
 * @author Don Schneider
 */
public interface Cursor<S> {
    /**
     * Call close to release any resources being held by this cursor. Further
     * operations on this cursor will behave as if there are no results.
     */
    void close() throws FetchException;

    /**
     * Returns true if this cursor has more elements. In other words, returns
     * true if {@link #next next} would return an element rather than throwing
     * an exception.
     *
     * @throws FetchException if storage layer throws an exception
     */
    boolean hasNext() throws FetchException;

    /**
     * Returns the next element from this cursor. This method may be called
     * repeatedly to iterate through the results.
     *
     * @throws FetchException if storage layer throws an exception
     * @throws NoSuchElementException if the cursor has no next element.
     */
    S next() throws FetchException;

    /**
     * Skips forward by the specified amount of elements, returning the actual
     * amount skipped. The actual amount is less than the requested amount only
     * if the end of the results was reached.
     *
     * @param amount maximum amount of elements to skip
     * @return actual amount skipped
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalArgumentException if amount is negative
     */
    int skipNext(int amount) throws FetchException;

    /**
     * Copies all remaining next elements into the given collection. This
     * method is roughly equivalent to the following:
     * <pre>
     * Cursor cursor;
     * ...
     * while (cursor.hasNext()) {
     *     c.add(cursor.next());
     * }
     * </pre>
     *
     * <p>As a side-effect of calling this method, the cursor is closed.
     *
     * @return actual amount of results added
     * @throws FetchException if storage layer throws an exception
     */
    int copyInto(Collection<? super S> c) throws FetchException;

    /**
     * Copies a limited amount of remaining next elements into the given
     * collection. This method is roughly equivalent to the following:
     * <pre>
     * Cursor cursor;
     * ...
     * while (--limit >= 0 && cursor.hasNext()) {
     *     c.add(cursor.next());
     * }
     * </pre>
     *
     * @param limit maximum amount of elements to copy
     * @return actual amount of results added
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalArgumentException if limit is negative
     */
    int copyInto(Collection<? super S> c, int limit) throws FetchException;

    /**
     * Copies all remaining next elements into a new modifiable list. This
     * method is roughly equivalent to the following:
     * <pre>
     * Cursor&lt;S&gt; cursor;
     * ...
     * List&lt;S&gt; list = new ...
     * cursor.copyInto(list);
     * </pre>
     *
     * <p>As a side-effect of calling this method, the cursor is closed.
     *
     * @return a new modifiable list
     * @throws FetchException if storage layer throws an exception
     */
    List<S> toList() throws FetchException;

    /**
     * Copies a limited amount of remaining next elements into a new modifiable
     * list. This method is roughly equivalent to the following:
     * <pre>
     * Cursor&lt;S&gt; cursor;
     * ...
     * List&lt;S&gt; list = new ...
     * cursor.copyInto(list, limit);
     * </pre>
     *
     * @param limit maximum amount of elements to copy
     * @return a new modifiable list
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalArgumentException if limit is negative
     */
    List<S> toList(int limit) throws FetchException;
}
