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

import java.util.NoSuchElementException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Wraps another cursor and applies custom filtering to reduce the set of
 * results.
 *
 * @author Brian S O'Neill
 */
public abstract class FilteredCursor<S> extends AbstractCursor<S> {
    /**
     * Returns a Cursor that is filtered by the given filter expression and values.
     *
     * @param cursor cursor to wrap
     * @param type type of storable
     * @param filter filter to apply
     * @param filterValues values for filter
     * @return wrapped cursor which filters results
     * @throws IllegalStateException if any values are not specified
     * @throws IllegalArgumentException if any argument is null
     * @since 1.2
     */
    public static <S extends Storable> Cursor<S> applyFilter(Cursor<S> cursor,
                                                             Class<S> type,
                                                             String filter,
                                                             Object... filterValues)
    {
        Filter<S> f = Filter.filterFor(type, filter).bind();
        FilterValues<S> fv = f.initialFilterValues().withValues(filterValues);
        return applyFilter(f, fv, cursor);
    }

    /**
     * Returns a Cursor that is filtered by the given Filter and FilterValues.
     * The given Filter must be composed only of the same PropertyFilter
     * instances as used to construct the FilterValues. An
     * IllegalStateException will result otherwise.
     *
     * @param filter filter to apply
     * @param filterValues values for filter, which may be null if filter has no parameters
     * @param cursor cursor to wrap
     * @return wrapped cursor which filters results
     * @throws IllegalStateException if any values are not specified
     * @throws IllegalArgumentException if filter is closed
     */
    public static <S extends Storable> Cursor<S> applyFilter(Filter<S> filter,
                                                             FilterValues<S> filterValues,
                                                             Cursor<S> cursor)
    {
        if (filter.isOpen()) {
            return cursor;
        }
        if (filter.isClosed()) {
            throw new IllegalArgumentException();
        }

        // Make sure the filter is the same one that filterValues should be using.
        filter = filter.bind();

        Object[] values = filterValues == null ? null : filterValues.getValuesFor(filter);
        return FilteredCursorGenerator.getFactory(filter).newFilteredCursor(cursor, values);
    }

    private final Cursor<S> mCursor;

    private S mNext;

    protected FilteredCursor(Cursor<S> cursor) {
        if (cursor == null) {
            throw new IllegalArgumentException();
        }
        mCursor = cursor;
    }

    /**
     * @return false if object should not be in results
     */
    protected abstract boolean isAllowed(S storable) throws FetchException;

    public void close() throws FetchException {
        mCursor.close();
        mNext = null;
    }

    public boolean hasNext() throws FetchException {
        if (mNext != null) {
            return true;
        }
        try {
            int count = 0;
            while (mCursor.hasNext()) {
                S next = mCursor.next();
                if (isAllowed(next)) {
                    mNext = next;
                    return true;
                }
                interruptCheck(++count);
            }
        } catch (NoSuchElementException e) {
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
        return false;
    }

    public S next() throws FetchException {
        try {
            if (hasNext()) {
                S next = mNext;
                mNext = null;
                return next;
            }
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
        throw new NoSuchElementException();
    }

    @Override
    public int skipNext(int amount) throws FetchException {
        if (amount <= 0) {
            if (amount < 0) {
                throw new IllegalArgumentException("Cannot skip negative amount: " + amount);
            }
            return 0;
        }

        try {
            int count = 0;
            while (--amount >= 0 && hasNext()) {
                interruptCheck(++count);
                mNext = null;
            }

            return count;
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
    }

    private void interruptCheck(int count) throws FetchException {
        if ((count & ~0xff) == 0 && Thread.interrupted()) {
            close();
            throw new FetchInterruptedException();
        }
    }
}
