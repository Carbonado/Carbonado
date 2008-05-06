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

/**
 * Abstract cursor which wraps another cursor and transforms each storable
 * result into a target storable. This class can be used for implementing
 * one-to-one joins. Use {@link MultiTransformedCursor} for one-to-many joins.
 *
 * @author Brian S O'Neill
 * @param <S> source type, can be anything
 * @param <T> target type, can be anything
 */
public abstract class TransformedCursor<S, T> extends AbstractCursor<T> {
    private final Cursor<S> mCursor;

    private T mNext;

    protected TransformedCursor(Cursor<S> cursor) {
        if (cursor == null) {
            throw new IllegalArgumentException();
        }
        mCursor = cursor;
    }

    /**
     * This method must be implemented to transform storables. If the storable
     * cannot be transformed, either throw a FetchException or return null. If
     * null is returned, the storable is simply filtered out.
     *
     * @return transformed storable, or null to filter it out
     */
    protected abstract T transform(S storable) throws FetchException;

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
                T next = transform(mCursor.next());
                if (next != null) {
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

    public T next() throws FetchException {
        try {
            if (hasNext()) {
                T next = mNext;
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
