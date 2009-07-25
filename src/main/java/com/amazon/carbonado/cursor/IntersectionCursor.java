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

import java.util.Comparator;
import java.util.NoSuchElementException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Cursor;

/**
 * Wraps two Cursors and performs a <i>set intersection</i> operation. In
 * boolean logic, this is an <i>and</i> operation.
 *
 * <p>Both cursors must return results in the same order. Ordering is preserved
 * by the intersection.
 *
 * @author Brian S O'Neill
 * @see UnionCursor
 * @see DifferenceCursor
 * @see SymmetricDifferenceCursor
 */
public class IntersectionCursor<S> extends AbstractCursor<S> {
    private final Cursor<S> mLeftCursor;
    private final Cursor<S> mRightCursor;
    private final Comparator<S> mOrder;

    private S mNext;

    /**
     * @param left cursor to wrap
     * @param right cursor to wrap
     * @param order describes sort ordering of wrapped cursors, which must be
     * a total ordering
     */
    public IntersectionCursor(Cursor<S> left, Cursor<S> right, Comparator<S> order) {
        if (left == null || right == null || order == null) {
            throw new IllegalArgumentException();
        }
        mLeftCursor = left;
        mRightCursor = right;
        mOrder = order;
    }

    public void close() throws FetchException {
        mLeftCursor.close();
        mRightCursor.close();
        mNext = null;
    }

    public boolean hasNext() throws FetchException {
        if (mNext != null) {
            return true;
        }

        S nextLeft, nextRight;

        try {
            if (mLeftCursor.hasNext()) {
                nextLeft = mLeftCursor.next();
            } else {
                close();
                return false;
            }
            if (mRightCursor.hasNext()) {
                nextRight = mRightCursor.next();
            } else {
                close();
                return false;
            }

            while (true) {
                int result = mOrder.compare(nextLeft, nextRight);
                if (result < 0) {
                    if (mLeftCursor.hasNext()) {
                        nextLeft = mLeftCursor.next();
                    } else {
                        close();
                        return false;
                    }
                } else if (result > 0) {
                    if (mRightCursor.hasNext()) {
                        nextRight = mRightCursor.next();
                    } else {
                        close();
                        return false;
                    }
                } else {
                    mNext = nextLeft;
                    return true;
                }
            }
        } catch (NoSuchElementException e) {
            return false;
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
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
}
