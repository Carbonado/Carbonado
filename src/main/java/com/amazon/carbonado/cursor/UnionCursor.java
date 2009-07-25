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
 * Wraps two Cursors and performs a <i>set union</i> operation. In boolean
 * logic, this is an <i>or</i> operation.
 *
 * <p>Both cursors must return results in the same order. Ordering is preserved
 * by the union.
 *
 * @author Brian S O'Neill
 * @see IntersectionCursor
 * @see DifferenceCursor
 * @see SymmetricDifferenceCursor
 */
public class UnionCursor<S> extends AbstractCursor<S> {
    private final Cursor<S> mLeftCursor;
    private final Cursor<S> mRightCursor;
    private final Comparator<S> mOrder;

    private S mNextLeft;
    private S mNextRight;

    /**
     * @param left cursor to wrap
     * @param right cursor to wrap
     * @param order describes sort ordering of wrapped cursors, which must be
     * a total ordering
     */
    public UnionCursor(Cursor<S> left, Cursor<S> right, Comparator<S> order) {
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
        mNextLeft = null;
        mNextRight = null;
    }

    public boolean hasNext() throws FetchException {
        try {
            if (mNextLeft == null && mLeftCursor.hasNext()) {
                mNextLeft = mLeftCursor.next();
            }
            if (mNextRight == null && mRightCursor.hasNext()) {
                mNextRight = mRightCursor.next();
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
        return mNextLeft != null || mNextRight != null;
    }

    public S next() throws FetchException {
        try {
            if (hasNext()) {
                S next;
                if (mNextLeft == null) {
                    next = mNextRight;
                    mNextRight = null;
                } else if (mNextRight == null) {
                    next = mNextLeft;
                    mNextLeft = null;
                } else {
                    int result = mOrder.compare(mNextLeft, mNextRight);
                    if (result < 0) {
                        next = mNextLeft;
                        mNextLeft = null;
                    } else if (result > 0) {
                        next = mNextRight;
                        mNextRight = null;
                    } else {
                        next = mNextLeft;
                        mNextLeft = null;
                        mNextRight = null;
                    }
                }
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
