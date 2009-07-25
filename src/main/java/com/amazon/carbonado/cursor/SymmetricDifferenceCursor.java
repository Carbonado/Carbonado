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
 * Wraps two Cursors and performs a <i>symmetric set difference</i>
 * operation. In boolean logic, this is an <i>exclusive or</i> operation.
 *
 * <p>Both cursors must return results in the same order. Ordering is preserved
 * by the difference.
 *
 * @author Brian S O'Neill
 * @see UnionCursor
 * @see IntersectionCursor
 * @see DifferenceCursor
 */
public class SymmetricDifferenceCursor<S> extends AbstractCursor<S> {
    private final Cursor<S> mLeftCursor;
    private final Cursor<S> mRightCursor;
    private final Comparator<S> mOrder;

    private S mNextLeft;
    private S mNextRight;
    private int mCompareResult;

    /**
     * @param left cursor to wrap
     * @param right cursor to wrap
     * @param order describes sort ordering of wrapped cursors, which must be
     * a total ordering
     */
    public SymmetricDifferenceCursor(Cursor<S> left, Cursor<S> right, Comparator<S> order) {
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
            return compareNext() != 0;
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
    }

    /**
     * Returns 0 if no next element available, {@literal <0} if next element is
     * from left source cursor, and {@literal >0} if next element is from right
     * source cursor.
     */
    public int compareNext() throws FetchException {
        if (mCompareResult != 0) {
            return mCompareResult;
        }

        try {
            while (true) {
                if (mNextLeft == null && mLeftCursor.hasNext()) {
                    mNextLeft = mLeftCursor.next();
                }
                if (mNextRight == null && mRightCursor.hasNext()) {
                    mNextRight = mRightCursor.next();
                }

                if (mNextLeft == null) {
                    return mNextRight != null ? 1 : 0;
                }
                if (mNextRight == null) {
                    return -1;
                }

                if ((mCompareResult = mOrder.compare(mNextLeft, mNextRight)) == 0) {
                    mNextLeft = null;
                    mNextRight = null;
                } else {
                    return mCompareResult;
                }
            }
        } catch (NoSuchElementException e) {
            return 0;
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
            S next;
            int result = compareNext();
            if (result < 0) {
                next = mNextLeft;
                mNextLeft = null;
            } else if (result > 0) {
                next = mNextRight;
                mNextRight = null;
            } else {
                throw new NoSuchElementException();
            }
            mCompareResult = 0;
            return next;
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
    }
}
