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

import com.amazon.carbonado.util.Throttle;

/**
 * Wraps another cursor and fetches results at a reduced speed.
 *
 * @author Brian S O'Neill
 */
public class ThrottledCursor<S> extends AbstractCursor<S> {
    private static final int WINDOW_SIZE = 10;
    private static final int SLEEP_PRECISION_MILLIS = 50;

    private final Cursor<S> mCursor;
    private final Throttle mThrottle;
    private final double mDesiredSpeed;

    /**
     * @param cursor cursor to wrap
     * @param throttle 1.0 = fetch at full speed, 0.5 = fetch at half speed,
     * 0.1 = fetch at one tenth speed, etc.
     */
    public ThrottledCursor(Cursor<S> cursor, double throttle) {
        mCursor = cursor;
        if (throttle < 1.0) {
            if (throttle < 0.0) {
                throttle = 0.0;
            }
            mThrottle = new Throttle(WINDOW_SIZE);
            mDesiredSpeed = throttle;
        } else {
            mThrottle = null;
            mDesiredSpeed = 1.0;
        }
    }

    public void close() throws FetchException {
        mCursor.close();
    }

    public boolean hasNext() throws FetchException {
        try {
            return mCursor.hasNext();
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
            throttle();
            return mCursor.next();
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
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
            while (--amount >= 0) {
                throttle();
                if (skipNext(1) <= 0) {
                    break;
                }
                count++;
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

    private void throttle() throws FetchInterruptedException {
        if (mThrottle != null) {
            try {
                mThrottle.throttle(mDesiredSpeed, SLEEP_PRECISION_MILLIS);
            } catch (InterruptedException e) {
                throw new FetchInterruptedException(e);
            }
        }
    }
}
