/*
 * Copyright 2011-2012 Amazon Technologies, Inc. or its affiliates.
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

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;

/**
 * Wraps another cursor and periodically calls a {@link com.amazon.carbonado.Query.Controller controller}.
 *
 * @author Brian S O'Neill
 */
public class ControllerCursor<S> extends AbstractCursor<S> {
    /**
     * Returns a ControllerCursor depending on whether a controller instance is
     * passed in or not.
     *
     * @param controller optional controller which can abort query operation
     * @throws IllegalArgumentException if source is null
     */
    public static <S> Cursor<S> apply(Cursor<S> source, Query.Controller controller) {
        return controller == null ? source : new ControllerCursor<S>(source, controller);
    }

    private final Cursor<S> mSource;
    private final Query.Controller mController;

    private byte mCount;

    /**
     * @param controller required controller which can abort query operation
     * @throws IllegalArgumentException if either argument is null
     */
    private ControllerCursor(Cursor<S> source, Query.Controller controller) {
        if (source == null) {
            throw new IllegalArgumentException("Source is null");
        }
        if (controller == null) {
            throw new IllegalArgumentException("Controller is null");
        }
        mSource = source;
        mController = controller;
        controller.begin();
    }

    public boolean hasNext() throws FetchException {
        if (mSource.hasNext()) {
            continueCheck();
            return true;
        }
        return false;
    }

    public S next() throws FetchException {
        S next = mSource.next();
        continueCheck();
        return next;
    }

    public void close() throws FetchException {
        try {
            mSource.close();
        } finally {
            mController.close();
        }
    }

    private void continueCheck() throws FetchException {
        if (++mCount == 0) {
            try {
                mController.continueCheck();
            } catch (FetchException e) {
                try {
                    close();
                } catch (FetchException e2) {
                    // Ignore.
                }
                throw e;
            }
        }
    }
}
