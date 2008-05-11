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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;

/**
 * AbstractCursor implements a small set of common Cursor methods.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractCursor<S> implements Cursor<S> {
    // Note: Since constructor takes no parameters, this class is called
    // Abstract instead of Base.
    protected AbstractCursor() {
    }

    public int copyInto(Collection<? super S> c) throws FetchException {
        try {
            int count = 0;
            while (hasNext()) {
                c.add(next());
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

    public int copyInto(Collection<? super S> c, int limit) throws FetchException {
        try {
            int count = 0;
            while (--limit >= 0 && hasNext()) {
                c.add(next());
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

    public List<S> toList() throws FetchException {
        try {
            List<S> list = new ArrayList<S>();
            copyInto(list);
            return list;
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
    }

    public List<S> toList(int limit) throws FetchException {
        try {
            List<S> list = new ArrayList<S>();
            copyInto(list, limit);
            return list;
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
    }

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
                next();
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
}
