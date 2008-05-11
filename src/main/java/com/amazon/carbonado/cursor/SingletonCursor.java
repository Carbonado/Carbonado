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
import java.util.NoSuchElementException;

import com.amazon.carbonado.Cursor;

/**
 * Special cursor implementation that returns only one element.
 *
 * @author Brian S O'Neill
 * @see EmptyCursor
 */
public class SingletonCursor<S> implements Cursor<S> {
    private volatile S mObject;

    /**
     * @param object single object to return from cursor, must not be null
     * @throws IllegalArgumentException if object is null
     */
    public SingletonCursor(S object) {
        if (object == null) {
            throw new IllegalArgumentException();
        }
        mObject = object;
    }

    public void close() {
        mObject = null;
    }

    public boolean hasNext() {
        return mObject != null;
    }

    public S next() {
        S object = mObject;
        mObject = null;
        if (object == null) {
            throw new NoSuchElementException();
        }
        return object;
    }

    public int skipNext(int amount) {
        if (amount <= 0) {
            if (amount < 0) {
                throw new IllegalArgumentException("Cannot skip negative amount: " + amount);
            }
            return 0;
        }
        S object = mObject;
        mObject = null;
        return object == null ? 0 : 1;
    }

    public int copyInto(Collection<? super S> c) {
        S object = mObject;
        mObject = null;
        if (object == null) {
            return 0;
        }
        c.add(object);
        return 1;
    }

    public int copyInto(Collection<? super S> c, int limit) {
        return limit <= 0 ? 0 : copyInto(c);
    }

    public List<S> toList() {
        List<S> list = new ArrayList<S>(1);
        copyInto(list);
        return list;
    }

    public List<S> toList(int limit) {
        List<S> list = new ArrayList<S>(1);
        copyInto(list, limit);
        return list;
    }
}
