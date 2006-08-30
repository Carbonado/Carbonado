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
import java.util.Collections;
import java.util.Comparator;

/**
 * Sort buffer implementation backed by an ArrayList.
 *
 * @author Brian S O'Neill
 * @see SortedCursor
 */
public class ArraySortBuffer<S> extends ArrayList<S> implements SortBuffer<S> {
    private static final long serialVersionUID = -5622302375191321452L;

    private Comparator<S> mComparator;

    public ArraySortBuffer() {
        super();
    }

    public ArraySortBuffer(int initialCapacity) {
        super(initialCapacity);
    }

    public void prepare(Comparator<S> comparator) {
        if (comparator == null) {
            throw new IllegalArgumentException();
        }
        clear();
        mComparator = comparator;
    }

    public void sort() {
        if (mComparator == null) {
            throw new IllegalStateException("Buffer was not prepared");
        }
        Collections.sort(this, mComparator);
    }

    public void close() {
        clear();
    }
}
