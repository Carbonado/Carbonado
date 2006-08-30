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
import java.util.Collection;

import com.amazon.carbonado.FetchException;

/**
 * Buffers up Storable instances allowing them to be sorted. Should any method
 * need to throw an undeclared exception, wrap it with an
 * UndeclaredThrowableException.
 *
 * @author Brian S O'Neill
 * @see SortedCursor
 */
public interface SortBuffer<S> extends Collection<S> {
    /**
     * Clears buffer and assigns a comparator for sorting.
     *
     * @throws IllegalArgumentException if comparator is null
     */
    void prepare(Comparator<S> comparator);

    /**
     * Finish sorting buffer.
     *
     * @throws IllegalStateException if prepare was never called
     */
    void sort() throws FetchException;

    /**
     * Clear and close buffer.
     */
    void close() throws FetchException;
}
