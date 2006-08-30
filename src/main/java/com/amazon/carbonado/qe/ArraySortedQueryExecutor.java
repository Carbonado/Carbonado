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

package com.amazon.carbonado.qe;

import java.util.List;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.ArraySortBuffer;
import com.amazon.carbonado.cursor.SortBuffer;

import com.amazon.carbonado.info.OrderedProperty;

/**
 * QueryExecutor which wraps another and sorts the results within an array.
 *
 * @author Brian S O'Neill
 * @see ArraySortBuffer
 */
public class ArraySortedQueryExecutor<S extends Storable> extends SortedQueryExecutor<S> {
    /**
     * @param executor executor to wrap
     * @throws IllegalArgumentException if executor is null or if remainder
     * orderings is empty
     */
    public ArraySortedQueryExecutor(QueryExecutor<S> executor,
                                    List<OrderedProperty<S>> handledOrderings,
                                    List<OrderedProperty<S>> remainderOrderings)
    {
        super(executor, handledOrderings, remainderOrderings);
    }

    protected SortBuffer<S> createSortBuffer() {
        return new ArraySortBuffer<S>();
    }
}
