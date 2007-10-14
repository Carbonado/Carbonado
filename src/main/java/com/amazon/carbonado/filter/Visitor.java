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

package com.amazon.carbonado.filter;

import com.amazon.carbonado.Storable;

/**
 * Traverses a filter tree in its canonical order. By overriding a visit
 * method, individual nodes can be captured and processed based on their
 * type. Call super.visit inside the overridden visit method to ensure that the
 * node's children are properly traversed.
 *
 * @author Brian S O'Neill
 */
public abstract class Visitor<S extends Storable, R, P> {
    public R visit(OrFilter<S> filter, P param) {
        filter.getLeftFilter().accept(this, param);
        filter.getRightFilter().accept(this, param);
        return null;
    }

    public R visit(AndFilter<S> filter, P param) {
        filter.getLeftFilter().accept(this, param);
        filter.getRightFilter().accept(this, param);
        return null;
    }

    public R visit(PropertyFilter<S> filter, P param) {
        return null;
    }

    /**
     * @since 1.2
     */
    public R visit(ExistsFilter<S> filter, P param) {
        return null;
    }

    public R visit(OpenFilter<S> filter, P param) {
        return null;
    }

    public R visit(ClosedFilter<S> filter, P param) {
        return null;
    }
}
