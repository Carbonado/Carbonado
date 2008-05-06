/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.jdbc;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.FilterValues;

/**
 * Simple DOM representing a SQL statement.
 *
 * @author Brian S O'Neill
 * @see SQLStatementBuilder
 */
abstract class SQLStatement<S extends Storable> {
    public abstract int maxLength();

    /**
     * Builds a statement string from the given values.
     *
     * @param initialCapacity expected size of finished string
     * length. Should be value returned from maxLength.
     * @param filterValues values may be needed to build complete statement
     */
    public String buildStatement(int initialCapacity, FilterValues<S> filterValues) {
        StringBuilder b = new StringBuilder(initialCapacity);
        this.appendTo(b, filterValues);
        return b.toString();
    }

    public abstract void appendTo(StringBuilder b, FilterValues<S> filterValues);

    /**
     * Just used for debugging.
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        appendTo(b, null);
        return b.toString();
    }
}
