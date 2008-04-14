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

import java.io.IOException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.LimitCursor;
import com.amazon.carbonado.cursor.SkipCursor;

import com.amazon.carbonado.filter.FilterValues;

/**
 * AbstractQueryExecutor implements a small set of common QueryExecutor methods.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractQueryExecutor<S extends Storable> implements QueryExecutor<S> {
    public Class<S> getStorableType() {
        return getFilter().getStorableType();
    }

    /**
     * Produces a slice via skip and limit cursors. Subclasses are encouraged
     * to override with a more efficient implementation.
     *
     * @since 1.2
     */
    public Cursor<S> fetchSlice(FilterValues<S> values, long from, Long to) throws FetchException {
        Cursor<S> cursor = fetch(values);
        if (from > 0) {
            cursor = new SkipCursor<S>(cursor, from);
        }
        if (to != null) {
            cursor = new LimitCursor<S>(cursor, to - from);
        }
        return cursor;
    }

    /**
     * Counts results by opening a cursor and skipping entries. Subclasses are
     * encouraged to override with a more efficient implementation.
     */
    public long count(FilterValues<S> values) throws FetchException {
        Cursor<S> cursor = fetch(values);
        try {
            long count = cursor.skipNext(Integer.MAX_VALUE);
            if (count == Integer.MAX_VALUE) {
                int amt;
                while ((amt = cursor.skipNext(Integer.MAX_VALUE)) > 0) {
                    count += amt;
                }
            }
            return count;
        } finally {
            cursor.close();
        }
    }

    /**
     * Does nothing and returns false.
     */
    public boolean printNative(Appendable app, int indentLevel, FilterValues<S> values)
        throws IOException
    {
        return false;
    }

    /**
     * Appends spaces to the given appendable. Useful for implementing
     * printNative and printPlan.
     */
    protected void indent(Appendable app, int indentLevel) throws IOException {
        for (int i=0; i<indentLevel; i++) {
            app.append(' ');
        }
    }

    /**
     * Appends a newline character.
     */
    protected void newline(Appendable app) throws IOException {
        app.append('\n');
    }

    /**
     * Adds a constant amount to the given indent level. Useful for
     * implementing printNative and printPlan.
     */
    protected int increaseIndent(int indentLevel) {
        return indentLevel + 2;
    }
}
