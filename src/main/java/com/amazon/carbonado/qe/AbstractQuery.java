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
import java.lang.reflect.UndeclaredThrowableException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchMultipleException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;

import com.amazon.carbonado.util.Appender;

/**
 * AbstractQuery implements a small set of common Query methods. Subclasses
 * should consider overriding some of these methods, if it provides better
 * performance.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractQuery<S extends Storable> implements Query<S>, Appender {
    protected AbstractQuery() {
    }

    @Override
    public Query<S> and(String filter) throws FetchException {
        return and(Filter.filterFor(getStorableType(), filter));
    }

    @Override
    public Query<S> or(String filter) throws FetchException {
        return or(Filter.filterFor(getStorableType(), filter));
    }

    @Override
    public <T extends S> Cursor<S> fetchAfter(T start) throws FetchException {
        return after(start).fetch();
    }

    @Override
    public S loadOne() throws FetchException {
        S obj = tryLoadOne();
        if (obj == null) {
            throw new FetchNoneException(toString());
        }
        return obj;
    }

    @Override
    public S tryLoadOne() throws FetchException {
        Cursor<S> cursor = fetch();
        try {
            if (cursor.hasNext()) {
                S obj = cursor.next();
                if (cursor.hasNext()) {
                    throw new FetchMultipleException(toString());
                }
                return obj;
            } else {
                return null;
            }
        } finally {
            cursor.close();
        }
    }

    @Override
    public void deleteOne() throws PersistException {
        if (!tryDeleteOne()) {
            throw new PersistNoneException(toString());
        }
    }

    @Override
    public boolean printNative() {
        try {
            return printNative(System.out);
        } catch (IOException e) {
            // Shouldn't happen since PrintStream suppresses exceptions.
            throw new UndeclaredThrowableException(e);
        }
    }

    @Override
    public boolean printNative(Appendable app) throws IOException {
        return printNative(app, 0);
    }

    @Override
    public boolean printPlan() {
        try {
            return printPlan(System.out);
        } catch (IOException e) {
            // Shouldn't happen since PrintStream suppresses exceptions.
            throw new UndeclaredThrowableException(e);
        }
    }

    @Override
    public boolean printPlan(Appendable app) throws IOException {
        return printPlan(app, 0);
    }

    /**
     * Implementation calls appendTo.
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        try {
            appendTo(b);
        } catch (IOException e) {
            // Not gonna happen
        }
        return b.toString();
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    /**
     * Called by sliced fetch to ensure that arguments are valid.
     *
     * @return false if from is 0 and to is null
     * @throws IllegalArgumentException if arguments are invalid
     * @since 1.2
     */
    protected boolean checkSliceArguments(long from, Long to) {
        if (from < 0) {
            throw new IllegalArgumentException("Slice from is negative: " + from);
        }
        if (to == null) {
            if (from == 0) {
                return false;
            }
        } else if (from > to) {
            throw new IllegalArgumentException
                ("Slice from is more than to: " + from + " > " + to);
        }
        return true;
    }
}
