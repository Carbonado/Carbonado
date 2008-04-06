/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

import java.util.EnumMap;

/**
 * An immutable map of query hints.
 *
 * @author Brian S O'Neill
 * @see QueryHint
 * @since 1.2
 */
public class QueryHints implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private static final QueryHints EMPTY_HINTS = new QueryHints(null);

    public static QueryHints emptyHints() {
        return EMPTY_HINTS;
    }

    private final EnumMap<QueryHint, Object> mMap;

    private QueryHints(EnumMap<QueryHint, Object> map) {
        mMap = map;
    }

    /**
     * Returns a new QueryHints object with the given hint. The associated
     * value is the hint object itself.
     *
     * @throws IllegalArgumentException if hint is null
     */
    public QueryHints with(QueryHint hint) {
        return with(hint, hint);
    }

    /**
     * Returns a new QueryHints object with the given hint and value.
     *
     * @throws IllegalArgumentException if hint or value is null
     */
    public QueryHints with(QueryHint hint, Object value) {
        if (hint == null) {
            throw new IllegalArgumentException("Null hint");
        }
        if (value == null) {
            throw new IllegalArgumentException("Null value");
        }
        EnumMap<QueryHint, Object> map;
        if (mMap == null) {
            map = new EnumMap<QueryHint, Object>(QueryHint.class);
        } else {
            map = mMap.clone();
        }
        map.put(hint, value);
        return new QueryHints(map);
    }

    /**
     * Returns a new QueryHints object without the given hint.
     */
    public QueryHints without(QueryHint hint) {
        if (hint == null || mMap == null || !mMap.containsKey(hint)) {
            return this;
        }
        EnumMap<QueryHint, Object> map = mMap.clone();
        map.remove(hint);
        if (map.size() == 0) {
            map = null;
        }
        return new QueryHints(map);
    }

    /**
     * Returns false if hint is not provided.
     */
    public boolean contains(QueryHint hint) {
        return get(hint) != null;
    }

    /**
     * Returns null if hint is not provided.
     */
    public Object get(QueryHint hint) {
        return hint == null ? null : (mMap == null ? null : mMap.get(hint));
    }

    public boolean isEmpty() {
        return mMap == null ? true : mMap.isEmpty();
    }

    @Override
    public int hashCode() {
        return mMap == null ? 0 : mMap.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof QueryHints) {
            QueryHints other = (QueryHints) obj;
            if (mMap == null) {
                return other.mMap == null;
            } else {
                return mMap.equals(other.mMap);
            }
        }
        return false;
    }

    @Override
    public String toString() {
        if (mMap == null) {
            return "QueryHints: {}";
        }
        return "QueryHints: " + mMap;
    }
}
