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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.OrFilter;
import com.amazon.carbonado.filter.PropertyFilter;
import com.amazon.carbonado.filter.RelOp;
import com.amazon.carbonado.filter.Visitor;

/**
 * Produces unmodifable lists of PropertyFilters which were originally all
 * 'and'ed together. The filters are ordered such that all '=' operators are
 * first and all '!=' operators are last.
 *
 * @author Brian S O'Neill
 */
class PropertyFilterList {
    private static Map<Filter<?>, List> cCache;

    static {
        cCache = new SoftValuedHashMap();
    }

    /**
     * @param filter filter to break up into separate PropertyFilters.
     * @return unmodifiable list of PropertyFilters, which is empty if input filter was null
     * @throws IllegalArgumentException if filter has any operators other than 'and'.
     */
    static <S extends Storable> List<PropertyFilter<S>> get(Filter<S> filter) {
        List<PropertyFilter<S>> list;

        synchronized (cCache) {
            list = (List<PropertyFilter<S>>) cCache.get(filter);
        }

        if (list != null) {
            return list;
        }

        if (filter == null) {
            list = Collections.emptyList();
        } else if (filter instanceof PropertyFilter) {
            list = Collections.singletonList((PropertyFilter<S>) filter);
        } else {
            list = new ArrayList<PropertyFilter<S>>();
            final List<PropertyFilter<S>> flist = list;

            filter.accept(new Visitor<S, Object, Object>() {
                public Object visit(OrFilter<S> filter, Object param) {
                    throw new IllegalArgumentException("Logical 'or' not allowed");
                }

                public Object visit(PropertyFilter<S> filter, Object param) {
                    flist.add(filter);
                    return null;
                }
            }, null);

            Collections.sort(list, new PropertyFilterComparator<S>());

            ((ArrayList) list).trimToSize();
            list = Collections.unmodifiableList(list);
        }

        synchronized (cCache) {
            cCache.put(filter, list);
        }

        return list;
    }

    private static class PropertyFilterComparator<S extends Storable>
        implements Comparator<PropertyFilter<S>>
    {
        public int compare(PropertyFilter<S> a, PropertyFilter<S> b) {
            if (a.getOperator() != b.getOperator()) {
                if (a.getOperator() == RelOp.EQ) {
                    return -1;
                }
                if (a.getOperator() == RelOp.NE) {
                    return 1;
                }
                if (b.getOperator() == RelOp.EQ) {
                    return 1;
                }
                if (b.getOperator() == RelOp.NE) {
                    return -1;
                }
            }
            return 0;
        }
    }
}
