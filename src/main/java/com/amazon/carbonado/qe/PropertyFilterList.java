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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.ExistsFilter;
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
class PropertyFilterList<S extends Storable> extends AbstractList<PropertyFilter<S>> {
    private static Map<Filter<?>, PropertyFilterList> cCache;

    static {
        cCache = new SoftValuedHashMap();
    }

    /**
     * @param filter filter to break up into separate PropertyFilters.
     * @return unmodifiable list of PropertyFilters, which is empty if input filter was null
     * @throws IllegalArgumentException if filter has any operators other than 'and'.
     */
    static <S extends Storable> PropertyFilterList<S> get(Filter<S> filter) {
        PropertyFilterList<S> plist;

        synchronized (cCache) {
            plist = (PropertyFilterList<S>) cCache.get(filter);
        }

        if (plist != null) {
            return plist;
        }

        List<PropertyFilter<S>> list;
        Map<PropertyFilter<S>, Integer> posMap;
        List<ExistsFilter<S>> existsList;

        if (filter == null) {
            list = Collections.emptyList();
            posMap = Collections.emptyMap();
            existsList = Collections.emptyList();
        } else if (filter instanceof PropertyFilter) {
            list = Collections.singletonList((PropertyFilter<S>) filter);
            posMap = Collections.singletonMap((PropertyFilter<S>) filter, 0);
            existsList = Collections.emptyList();
        } else {
            list = new ArrayList<PropertyFilter<S>>();
            existsList = new ArrayList<ExistsFilter<S>>();
            final List<PropertyFilter<S>> flist = list;
            final List<ExistsFilter<S>> fexistsList = existsList;

            filter.accept(new Visitor<S, Object, Object>() {
                @Override
                public Object visit(OrFilter<S> filter, Object param) {
                    throw new IllegalArgumentException("OrFilter not allowed");
                }

                @Override
                public Object visit(ExistsFilter<S> filter, Object param) {
                    fexistsList.add(filter);
                    return null;
                }

                @Override
                public Object visit(PropertyFilter<S> filter, Object param) {
                    flist.add(filter);
                    return null;
                }
            }, null);

            posMap = new HashMap<PropertyFilter<S>, Integer>();
            for (int i=0; i<list.size(); i++) {
                posMap.put(list.get(i), i);
            }

            Collections.sort(list, new PFComparator<S>());

            ((ArrayList) list).trimToSize();
            list = Collections.unmodifiableList(list);
            existsList = Collections.unmodifiableList(existsList);
        }

        plist = new PropertyFilterList<S>(list, posMap, existsList);

        synchronized (cCache) {
            cCache.put(filter, plist);
        }

        return plist;
    }

    private final List<PropertyFilter<S>> mList;
    private final Map<PropertyFilter<S>, Integer> mPosMap;
    private final List<ExistsFilter<S>> mExistsList;

    private PropertyFilterList(List<PropertyFilter<S>> list,
                               Map<PropertyFilter<S>, Integer> posMap,
                               List<ExistsFilter<S>> existsList)
    {
        mList = list;
        mPosMap = posMap;
        mExistsList = existsList;
    }

    public Integer getOriginalPosition(PropertyFilter<S> filter) {
        return mPosMap.get(filter);
    }

    @Override
    public int size() {
        return mList.size();
    }

    @Override
    public PropertyFilter<S> get(int index) {
        return mList.get(index);
    }

    /**
     * Returns a list of extracted ExistsFilters which can be and'ed together.
     */
    public List<ExistsFilter<S>> getExistsFilters() {
        return mExistsList;
    }

    private static class PFComparator<S extends Storable>
        implements Comparator<PropertyFilter<S>>, java.io.Serializable
    {
        private static final long serialVersionUID = 2322537712763223517L;

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
