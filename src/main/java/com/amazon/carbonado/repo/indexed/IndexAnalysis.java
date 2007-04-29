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

package com.amazon.carbonado.repo.indexed;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.qe.FilteringScore;

import com.amazon.carbonado.spi.StorableIndexSet;

/**
 * Collection of static methods which perform index analysis.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
class IndexAnalysis {
    static <S extends Storable> StorableIndexSet<S> gatherDesiredIndexes(StorableInfo<S> info) {
        StorableIndexSet<S> indexSet = new StorableIndexSet<S>();
        indexSet.addIndexes(info);
        indexSet.addAlternateKeys(info);

        // If any join properties are used by indexed derived properties, make
        // sure join internal properties are indexed.

        for (StorableProperty<S> property : info.getAllProperties().values()) {
            if (!isJoinAndUsedByIndexedDerivedProperty(property)) {
                continue;
            }

            // Internal properties of join need to be indexed. Check if a
            // suitable index exists before defining a new one.

            Filter<S> filter = Filter.getOpenFilter(info.getStorableType());
            for (int i=property.getJoinElementCount(); --i>=0; ) {
                filter = filter.and(property.getInternalJoinElement(i).getName(), RelOp.EQ);
            }

            for (int i=info.getIndexCount(); --i>=0; ) {
                FilteringScore<S> score = FilteringScore.evaluate(info.getIndex(i), filter);
                if (score.getIdentityCount() == property.getJoinElementCount()) {
                    // Suitable index already exists.
                    continue;
                }
            }

            Direction[] directions = new Direction[property.getJoinElementCount()];
            Arrays.fill(directions, Direction.UNSPECIFIED);

            StorableIndex<S> index =
                new StorableIndex<S>(property.getInternalJoinElements(), directions);

            indexSet.add(index);
        }

        return indexSet;
    }

    static boolean isUsedByIndex(StorableProperty<?> property) {
        StorableInfo<?> info = StorableIntrospector.examine(property.getEnclosingType());
        for (int i=info.getIndexCount(); --i>=0; ) {
            StorableIndex<?> index = info.getIndex(i);
            int propertyCount = index.getPropertyCount();
            for (int j=0; j<propertyCount; j++) {
                if (index.getProperty(j).equals(property)) {
                    return true;
                }
            }
        }
        return false;
    }

    static boolean isJoinAndUsedByIndexedDerivedProperty(StorableProperty<?> property) {
        if (property.isJoin()) {
            for (ChainedProperty<?> derivedTo : property.getDerivedToProperties()) {
                if (isUsedByIndex(derivedTo.getPrimeProperty())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns derived-to properties in external storables that are used by indexes.
     *
     * @return null if none
     */
    static Set<ChainedProperty<?>> gatherDerivedToDependencies(StorableInfo<?> info) {
        Set<ChainedProperty<?>> set = null;
        for (StorableProperty<?> property : info.getAllProperties().values()) {
            for (ChainedProperty<?> derivedTo : property.getDerivedToProperties()) {
                if (derivedTo.getChainCount() > 0 && isUsedByIndex(derivedTo.getPrimeProperty())) {
                    if (set == null) {
                        set = new HashSet<ChainedProperty<?>>();
                    }
                    set.add(derivedTo);
                }
            }
        }
        return set;
    }
}
