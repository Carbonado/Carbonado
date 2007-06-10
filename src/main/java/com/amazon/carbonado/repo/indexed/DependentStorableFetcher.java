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
import java.util.ArrayList;
import java.util.List;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Fetches Storables that have indexed derived-to properties which depend on S.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
class DependentStorableFetcher<S extends Storable, D extends Storable> {
    private final IndexedRepository mRepository;
    private final IndexEntryAccessor<D>[] mIndexEntryAccessors;
    private final Query<D> mQuery;
    private final String[] mJoinProperties;

    /**
     * @param derivedTo special chained property from StorableProperty.getDerivedToProperties
     */
    DependentStorableFetcher(IndexedRepository repository,
                             Class<S> sType, ChainedProperty<D> derivedTo)
        throws RepositoryException
    {
        if (derivedTo.getChainCount() == 0) {
            throw new IllegalArgumentException();
        }
        if (derivedTo.getLastProperty().getType() != sType) {
            throw new IllegalArgumentException();
        }
        if (!derivedTo.getLastProperty().isJoin()) {
            throw new IllegalArgumentException();
        }

        Class<D> dType = derivedTo.getPrimeProperty().getEnclosingType();

        // Find the indexes that contain the prime derivedTo property.
        List<IndexEntryAccessor<D>> accessorList = new ArrayList<IndexEntryAccessor<D>>();
        for (IndexEntryAccessor<D> acc : repository.getIndexEntryAccessors(dType)) {
            for (String indexPropName : acc.getPropertyNames()) {
                if (indexPropName.equals(derivedTo.getPrimeProperty().getName())) {
                    accessorList.add(acc);
                    break;
                }
            }
        }

        if (accessorList.size() == 0) {
            throw new SupportException
                ("Unable to find index accessors for derived-to property: " + derivedTo +
                 ", enclosing type: " + dType);
        }

        // Build a query on D joined to S.

        StorableProperty<S> join = (StorableProperty<S>) derivedTo.getLastProperty();

        ChainedProperty<?> base;
        if (derivedTo.getChainCount() <= 1) {
            base = null;
        } else {
            base = derivedTo.tail().trim();
        }

        int joinElementCount = join.getJoinElementCount();
        String[] joinProperties = new String[joinElementCount];

        Filter<D> dFilter = Filter.getOpenFilter(dType);
        for (int i=0; i<joinElementCount; i++) {
            StorableProperty<S> element = join.getInternalJoinElement(i);
            joinProperties[i] = element.getName();
            if (base == null) {
                dFilter = dFilter.and(element.getName(), RelOp.EQ);
            } else {
                dFilter = dFilter.and(base.append(element).toString(), RelOp.EQ);
            }
        }

        mRepository = repository;
        mIndexEntryAccessors = accessorList.toArray(new IndexEntryAccessor[accessorList.size()]);
        mQuery = repository.storageFor(dType).query(dFilter);
        mJoinProperties = joinProperties;
    }

    public Transaction enterTransaction() {
        return mRepository.enterTransaction();
    }

    public Cursor<D> fetchDependenentStorables(S storable) throws FetchException {
        Query<D> query = mQuery;
        for (String property : mJoinProperties) {
            query = query.with(storable.getPropertyValue(property));
        }
        return query.fetch();
    }

    /**
     * @return amount added to list
     */
    public int createIndexEntries(D master, List<Storable> indexEntries) {
        IndexEntryAccessor[] accessors = mIndexEntryAccessors;
        int length = accessors.length;
        for (int i=0; i<length; i++) {
            IndexEntryAccessor accessor = accessors[i];
            Storable indexEntry = accessor.getIndexEntryStorage().prepare();
            accessor.copyFromMaster(indexEntry, master);
            indexEntries.add(indexEntry);
        }
        return length;
    }

    @Override
    public int hashCode() {
        return mQuery.getFilter().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof DependentStorableFetcher) {
            DependentStorableFetcher other = (DependentStorableFetcher) obj;
            return mQuery.getFilter().equals(other.mQuery.getFilter())
                && Arrays.equals(mJoinProperties, other.mJoinProperties)
                && Arrays.equals(mIndexEntryAccessors, other.mIndexEntryAccessors);
        }
        return false;
    }

    @Override
    public String toString() {
        return "DependentStorableFetcher: {indexes=" + Arrays.toString(mIndexEntryAccessors) +
            ", query=" + mQuery +
            ", join properties=" + Arrays.toString(mJoinProperties) + '}';
    }
}
