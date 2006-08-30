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
package com.amazon.carbonado.synthetic;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.FetchException;

/**
 * An synthetic reference contains information which is directly related to another
 * storable, and which requires that the other storable is retrievable.  This is a
 * convenience class which unifies the builder and the repository information.
 *
 * @author Don Schneider
 */
public class SyntheticReferenceAccessor<S extends Storable>  {
    private Repository mIndexEntryRepository;
    private Storage mIndexEntryStorage;
    private Class<? extends Storable> mIndexEntryClass;

    private SyntheticStorableReferenceBuilder<S> mBuilder;


    public SyntheticReferenceAccessor(Repository repo,
                                      SyntheticStorableReferenceBuilder<S> builder)
            throws RepositoryException
    {
        mBuilder = builder;
        mIndexEntryClass = mBuilder.getStorableClass();
        mIndexEntryStorage = repo.storageFor(mIndexEntryClass);
        mIndexEntryRepository = repo;
    }

    /**
     * @return repository in which this index entry is stored
     */
    public Repository getRepository() {
        return mIndexEntryRepository;
    }

    /**
     * @return storage for this index entry
     * @throws SupportException
     * @throws RepositoryException
     */
    public Storage getStorage()
            throws SupportException, RepositoryException
    {
        return mIndexEntryStorage;
    }

    /**
     * Retrieve the object for which this index entry was constructed
     * @param indexEntry
     * @throws FetchException
     */
    public Storable loadMaster(Storable indexEntry)
            throws FetchException
    {
        return mBuilder.loadMaster(indexEntry);
    }

    /**
     * Creates an index entry storable and fills in all related properties from a master storable.
     * @return the filled in metadata storable
     */
    public Storable setAllProperties(S master)
            throws SupportException, RepositoryException
    {
        Storable entry = getStorage().prepare();
        mBuilder.setAllProperties(entry, master);
        return entry;
    }

    public Class getSyntheticClass() {
        return mIndexEntryClass;
    }

    public String toString() {
        return mBuilder.getClass().toString();
    }
}
