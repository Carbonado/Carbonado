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

package com.amazon.carbonado.spi;

import com.amazon.carbonado.MalformedTypeException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.util.AbstractPool;

/**
 * A concurrent pool of strongly referenced Storage instances mapped by
 * Storable type. Storage instances are lazily created and pooled.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public abstract class StoragePool
    extends AbstractPool<Class, Storage, RepositoryException>
{
    public StoragePool() {
    }

    /**
     * Returns a Storage instance for the given Storable type, which is lazily
     * created and pooled. If multiple threads are requesting upon the same type
     * concurrently, at most one thread attempts to lazily create the
     * Storage. The others wait for it to become available.
     */
    public <S extends Storable> Storage<S> getStorage(Class<S> type)
        throws MalformedTypeException, SupportException, RepositoryException
    {
        return (Storage<S>) super.get(type);
    }

    @Override
    protected final Storage create(Class type) throws SupportException, RepositoryException {
        return createStorage(type);
    }

    protected abstract <S extends Storable> Storage<S> createStorage(Class<S> type)
        throws SupportException, RepositoryException;
}
