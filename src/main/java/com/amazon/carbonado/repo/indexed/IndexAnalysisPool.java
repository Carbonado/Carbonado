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

package com.amazon.carbonado.repo.indexed;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.util.AbstractPool;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class IndexAnalysisPool extends AbstractPool<Storage, IndexAnalysis, RepositoryException> {
    private final IndexedRepository mRepository;

    public IndexAnalysisPool(IndexedRepository repository) {
        mRepository = repository;
    }

    protected IndexAnalysis create(Storage masterStorage) throws RepositoryException {
        return new IndexAnalysis(mRepository, masterStorage);
    }
}
