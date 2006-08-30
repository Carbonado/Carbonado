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

package com.amazon.carbonado.layout;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.capability.Capability;

/**
 * Capability to get layout information on any storable generation.
 *
 * @author Brian S O'Neill
 */
public interface LayoutCapability extends Capability {
    /**
     * Returns the layout matching the current definition of the given type.
     *
     * @throws PersistException if type represents a new generation, but
     * persisting this information failed
     */
    public Layout layoutFor(Class<? extends Storable> type)
        throws FetchException, PersistException;

    /**
     * Returns the layout for a particular generation of the given type.
     *
     * @param generation desired generation
     * @throws FetchNoneException if generation not found
     */
    public Layout layoutFor(Class<? extends Storable> type, int generation)
        throws FetchException, FetchNoneException;
}
