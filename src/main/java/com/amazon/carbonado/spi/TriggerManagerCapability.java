/*
 * Copyright 2006-2013 Amazon Technologies, Inc. or its affiliates.
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

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.capability.Capability;
import com.amazon.carbonado.spi.TriggerManager;

/**
 * Provides the capability to get the {@link TriggerManager} from a {@link Repository}.
 * 
 * @author Pranay Dalmia
 * 
 */
public interface TriggerManagerCapability extends Capability {

    /**
     * Returns the {@link TriggerManager} for the given {@link Storable} type.
     */
    public <S extends Storable> TriggerManager<S> getTriggerManagerFor(Class<S> type) throws RepositoryException;

}
