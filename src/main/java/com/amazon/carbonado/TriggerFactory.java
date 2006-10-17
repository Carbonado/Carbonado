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

package com.amazon.carbonado;

/**
 * Can be used with {@link RepositoryBuilder} to automatically register
 * triggers as Storable types become available.
 *
 * @author Brian S O'Neill
 */
public interface TriggerFactory {
    /**
     * Return an appropriate trigger for the given type, or null if none. This
     * method is expected to be called at most once per Storable type. As an
     * extra safeguard, trigger implementations are encouraged to implement the
     * equals method.
     *
     * @param type Storable type requesting an automatic trigger
     * @return trigger instance or null if not applicable
     */
    <S extends Storable> Trigger<? super S> triggerFor(Class<S> type) throws RepositoryException;
}
