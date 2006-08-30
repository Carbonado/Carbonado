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

package com.amazon.carbonado.info;

import java.util.Set;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.util.Appender;

/**
 * Represents a primary or alternate key of a specific {@link Storable} type.
 *
 * @author Brian S O'Neill
 * @see StorableIntrospector
 */
public interface StorableKey<S extends Storable> extends Appender {
    /**
     * Returns true if this key is primary, false if an alternate.
     */
    boolean isPrimary();

    /**
     * Returns all the properties of the key in a properly ordered,
     * unmodifiable set.
     */
    Set<? extends OrderedProperty<S>> getProperties();
}
