/*
 * Copyright 2006-2012 Amazon Technologies, Inc. or its affiliates.
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

import java.lang.annotation.*;

/**
 * Indicates that all accesses to this {@link Storable} must come from an
 * authoritative source. When reading or writing the Storable, the {@link
 * Repository} must guarantee that it is operating on the latest, correct
 * version of the Storable.
 *
 * <p>Repositories that cache potentially stale Storables are required to
 * ensure the cache is always up-to-date or bypass the cache
 * altogether. Replicating repositories which may have a propagation delay must
 * always access the master repository.
 *
 * <p>Repositories which provide eventual consistency but don't rely on a
 * master <i>must</i> throw {@link UnsupportedTypeException}, as there is no
 * authoritative source.
 *
 * <p>Example:<pre>
 * <b>&#64;Authoritative</b>
 * &#64;PrimaryKey("sequenceName")
 * public interface SequenceValue extends Storable&lt;SequenceValue&gt; {
 *     String getSequenceName();
 *     void setSequenceName(String name);
 *
 *     long getNextValue();
 *     void setNextValue(long value);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Authoritative {
}
