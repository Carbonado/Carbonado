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

import java.lang.annotation.*;

/**
 * Identifies that a {@link Storable} property can have a null value. By
 * default, all Storable properties are required to have a non-null value. It
 * is illegal to declare a property as nullable whose type is a primitive
 * non-object.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     <b>&#64;Nullable</b>
 *     String getName();
 *     void setName(String name);
 *
 *     ...
 * }
 * </pre>
 *
 * <p>If the repository does not allow a property to be declared as nullable
 * because the underlying schema differs, it can be also annotated as {@link
 * Independent}. This makes it easier for a common set of Storables to interact
 * with schemas which are slightly different. Attempting to persist null into a
 * property for which null is not allowed will likely result in a constraint
 * exception.
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Nullable {
}
