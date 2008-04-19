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
 * A hint for a dependent {@link Repository} to allow a {@link Storable} property or type
 * to be undefined in the underlying schema.  Ordinarily, if a dependent repository cannot
 * find a matching property, it throws {@link MismatchException} when the {@link Storage} is
 * first retrieved for the storable. This annotation suppresses that exception, and instead
 * makes the property or type unsupported.  Any subsequent invocation of a property access
 * method for the independent type or property will cause an UnsupportedOperationException
 * to be thrown.
 *
 * <p>One example of when this might be used would be to store a calculated
 * field in the cached representation of the object.  It is <b>not</b>
 * necessary to prevent implemented methods of the form {@literal "get<value>"}
 * from being inadvertently interpreted as properties of the storable; any
 * implementation is by definition not a property.
 *
 * <p>If a correctly matching property actually is found, then this annotation
 * is ignored and the property or type is defined as usual. If the Repository
 * finds a property whose name matches, but whose type does not match, a
 * MismatchException will be thrown regardless of this annotation.
 *
 * <p>Independent repositories completely ignore this annotation.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     <b>&#64;Independent</b>
 *     String getName();
 *     void setName(String name);
 *
 *     ...
 * }
 * </pre>
 *
 * <b>Note:</b> If a {@link Version versioned} Storable with an independent
 * property is managed by a replicating repository, updates which modify just
 * the independent property still update the master Storable, in order to get a
 * new record version. Therefore, independent properties should not be used as
 * a performance enhancement which avoids writes to a master repository.
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Independent {
}
