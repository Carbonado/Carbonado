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
 * Designates a {@link Storable} property as being the authoritative version
 * number for the entire Storable instance. Only one property can have this
 * designation.
 *
 * <p>Philosophically, a version property can be considered part of the
 * identity of the storable. Unless the version is {@link Derived}, the
 * repository is responsible for establishing the version on insert, and for
 * auto-incrementing it on update.  Under no circumstances should a normal
 * version property be incremented manually; this can result in a false {@link
 * OptimisticLockException}, or worse may allow the persistent record to become
 * corrupted.
 *
 * <p>When updating a storable which has a normal version property, a value for
 * the version must be specified along with its primary key.  Otherwise, an
 * {@link IllegalStateException} is thrown when calling update.  If the update
 * operation detects that the specified version doesn't exactly match the
 * version of the existing persisted storable, an {@link
 * OptimisticLockException} is thrown. For {@link Derived} versions, an {@link
 * OptimisticLockException} is thrown only if the update detects that the new
 * version hasn't incremented.
 *
 * <p>The actual type of the version property can be anything, but some
 * repositories might only support integers. For maximum portability, version
 * properties should be a regular 32-bit int.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     <b>&#64;Version</b>
 *     int getRecordVersionNumber();
 *     void setRecordVersionNumber(int version);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @author Don Schneider
 * @see OptimisticLockException
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Version {
}
