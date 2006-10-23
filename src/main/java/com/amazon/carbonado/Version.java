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
 * <p>Support for the version property falls into three categories.  A
 * repository may manage the version; it may respect the version; or it may
 * merely check the version.
 *
 * <p><b>Manage</b>: Each storable with a version property must have one and
 * only one repository which is responsible for managing the version property.
 * That repository takes responsibility for establishing the version on insert,
 * and for auto-incrementing it on update.  Under no circumstances should the
 * version property be incremented manually; this can result in a false
 * optimistic lock exception, or worse may allow the persistent record to
 * become corrupted.  Prior to incrementing, these repositories will verify
 * that the version exactly matches the version of the current record, throwing
 * an {@link OptimisticLockException} otherwise.  The JDBC repository is the
 * canonical example of this sort of repository.
 *
 * <p><b>Respect</b>: Repositories which respect the version use the version to
 * guarantee that updates are idempotent -- that is, that an update is applied
 * once and only once.  These repositories will check that the version property
 * is strictly greater than the version of the current record, and will
 * (silently) ignore changes which fail this check.
 *
 * <p><b>Check</b>: Philosophically, a version property can be considered part
 * of the identity of the storable.  That is, if the storable has a version
 * property, it cannot be considered fully specified unless that property is
 * specified.  Thus, the minimal required support for all repositories is to
 * check that the version is specified on update.  All repositories -- even
 * those which neither check nor manage the version -- will throw an {@link
 * IllegalStateException} if the version property is not set before update.
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
