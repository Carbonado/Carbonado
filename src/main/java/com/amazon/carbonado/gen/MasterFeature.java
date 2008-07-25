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

package com.amazon.carbonado.gen;

/**
 * Master feature to enable when using {@link MasterStorableGenerator}.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public enum MasterFeature {
    /** Insert and update operations implement record versioning, if version property exists. */
    VERSIONING,

    /** Insert and update operations normalize property types such as BigDecimal. */
    NORMALIZE,

    /** Update operations load clean copy first, to prevent destructive update. */
    UPDATE_FULL,

    /** Ensure update operation always is in a transaction. */
    UPDATE_TXN,

    /** Ensure update operation always is in a transaction, "for update". */
    UPDATE_TXN_FOR_UPDATE,

    /** Insert operation applies any sequences to unset properties. */
    INSERT_SEQUENCES,

    /**
     * Insert operation checks that all required data properties have been set,
     * excluding automatic properties and version property.
     */
    INSERT_CHECK_REQUIRED,

    /** Ensure insert operation always is in a transaction. */
    INSERT_TXN,

    /** Ensure insert operation always is in a transaction, "for update". */
    INSERT_TXN_FOR_UPDATE,

    /** Ensure delete operation always is in a transaction. */
    DELETE_TXN,

    /** Ensure delete operation always is in a transaction, "for update". */
    DELETE_TXN_FOR_UPDATE,
}
