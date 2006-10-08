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

package com.amazon.carbonado.repo.sleepycat;

/**
 * Set of supported BDB products.
 *
 * @author Brian S O'Neill
 */
public enum BDBProduct {
    /** BDB Native, legacy API */
    DB_Legacy,

    /** BDB Native */
    DB,

    /** BDB Native, High Availability */
    DB_HA,

    /** BDB Java Edition */
    JE;

    public static BDBProduct forString(String name) {
        name = name.toLowerCase();
	if (name.equals("db_legacy")) {
	    return DB_Legacy;
	} else if (name.equals("db")) {
	    return DB;
	} else if (name.equals("db_ha")) {
	    return DB_HA;
	} else if (name.equals("je")) {
	    return JE;
	}
        throw new IllegalArgumentException("Unsupported product: " + name);
    }
}
