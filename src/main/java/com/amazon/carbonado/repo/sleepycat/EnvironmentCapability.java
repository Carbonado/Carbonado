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

import java.io.File;

import com.amazon.carbonado.capability.Capability;

/**
 * Capability to provide direct access to the underlying BDB environment.
 *
 * @author Brian S O'Neill
 */
public interface EnvironmentCapability extends Capability {
    /**
     * Returns the BDB environment object, which must be cast to the expected
     * type, depending on the BDB product and version being used.
     */
    Object getEnvironment();

    BDBProduct getBDBProduct();

    /**
     * Returns the major, minor, and patch version numbers.
     */
    int[] getVersion();

    /**
     * Returns the home directory for the BDB environment.
     */
    File getHome();

    /**
     * Returns the directory where data files are stored, which is the same as
     * the home directory by default.
     */
    File getDataHome();
}
