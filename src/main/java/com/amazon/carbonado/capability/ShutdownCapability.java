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

package com.amazon.carbonado.capability;

/**
 * Capability for repositories that require special attention with the Java
 * virtual machine exits.
 *
 * @author Brian S O'Neill
 */
public interface ShutdownCapability extends Capability {
    /**
     * Returns true if repository has a shutdown hook registered to
     * automatically call shutdown when the virtual machine exits.
     */
    boolean isAutoShutdownEnabled();

    /**
     * Request to enable or disable the automatic shutdown hook. Repository may
     * ignore this request if shutdown is in progress.
     *
     * @throws SecurityException if caller does not have permission
     */
    void setAutoShutdownEnabled(boolean enabled);

    /**
     * Similar to calling close on a repository, except should only be called
     * when the virtual machine is in the process of shutting down. Calling
     * close may cause spurious exceptions to be thrown by other threads which
     * may be interacting with the repository. Shutdown tries to reduce these
     * exceptions from being thrown by effectively <i>suspending</i> any
     * threads which continue to interact with this repository. <b>For this
     * reason, this method should only ever be called during a virtual machine
     * shutdown.</b>
     *
     * <p>Repositories may choose to implement this method by simply calling
     * close. There is no guarantee that shutdown will reduce exceptions, and
     * it might not suspend any threads. Also, repositories that require proper
     * shutdown should automatically register runtime hooks, and so this method
     * usually doesn't need to be called manually.
     *
     * @throws SecurityException if caller does not have permission
     */
    void shutdown();
}
