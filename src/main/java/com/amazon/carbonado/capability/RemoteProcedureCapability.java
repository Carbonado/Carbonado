/*
 * Copyright 2010-2012 Amazon Technologies, Inc. or its affiliates.
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

import com.amazon.carbonado.RepositoryException;

/**
 * Capability which allows arbitrary code to run on a remote host and access
 * its repository. The remote procedure might have security restrictions
 * applied to it.
 *
 * <p>Examples:<pre>
 * RemoteProcedureCapability cap = ...
 * Cursor&lt;MyRecord&gt; c1 = cap.beginCall(new CustomQuery&lt;MyRecord&gt;(params)).fetchReply();
 * ...
 *
 * Cursor&lt;InputRecord&gt; c2 = ...
 * cap.beginCall(new Importer&lt;InputRecord&gt;()).sendAll(c2).finish();
 * </pre>
 *
 * @author Brian S O'Neill
 * @see RemoteProcedure
 */
public interface RemoteProcedureCapability extends Capability {
    /**
     * Begins a call to execute the given procedure on a remote host.
     * Execution commences when the Call object is instructed to do so.
     *
     * @param <R> reply object type
     * @param <D> request data object type
     * @param proc procedure to execute
     * @return object for defining the call and receiving a reply
     */
    <R, D> RemoteProcedure.Call<R, D> beginCall(RemoteProcedure<R, D> proc)
        throws RepositoryException;
}
