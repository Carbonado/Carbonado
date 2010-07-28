/*
 * Copyright 2010 Amazon Technologies, Inc. or its affiliates.
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

import java.io.Serializable;

import java.util.Collection;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;

/**
 * Defines a remote procedure which can be executed by {@link
 * RemoteProcedureCapability}. Any data within the procedure instance is
 * serialized to the remote host, and possibly the class definition
 * too. Execution might have security restrictions applied.
 *
 * <p>The RemoteProcedure instance is Serializable, and so and serializable
 * parameters can be passed with it. Storables and extra data can be sent
 * through the {@link Request} object. Any data returned by procedure
 * implementation must be sent through the {@link Reply} object.
 *
 * @param <R> reply object type
 * @param <D> request data object type
 * @author Brian S O'Neill
 */
public interface RemoteProcedure<R, D> extends Serializable {
    /**
     * Request handler for remote procedure implementation.
     *
     * @param repo repository as seen by host that procedure is running from
     * @param request non-null request object
     * @return false if request is still active when this method returns;
     * request must eventually be explicitly finished
     */
    boolean handleRequest(Repository repo, Request<R, D> request) throws RepositoryException;

    /**
     * Client-side call into a remote procedure. To avoid leaking resources,
     * the finish method must be invoked or all reply data be fully read. If an
     * exception is thrown by a method defined in this interface, resources are
     * automatically released.
     *
     * @param <R> reply object type
     * @param <D> request data object type
     * @see RemoteProcedureCapability#beginCall
     */
    public static interface Call<R, D> {
        /**
         * Send data to the remote procedure.
         *
         * @return this Call instance
         * @throws IllegalArgumentException if data is null
         * @throws IllegalStateException if a call has been executed
         */
        Call<R, D> send(D data) throws RepositoryException;

        /**
         * Send all data from the given cursor to the remote procedure.
         *
         * @return this Call instance
         * @throws IllegalArgumentException if data is null
         * @throws IllegalStateException if a call has been executed
         */
        Call<R, D> sendAll(Cursor<? extends D> cursor) throws RepositoryException;

        /**
         * Reset the internal object stream of the call, allowing cached
         * objects to get freed.
         *
         * @return this Call instance
         * @throws IllegalStateException if a call has been executed
         */
        Call<R, D> reset() throws RepositoryException;

        /**
         * Flushes all the data sent so far. Flush is invoked automatically
         * when call is executed.
         */
        void flush() throws RepositoryException;

        /**
         * Executes the call and receive a reply. Calling this method does not
         * block, but methods on the returned Cursor may block waiting for
         * data.
         *
         * @throws IllegalStateException if a call has been executed
         */
        Cursor<R> fetchReply() throws RepositoryException;

        /**
         * Executes the call without expecting a reply. Method blocks waiting
         * for procedure to finish.
         *
         * @throws IllegalStateException if a call has been executed
         */
        void execute() throws RepositoryException;

        /**
         * Executes the call without expecting a reply. Method does not block
         * waiting for procedure to finish. Asynchronous execution is not
         * allowed if the current thread is in a transaction. This is because
         * transaction ownership becomes ambiguous.
         *
         * @throws IllegalStateException if a call has been executed or if
         * current thread is in a transaction
         */
        void executeAsync() throws RepositoryException;
    }

    /**
     * Request into a remote procedure, as seen by procedure implementation. To
     * avoid leaking resources, the request or reply object must always be
     * finished. If an exception is thrown by a method defined in this
     * interface, resources are automatically released.
     *
     * @param <R> reply object type
     * @param <D> request data object type
     */
    public static interface Request<R, D> {
        /**
         * Receive data from caller.
         *
         * @return null if no more data
         */
        D receive() throws RepositoryException;

        /**
         * Receive all remaining data from caller.
         *
         * @param c collection to receive data
         * @return amount received
         */
        int receiveInto(Collection<? super D> c) throws RepositoryException;

        /**
         * Begin a reply after receiving all data. If no data is expected,
         * reply can be made without calling receive.
         *
         * @throws IllegalStateException if reply was already begun or if
         * request is finished
         */
        Reply<R> beginReply() throws RepositoryException;

        /**
         * Reply and immediately finish, without sending and data to caller.
         *
         * @throws IllegalStateException if a reply was already begun
         */
        void finish() throws RepositoryException;
    }

    /**
     * Reply from remote procedure implementation. To avoid leaking resources,
     * the finish method must always be invoked. If an exception is thrown by a
     * method defined in this interface, resources are automatically released.
     *
     * @param <R> reply object type
     */
    public static interface Reply<R> {
        /**
         * Send reply data to the caller.
         *
         * @return this Reply instance
         * @throws IllegalStateException if reply is finished
         */
        Reply<R> send(R data) throws RepositoryException;

        /**
         * Reply with all data from the given cursor to the caller.
         *
         * @return this Reply instance
         * @throws IllegalStateException if reply is finished
         */
        Reply<R> sendAll(Cursor<? extends R> cursor) throws RepositoryException;

        /**
         * Reset the internal object stream of the reply, allowing cached
         * objects to get freed.
         *
         * @return this Reply instance
         * @throws IllegalStateException if reply is finished
         */
        Reply<R> reset() throws RepositoryException;

        /**
         * Flushes all the data sent so far. Flush is invoked automatically
         * when reply is finished.
         */
        void flush() throws RepositoryException;

        /**
         * Finish the reply.
         */
        void finish() throws RepositoryException;
    }
}
