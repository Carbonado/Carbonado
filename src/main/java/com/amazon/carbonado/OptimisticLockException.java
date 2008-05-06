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

/**
 * An OptimisticLockException is thrown if the {@link Repository} is using
 * optimistic locking for concurrency control, and lock aquisition failed.
 * This exception may also be thrown if multiversion concurrency control (MVCC)
 * is being used and the commit fails.
 *
 * @author Brian S O'Neill
 */
public class OptimisticLockException extends PersistException {

    private static final long serialVersionUID = 4081788711829580886L;

    private final transient Storable mStorable;

    public OptimisticLockException() {
        super();
        mStorable = null;
    }

    public OptimisticLockException(String message) {
        super(message);
        mStorable = null;
    }

    public OptimisticLockException(String message, Throwable cause) {
        super(message, cause);
        mStorable = null;
    }

    public OptimisticLockException(Throwable cause) {
        super(cause);
        mStorable = null;
    }

    /**
     * @param expectedVersion version number that was expected for persistent
     * record when update was executed
     */
    public OptimisticLockException(long expectedVersion) {
        this((Long) expectedVersion);
    }

    /**
     * @param expectedVersion version number that was expected for persistent
     * record when update was executed
     */
    public OptimisticLockException(Object expectedVersion) {
        this(expectedVersion, null);
    }

    /**
     * @param expectedVersion version number that was expected for persistent
     * record when update was executed
     * @param savedVersion actual persistent version number of storable
     */
    public OptimisticLockException(Object expectedVersion, Object savedVersion) {
        this(expectedVersion, savedVersion, null);
    }

    /**
     * @param expectedVersion version number that was expected for persistent
     * record when update was executed
     * @param savedVersion actual persistent version number of storable
     * @param s Storable which was acted upon
     */
    public OptimisticLockException(Object expectedVersion, Object savedVersion, Storable s) {
        super(makeMessage(expectedVersion, savedVersion, s));
        mStorable = s;
    }

    /**
     * Construct exception for when new version was expected to have increased.
     *
     * @param savedVersion actual persistent version number of storable
     * @param s Storable which was acted upon
     * @param newVersion new version which was provided
     * @since 1.2
     */
    public OptimisticLockException(Object savedVersion, Storable s, Object newVersion) {
        super(makeMessage(savedVersion, s, newVersion));
        mStorable = s;
    }

    /**
     * Returns the Storable which was acted upon, or null if not available.
     */
    public Storable getStorable() {
        return mStorable;
    }

    private static String makeMessage(Object expectedVersion, Object savedVersion, Storable s) {
        String message;
        if (expectedVersion == null && savedVersion == null) {
            message = null;
        } else {
            message = "Update acted on version " + expectedVersion +
                ", but canonical version is " + savedVersion;
        }

        if (s != null) {
            if (message == null) {
                message = s.toStringKeyOnly();
            } else {
                message = message + ": " + s.toStringKeyOnly();
            }
        }

        return message;
    }

    private static String makeMessage(Object savedVersion, Storable s, Object newVersion) {
        String message;
        if (savedVersion == null && newVersion == null) {
            message = "New version is not larger than existing version";
        } else {
            message = "New version of " + newVersion +
                " is not larger than existing version of " + savedVersion;
        }

        if (s != null) {
            message = message + ": " + s.toStringKeyOnly();
        }

        return message;
    }
}
