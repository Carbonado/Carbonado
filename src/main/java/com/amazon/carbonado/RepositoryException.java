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

import java.util.Random;

/**
 * General checked exception thrown when accessing a {@link Repository}.
 *
 * <p>Some repository exceptions are the result of an optimistic lock failure
 * or deadlock. One resolution strategy is to exit all transactions and try the
 * operation again, after waiting some bounded random amount of time. As a
 * convenience, this class provides a mechanism to support such a backoff
 * strategy. For example:
 *
 * <pre>
 * // Retry at most three more times
 * for (int retryCount = 3;;) {
 *     try {
 *         ...
 *         myObject.load();
 *         ...
 *         myObject.update();
 *         break;
 *     } catch (OptimisticLockException e) {
 *         // Wait up to one second before retrying
 *         retryCount = e.backoff(e, retryCount, 1000);
 *     }
 * }
 * </pre>
 *
 * If the retry count is zero (or less) when backoff is called, then the
 * original exception is rethrown, indicating retry failure.
 *
 * @author Brian S O'Neill
 */
public class RepositoryException extends Exception {

    private static final long serialVersionUID = 7261406895435249366L;

    /**
     * One strategy for resolving an optimistic lock failure is to try the
     * operation again, after waiting some bounded random amount of time. This
     * method is provided as a convenience, to support such a random wait.
     * <p>
     * A retry count is required as well, which is decremented and returned by
     * this method. If the retry count is zero (or less) when this method is
     * called, then this exception is thrown again, indicating retry failure.
     *
     * @param retryCount current retry count, if zero, throw this exception again
     * @param milliseconds upper bound on the random amount of time to wait
     * @return retryCount minus one
     * @throws E if retry count is zero
     */
    public static <E extends Throwable> int backoff(E e, int retryCount, int milliseconds)
        throws E
    {
        if (retryCount <= 0) {
            // Workaround apparent compiler bug.
            com.amazon.carbonado.util.ThrowUnchecked.fire(e);
        }
        if (milliseconds > 0) {
            Random rnd = cRandom;
            if (rnd == null) {
                cRandom = rnd = new Random();
            }
            if ((milliseconds = rnd.nextInt(milliseconds)) > 0) {
                try {
                    Thread.sleep(milliseconds);
                } catch (InterruptedException e2) {
                }
                return retryCount - 1;
            }
        }
        Thread.yield();
        return retryCount - 1;
    }

    private static Random cRandom;

    public RepositoryException() {
        super();
    }

    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public RepositoryException(Throwable cause) {
        super(cause);
    }

    /**
     * Recursively calls getCause, until the root cause is found. Returns this
     * if no root cause.
     */
    public Throwable getRootCause() {
        Throwable cause = this;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * Converts RepositoryException into an appropriate PersistException.
     */
    public final PersistException toPersistException() {
        return toPersistException(null);
    }

    /**
     * Converts RepositoryException into an appropriate PersistException, prepending
     * the specified message. If message is null, original exception message is
     * preserved.
     *
     * @param message message to prepend, which may be null
     */
    public final PersistException toPersistException(final String message) {
        Throwable cause;
        if (this instanceof PersistException) {
            cause = this;
        } else {
            cause = getCause();
        }

        if (cause == null) {
            cause = this;
        } else if (cause instanceof PersistException && message == null) {
            return (PersistException) cause;
        }

        String causeMessage = cause.getMessage();
        if (causeMessage == null) {
            causeMessage = message;
        } else if (message != null) {
            causeMessage = message + " : " + causeMessage;
        }

        return makePersistException(causeMessage, cause);
    }

    /**
     * Converts RepositoryException into an appropriate FetchException.
     */
    public final FetchException toFetchException() {
        return toFetchException(null);
    }

    /**
     * Converts RepositoryException into an appropriate FetchException, prepending
     * the specified message. If message is null, original exception message is
     * preserved.
     *
     * @param message message to prepend, which may be null
     */
    public final FetchException toFetchException(final String message) {
        Throwable cause;
        if (this instanceof FetchException) {
            cause = this;
        } else {
            cause = getCause();
        }

        if (cause == null) {
            cause = this;
        } else if (cause instanceof FetchException && message == null) {
            return (FetchException) cause;
        }

        String causeMessage = cause.getMessage();
        if (causeMessage == null) {
            causeMessage = message;
        } else if (message != null) {
            causeMessage = message + " : " + causeMessage;
        }

        return makeFetchException(causeMessage, cause);
    }

    /**
     * Subclasses can override this to provide a more specialized exception.
     *
     * @param message exception message, which may be null
     * @param cause non-null cause
     */
    protected PersistException makePersistException(String message, Throwable cause) {
        return new PersistException(message, cause);
    }

    /**
     * Subclasses can override this to provide a more specialized exception.
     *
     * @param message exception message, which may be null
     * @param cause non-null cause
     */
    protected FetchException makeFetchException(String message, Throwable cause) {
        return new FetchException(message, cause);
    }
}
