/*
 * Copyright (c) 2013 Amazon.com Inc. All Rights Reserved.
 * AMAZON.COM CONFIDENTIAL
 */

package com.amazon.carbonado.util;

/**
 * Exception throw when attempting to perform an unavailable operation on an
 * object undergoing belated creation.
 *
 * @see com.amazon.carbonado.util.BelatedCreator;
 *
 * @author Jesse Morgan (morganjm)
 */
public class BelatedCreationException extends IllegalStateException {
    /**
     * Create a new exception with the given message.
     *
     * @param message The exception message.
     */
    public BelatedCreationException(String message) {
        super(message);
    }

    /**
     * Create a new exception with the given message and cause.
     *
     * @param message The exception message.
     * @param cause The cause of the exception.
     */
    public BelatedCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
