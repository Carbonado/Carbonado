/*
 * Copyright 2006-2013 Amazon Technologies, Inc. or its affiliates.
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
