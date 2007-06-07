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

import java.util.Collections;
import java.util.List;

/**
 * Indicates that a {@link Storable} doesn't exactly match up with an external
 * schema. This exception may only be thrown by repositories with a dependency
 * on an external schema.
 *
 * @author Brian S O'Neill
 */
public class MismatchException extends SupportException {

    private static final long serialVersionUID = 5840495857407789424L;

    private Class<?> mType;
    private List<String> mMessages;

    public MismatchException() {
        super();
        mMessages = null;
    }

    public MismatchException(String message) {
        super(message);
        mMessages = null;
    }

    public MismatchException(List<String> messages) {
        super();
        mMessages = Collections.unmodifiableList(messages);
    }

    /**
     * @since 1.2
     */
    public MismatchException(Class<?> malformedType) {
        this();
        mType = malformedType;
    }

    /**
     * @since 1.2
     */
    public MismatchException(Class<?> malformedType, String message) {
        this(message);
        mType = malformedType;
    }

    /**
     * @since 1.2
     */
    public MismatchException(Class<?> malformedType, List<String> messages) {
        this(messages);
        mType = malformedType;
    }

    @Override
    public String getMessage() {
        String message;
        if (mMessages == null || mMessages.size() == 0) {
            message = super.getMessage();
        } else {
            message = mMessages.get(0);
        }
        if (mType != null) {
            message = mType.getName() + ": " + message;
        }
        return message;
    }

    /**
     * Multiple error messages may be embedded in a MismatchException.
     *
     * @return non-null, unmodifiable list of messages
     */
    public List<String> getMessages() {
        if (mMessages == null || mMessages.size() == 0) {
            mMessages = Collections.singletonList(super.getMessage());
        }
        return mMessages;
    }
}
