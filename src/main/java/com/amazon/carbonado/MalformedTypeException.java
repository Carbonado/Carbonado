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

import java.util.List;

/**
 * A MalformedTypeException indicates that a {@link Storable} is defined in a
 * way that violates the requirements for Storable objects.
 *
 * @author Brian S O'Neill
 */
public class MalformedTypeException extends MalformedArgumentException {

    private static final long serialVersionUID = 5463649671507513977L;

    private final Class<?> mType;

    public MalformedTypeException(Class<?> malformedType) {
        super();
        mType = malformedType;
    }

    public MalformedTypeException(Class<?> malformedType, String message) {
        super(message);
        mType = malformedType;
    }

    public MalformedTypeException(Class<?> malformedType, List<String> messages) {
        super(messages);
        mType = malformedType;
    }

    /**
     * Returns first message, prefixed with the malformed type.
     */
    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (mType != null) {
            message = mType.getName() + ": " + message;
        }
        return message;
    }

    public Class<?> getMalformedType() {
        return mType;
    }
}
