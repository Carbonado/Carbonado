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
 * A MalformedArgumentException is thrown after detailed analysis on an
 * argument determined it was not suitable. This class is abstract to prevent
 * its direct use. Subclasses are encouraged to provide more detail as to the
 * cause of the exception.
 *
 * @author Brian S O'Neill
 */
public abstract class MalformedArgumentException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private List<String> mMessages;

    protected MalformedArgumentException() {
        super();
    }

    protected MalformedArgumentException(String message) {
        super(message);
    }

    protected MalformedArgumentException(List<String> messages) {
        super();
        mMessages = Collections.unmodifiableList(messages);
    }

    @Override
    public String getMessage() {
        if (mMessages == null || mMessages.size() == 0) {
            return super.getMessage();
        }
        return mMessages.get(0);
    }

    /**
     * Multiple error messages may be embedded in a MalformedArgumentException.
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
