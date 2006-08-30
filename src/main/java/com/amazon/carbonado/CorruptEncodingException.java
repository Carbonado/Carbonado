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
 * A CorruptEncodingException is caused when decoding an encoded record fails.
 *
 * @author Brian S O'Neill
 */
public class CorruptEncodingException extends FetchException {

    private static final long serialVersionUID = 4543503149683482362L;

    public CorruptEncodingException() {
        super();
    }

    public CorruptEncodingException(String message) {
        super(message);
    }

    public CorruptEncodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public CorruptEncodingException(Throwable cause) {
        super(cause);
    }

    /**
     * @param expectedGeneration expected layout generation of decoded storable
     * @param actualGeneration actual layout generation of decoded storable
     */
    public CorruptEncodingException(int expectedGeneration, int actualGeneration) {
        super("Expected layout generation of " + expectedGeneration +
              ", but actual layout generation was " + actualGeneration);
    }
}
