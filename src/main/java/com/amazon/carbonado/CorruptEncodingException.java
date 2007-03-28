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

    private transient Storable mStorable;

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

    /**
     * If the decoder can at least extract the primary key, it should set it here.
     */
    public void setStorableWithPrimaryKey(Storable s) {
        if (s != null) {
            // Do this to ensure that primary key is known to be defined.
            s.markAllPropertiesClean();
        }
        mStorable = s;
    }

    /**
     * If the decoder was able to extract the primary key, it will be available
     * in the returned Storable.
     *
     * @return partial Storable with primary key defined, or null if unable to
     * decode the key
     */
    public Storable getStorableWithPrimaryKey() {
        return mStorable;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        
        if (mStorable != null) {
            message = message + "; " + mStorable.toStringKeyOnly();
        }

        return message;
    }
}
