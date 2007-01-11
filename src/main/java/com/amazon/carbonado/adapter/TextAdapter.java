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

package com.amazon.carbonado.adapter;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.ByteArrayBlob;
import com.amazon.carbonado.lob.Clob;
import com.amazon.carbonado.lob.StringClob;

import com.amazon.carbonado.adapter.AdapterDefinition;

/**
 * Converts database Blobs and Clobs to Strings. This is suitable for text
 * values which are expected to fit entirely in memory. The storage layer will
 * attempt to store the text as a regular string, but will use a blob type if
 * required to.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     <b>&#64;TextAdapter(charset="UTF-8")</b>
 *     String getWelcomeMessage();
 *     void setWelcomeMessage(String message);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see Clob
 * @see Blob
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@AdapterDefinition(storageTypePreferences={
    String.class,
    Clob.class,
    Blob.class
})
public @interface TextAdapter {
    /**
     * Optionally specify a character set, which is used if the storage type is
     * a Blob.
     */
    String charset() default "UTF-8";

    /**
     * Optionally specify alternate character sets, which are used if text
     * cannot be decoded with primary charset.
     */
    String[] altCharsets() default {};

    /**
     * Adapter implementation for {@link TextAdapter}.
     */
    public static class Adapter {
        private static byte[] EMPTY_BYTE_ARRAY = new byte[0];

        static PersistException toPersistException(IOException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PersistException) {
                return (PersistException) cause;
            }
            if (cause == null) {
                cause = e;
            }
            return new PersistException(cause);
        }

        private static final Charset[] prepareAltCharsets(TextAdapter ann) {
            if (ann == null) {
                return null;
            }
            String[] strs = ann.altCharsets();
            if (strs == null || strs.length == 0) {
                return null;
            }
            Charset[] altCharsets = new Charset[strs.length];
            for (int i=0; i<strs.length; i++) {
                altCharsets[i] = Charset.forName(strs[i]);
            }
            return altCharsets;
        }

        private final Charset mCharset;
        private final Charset[] mAltCharsets;

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with adapter
         * @param ann specific annotation that binds to this adapter class
         */
        public Adapter(Class<?> type, String propertyName, TextAdapter ann) {
            this(type, propertyName,
                 (ann == null ? null : Charset.forName(ann.charset())),
                 prepareAltCharsets(ann));
        }

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with
         * @param charset character set to use, or null to use default of UTF-8.
         */
        public Adapter(Class<?> type, String propertyName, String charset) {
            this(type, propertyName, charset == null ? null : Charset.forName(charset), null);
        }

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with
         * @param charset character set to use, or null to use default of UTF-8.
         */
        public Adapter(Class<?> type, String propertyName, Charset charset) {
            this(type, propertyName, charset, null);
        }

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with
         * @param charset character set to use, or null to use default of UTF-8.
         * @param altCharsets alternate character sets to use, if text cannot be
         * decoded with primary charset
         */
        public Adapter(Class<?> type, String propertyName, Charset charset, Charset[] altCharsets)
        {
            mCharset = charset == null ? Charset.forName("UTF-8") : charset;
            if (altCharsets == null || altCharsets.length == 0) {
                altCharsets = null;
            } else {
                altCharsets = altCharsets.clone();
            }
            mAltCharsets = altCharsets;
        }

        // Adapt to String...

        public String adaptToString(Clob clob) throws FetchException {
            return clob == null ? null : clob.asString();
        }

        public String adaptToString(Blob blob) throws FetchException {
            FetchException error;

            try {
                return blob == null ? null : blob.asString(mCharset);
            } catch (FetchException e) {
                if (mAltCharsets == null) {
                    throw e;
                }
                Throwable cause = e.getCause();
                if (!(cause instanceof CharacterCodingException)) {
                    throw e;
                }
                error = e;
            }

            for (int i=0; i<mAltCharsets.length; i++) {
                try {
                    return blob.asString(mAltCharsets[i]);
                } catch (FetchException e) {
                    Throwable cause = e.getCause();
                    if (!(cause instanceof CharacterCodingException)) {
                        throw e;
                    }
                }
            }

            throw error;
        }

        // Adapt from String...

        public Clob adaptToClob(String text) {
            if (text == null) {
                return null;
            }
            return new StringClob(text);
        }

        public Blob adaptToBlob(String text) throws PersistException {
            if (text == null) {
                return null;
            }

            if (text.length() == 0) {
                return new ByteArrayBlob(EMPTY_BYTE_ARRAY);
            }

            CharsetEncoder encoder = mCharset.newEncoder();

            long byteLength = (long) (text.length() * encoder.averageBytesPerChar());

            if (byteLength >= Integer.MAX_VALUE) {
                byteLength = Integer.MAX_VALUE;
            }

            byte[] buffer = new byte[(int) byteLength];

            Blob blob = new ByteArrayBlob(buffer, 0);

            Writer w = new OutputStreamWriter(blob.openOutputStream(), encoder);
            try {
                try {
                    w.write(text, 0, text.length());
                } finally {
                    w.close();
                }
            } catch (IOException e) {
                throw toPersistException(e);
            }

            return blob;
        }
    }
}
