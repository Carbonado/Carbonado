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

package com.amazon.carbonado.lob;

/**
 * Marker interface for {@link Blob Blobs} and {@link Clob Clobs}.
 *
 * @author Brian S O'Neill
 */
public interface Lob {
    /**
     * Returns an object which identifies the Lob data, which may be null if
     * not supported.
     *
     * @since 1.2
     */
    Object getLocator();

    /**
     * Two Lobs are considered equal if the object instances are the same or if
     * they point to the same content. Lob data is not compared, as that would
     * be expensive or it may result in a fetch exception.
     */
    boolean equals(Object obj);
}
