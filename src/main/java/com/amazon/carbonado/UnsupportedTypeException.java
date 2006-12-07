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
 * Thrown by a {@link Repository} which cannot support a {@link Storable} which
 * is declared as {@link Independent} or {@link Authoritative}.
 *
 * @author Brian S O'Neill
 */
public class UnsupportedTypeException extends SupportException {
    private static final long serialVersionUID = 1L;

    private final Class<? extends Storable> mType;

    public UnsupportedTypeException(Class<? extends Storable> type) {
        super("Independent type not supported: " + type.getName());
        mType = type;
    }

    public Class<? extends Storable> getType() {
        return mType;
    }
}
