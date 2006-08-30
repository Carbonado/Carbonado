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
 * A PersistNoneException is thrown when a persist operation applied to no
 * records when at least one was expected.
 *
 * @author Brian S O'Neill
 */
public class PersistNoneException extends PersistException {

    private static final long serialVersionUID = -3271539536650048094L;

    public PersistNoneException() {
        super();
    }

    public PersistNoneException(String message) {
        super(message);
    }

    public PersistNoneException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistNoneException(Throwable cause) {
        super(cause);
    }

    @Override
    protected FetchException makeFetchException(String message, Throwable cause) {
        return new FetchNoneException(message, cause);
    }
}
