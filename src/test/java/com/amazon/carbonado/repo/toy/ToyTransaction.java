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

package com.amazon.carbonado.repo.toy;

import java.util.concurrent.TimeUnit;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Transaction;

/**
 *
 * @author Brian S O'Neill
 */
public class ToyTransaction implements Transaction {
    public void commit() throws PersistException {
    }

    public void exit() throws PersistException {
    }

    public void setForUpdate(boolean forUpdate) {
    }

    public boolean isForUpdate() {
        return false;
    }

    public void setDesiredLockTimeout(int timeout, TimeUnit unit) {
    }

    public IsolationLevel getIsolationLevel() {
        throw new UnsupportedOperationException();
    }
}
