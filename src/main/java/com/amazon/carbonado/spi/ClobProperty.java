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

package com.amazon.carbonado.spi;

import java.io.IOException;

import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;

import com.amazon.carbonado.lob.Clob;

/**
 *
 *
 * @author Brian S O'Neill
 * @see LobEngine
 * @see LobEngineTrigger
 */
class ClobProperty extends LobProperty<Clob> {
    ClobProperty(LobEngine engine, String propertyName) {
        super(engine, propertyName);
    }

    @Override
    Clob createNewLob(int blockSize) throws PersistException {
        return mEngine.createNewClob(blockSize);
    }

    @Override
    void setLobValue(long locator, Clob data) throws PersistException {
        try {
            mEngine.setClobValue(locator, data);
        } catch (IOException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RepositoryException) {
                throw ((RepositoryException) cause).toPersistException();
            }
            throw new PersistException(e);
        }
    }
}
