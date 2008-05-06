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

import java.util.List;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.lob.Lob;

/**
 *
 *
 * @author Brian S O'Neill
 * @see LobEngine
 */
class LobEngineTrigger<S extends Storable> extends Trigger<S> {
    final LobEngine mEngine;
    private final int mBlockSize;
    private final LobProperty<Lob>[] mLobProperties;

    LobEngineTrigger(LobEngine engine, Class<S> type, int blockSize,
                     List<LobProperty<?>> lobProperties)
    {
        mEngine = engine;
        mBlockSize = blockSize;

        mLobProperties = new LobProperty[lobProperties.size()];
        lobProperties.toArray(mLobProperties);
    }

    // Returns user specified Lob values
    @Override
    public Object beforeInsert(S storable) throws PersistException {
        // Capture user lob values for later and replace with new locators.
        int length = mLobProperties.length;
        Object[] userLobs = new Object[length];
        for (int i=0; i<length; i++) {
            LobProperty<Lob> prop = mLobProperties[i];
            Object userLob = storable.getPropertyValue(prop.mName);
            userLobs[i] = userLob;
            if (userLob != null) {
                Object lob = prop.createNewLob(mBlockSize);
                storable.setPropertyValue(prop.mName, lob);
            }
        }
        return userLobs;
    }

    @Override
    public void afterInsert(S storable, Object state) throws PersistException {
        // Save user lob value contents into new lobs. This is done after the
        // insert of the enclosing record to avoid an expensive rollback if a
        // constraint violation is detected.
        Object[] userLobs = (Object[]) state;
        int length = mLobProperties.length;
        for (int i=0; i<length; i++) {
            Object userLob = userLobs[i];
            if (userLob != null) {
                LobProperty<Lob> prop = mLobProperties[i];
                Lob lob = (Lob) storable.getPropertyValue(prop.mName);
                prop.setLobValue(mEngine.getLocator(lob), (Lob) userLob);
            }
        }
    }

    @Override
    public void failedInsert(S storable, Object state) {
        unreplaceLobs(storable, state);
    }

    @Override
    public Object beforeUpdate(S storable) throws PersistException {
        // For each dirty lob property, capture it in case update fails. All
        // lob updates are made in this method.

        int length = mLobProperties.length;
        Object[] userLobs = new Object[length];
        S existing = null;

        for (int i=0; i<length; i++) {
            LobProperty<Lob> prop = mLobProperties[i];
            if (!storable.isPropertyDirty(prop.mName)) {
                continue;
            }

            try {
                if (existing == null && (existing = loadExisting(storable)) == null) {
                    // Update will fail so don't touch lobs.
                    return null;
                }
            } catch (FetchException e) {
                throw e.toPersistException();
            }

            Object userLob = storable.getPropertyValue(prop.mName);
            userLobs[i] = userLob;
            Lob existingLob = (Lob) existing.getPropertyValue(prop.mName);
            if (userLob == null) {
                if (existingLob != null) {
                    // User is setting existing lob to null, so delete it.
                    mEngine.deleteLob(existingLob);
                }
            } else {
                if (existingLob == null) {
                    // User is setting a lob that has no locator yet, so make one.
                    existingLob = prop.createNewLob(mBlockSize);
                }
                prop.setLobValue(mEngine.getLocator(existingLob), (Lob) userLob);
                storable.setPropertyValue(prop.mName, existingLob);
            }
        }

        return userLobs;
    }

    @Override
    public void failedUpdate(S storable, Object state) {
        unreplaceLobs(storable, state);
    }

    // Returns existing Storable or null
    @Override
    public Object beforeDelete(S storable) throws PersistException {
        S existing = (S) storable.copy();
        try {
            if (!existing.tryLoad()) {
                existing = null;
            }
            return existing;
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    @Override
    public void afterDelete(S storable, Object existing) throws PersistException {
        if (existing != null) {
            // After successful delete of master storable, delete all the lobs.
            for (LobProperty<Lob> prop : mLobProperties) {
                Lob lob = (Lob) ((S) existing).getPropertyValue(prop.mName);
                mEngine.deleteLob(lob);
            }
        }
    }

    private S loadExisting(S storable) throws FetchException {
        S existing = (S) storable.copy();
        if (!existing.tryLoad()) {
            return null;
        }
        return existing;
    }

    private void unreplaceLobs(S storable, Object state) {
        if (state != null) {
            Object[] userLobs = (Object[]) state;
            int length = mLobProperties.length;
            for (int i=0; i<length; i++) {
                Object userLob = userLobs[i];
                if (userLob != null) {
                    storable.setPropertyValue(mLobProperties[i].mName, userLob);
                }
            }
        }
    }
}
