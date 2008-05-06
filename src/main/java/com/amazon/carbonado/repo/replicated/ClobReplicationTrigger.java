/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.replicated;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.lob.AbstractClob;
import com.amazon.carbonado.lob.Clob;

/**
 * After loading a replica, replaces all Clobs with ReplicatedClobs.
 *
 * @author Brian S O'Neill
 */
class ClobReplicationTrigger<S extends Storable> extends Trigger<S> {
    /**
     * Returns null if no Clobs need to be replicated.
     */
    static <S extends Storable> ClobReplicationTrigger<S> create(Storage<S> masterStorage) {
        Map<String, ? extends StorableProperty<S>> properties = 
            StorableIntrospector.examine(masterStorage.getStorableType()).getDataProperties();

        List<String> clobNames = new ArrayList<String>(2);

        for (StorableProperty<S> property : properties.values()) {
            if (property.getType() == Clob.class) {
                clobNames.add(property.getName());
            }
        }

        if (clobNames.size() == 0) {
            return null;
        }

        return new ClobReplicationTrigger<S>(masterStorage,
                                             clobNames.toArray(new String[clobNames.size()]));
    }

    private final Storage<S> mMasterStorage;
    private final String[] mClobNames;

    private ClobReplicationTrigger(Storage<S> masterStorage, String[] clobNames) {
        mMasterStorage = masterStorage;
        mClobNames = clobNames;
    }

    @Override
    public void afterInsert(S replica, Object state) {
        afterLoad(replica);
    }

    @Override
    public void afterUpdate(S replica, Object state) {
        afterLoad(replica);
    }

    @Override
    public void afterLoad(S replica) {
        for (String name : mClobNames) {
            if (!replica.isPropertySupported(name)) {
                continue;
            }
            Clob replicaClob = (Clob) replica.getPropertyValue(name);
            if (replicaClob != null) {
                if (replicaClob instanceof ClobReplicationTrigger.Replicated) {
                    if (((Replicated) replicaClob).parent() == this) {
                        continue;
                    }
                }
                Replicated clob = new Replicated(name, replica, replicaClob);
                replica.setPropertyValue(name, clob);
            }
        }
        replica.markAllPropertiesClean();
    }

    /**
     * Writes go to master property first, and then to replica.
     */
    class Replicated extends AbstractClob {
        private static final int DEFAULT_BUFFER_SIZE = 4000;

        private final String mClobName;
        private final S mReplica;
        private final Clob mReplicaClob;

        private Clob mMasterClob;
        private boolean mMasterClobLoaded;

        Replicated(String clobName, S replica, Clob replicaClob) {
            mClobName = clobName;
            mReplica = replica;
            mReplicaClob = replicaClob;
        }

        public Reader openReader() throws FetchException {
            return mReplicaClob.openReader();
        }

        public Reader openReader(long pos) throws FetchException {
            return mReplicaClob.openReader(pos);
        }

        public Reader openReader(long pos, int bufferSize) throws FetchException {
            return mReplicaClob.openReader(pos, bufferSize);
        }

        public long getLength() throws FetchException {
            return mReplicaClob.getLength();
        }

        @Override
        public String asString() throws FetchException {
            return mReplicaClob.asString();
        }

        public Writer openWriter() throws PersistException {
            Clob masterClob = masterClob();
            if (masterClob == null) {
                return mReplicaClob.openWriter();
            } else {
                return openWriter(masterClob, 0, DEFAULT_BUFFER_SIZE);
            }
        }

        public Writer openWriter(long pos) throws PersistException {
            Clob masterClob = masterClob();
            if (masterClob == null) {
                return mReplicaClob.openWriter(pos);
            } else {
                return openWriter(masterClob, pos, DEFAULT_BUFFER_SIZE);
            }
        }

        public Writer openWriter(long pos, int bufferSize) throws PersistException {
            Clob masterClob = masterClob();
            if (masterClob == null) {
                return mReplicaClob.openWriter(pos, bufferSize);
            } else {
                return openWriter(masterClob, pos, bufferSize);
            }
        }

        private Writer openWriter(Clob masterClob, long pos, int bufferSize)
            throws PersistException
        {
            if (bufferSize < DEFAULT_BUFFER_SIZE) {
                bufferSize = DEFAULT_BUFFER_SIZE;
            }

            Writer masterOut = masterClob.openWriter(pos, 0);
            Writer replicaOut = mReplicaClob.openWriter(pos, 0);

            return new BufferedWriter(new Copier(masterOut, replicaOut), bufferSize);
        }

        public void setLength(long length) throws PersistException {
            Clob masterClob = masterClob();
            if (masterClob != null) {
                masterClob.setLength(length);
            }
            mReplicaClob.setLength(length);
        }

        public Object getLocator() {
            return mReplicaClob.getLocator();
        }

        @Override
        public int hashCode() {
            return mReplicaClob.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof ClobReplicationTrigger.Replicated) {
                Replicated other = (Replicated) obj;
                return parent() == other.parent() &&
                    mReplicaClob.equals(other.mReplicaClob);
            }
            return false;
        }

        @Override
        public String toString() {
            Object locator = getLocator();
            return locator == null ? super.toString() : ("ReplicatedClob@" + locator);
        }

        ClobReplicationTrigger parent() {
            return ClobReplicationTrigger.this;
        }

        /**
         * Returns null if not supported.
         */
        private Clob masterClob() throws PersistException {
            Clob masterClob = mMasterClob;

            if (mMasterClobLoaded) {
                return masterClob;
            }

            S master = mMasterStorage.prepare();
            mReplica.copyPrimaryKeyProperties(master);

            try {
                // FIXME: handle missing master with resync
                master.load();

                if (master.isPropertySupported(mClobName)) {
                    masterClob = (Clob) master.getPropertyValue(mClobName);
                    if (masterClob == null) {
                        // FIXME: perform resync, but still throw exception
                        throw new PersistNoneException("Master Clob is null: " + mClobName);
                    }
                }

                mMasterClob = masterClob;
                mMasterClobLoaded = true;

                return masterClob;
            } catch (FetchException e) {
                throw e.toPersistException();
            }
        }
    }

    private static class Copier extends Writer {
        private final Writer mReplicaOut;
        private final Writer mMasterOut;

        Copier(Writer master, Writer replica) {
            mMasterOut = master;
            mReplicaOut = replica;
        }

        @Override
        public void write(int c) throws IOException {
            mMasterOut.write(c);
            mReplicaOut.write(c);
        }

        @Override
        public void write(char[] c, int off, int len) throws IOException {
            mMasterOut.write(c, off, len);
            mReplicaOut.write(c, off, len);
        }

        @Override
        public void flush() throws IOException {
            mMasterOut.flush();
            mReplicaOut.flush();
        }

        @Override
        public void close() throws IOException {
            mMasterOut.close();
            mReplicaOut.close();
        }
    }
}
