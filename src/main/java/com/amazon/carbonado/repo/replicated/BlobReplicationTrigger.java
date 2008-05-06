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

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.charset.Charset;

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

import com.amazon.carbonado.lob.AbstractBlob;
import com.amazon.carbonado.lob.Blob;

/**
 * After loading a replica, replaces all Blobs with ReplicatedBlobs.
 *
 * @author Brian S O'Neill
 */
class BlobReplicationTrigger<S extends Storable> extends Trigger<S> {
    /**
     * Returns null if no Blobs need to be replicated.
     */
    static <S extends Storable> BlobReplicationTrigger<S> create(Storage<S> masterStorage) {
        Map<String, ? extends StorableProperty<S>> properties = 
            StorableIntrospector.examine(masterStorage.getStorableType()).getDataProperties();

        List<String> blobNames = new ArrayList<String>(2);

        for (StorableProperty<S> property : properties.values()) {
            if (property.getType() == Blob.class) {
                blobNames.add(property.getName());
            }
        }

        if (blobNames.size() == 0) {
            return null;
        }

        return new BlobReplicationTrigger<S>(masterStorage,
                                             blobNames.toArray(new String[blobNames.size()]));
    }

    private final Storage<S> mMasterStorage;
    private final String[] mBlobNames;

    private BlobReplicationTrigger(Storage<S> masterStorage, String[] blobNames) {
        mMasterStorage = masterStorage;
        mBlobNames = blobNames;
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
        for (String name : mBlobNames) {
            if (!replica.isPropertySupported(name)) {
                continue;
            }
            Blob replicaBlob = (Blob) replica.getPropertyValue(name);
            if (replicaBlob != null) {
                if (replicaBlob instanceof BlobReplicationTrigger.Replicated) {
                    if (((Replicated) replicaBlob).parent() == this) {
                        continue;
                    }
                }
                Replicated blob = new Replicated(name, replica, replicaBlob);
                replica.setPropertyValue(name, blob);
            }
        }
        replica.markAllPropertiesClean();
    }

    /**
     * Writes go to master property first, and then to replica.
     */
    class Replicated extends AbstractBlob {
        private static final int DEFAULT_BUFFER_SIZE = 4000;

        private final String mBlobName;
        private final S mReplica;
        private final Blob mReplicaBlob;

        private Blob mMasterBlob;
        private boolean mMasterBlobLoaded;

        Replicated(String blobName, S replica, Blob replicaBlob) {
            mBlobName = blobName;
            mReplica = replica;
            mReplicaBlob = replicaBlob;
        }

        public InputStream openInputStream() throws FetchException {
            return mReplicaBlob.openInputStream();
        }

        public InputStream openInputStream(long pos) throws FetchException {
            return mReplicaBlob.openInputStream(pos);
        }

        public InputStream openInputStream(long pos, int bufferSize) throws FetchException {
            return mReplicaBlob.openInputStream(pos, bufferSize);
        }

        public long getLength() throws FetchException {
            return mReplicaBlob.getLength();
        }

        @Override
        public String asString() throws FetchException {
            return mReplicaBlob.asString();
        }

        @Override
        public String asString(String charsetName) throws FetchException {
            return mReplicaBlob.asString(charsetName);
        }

        @Override
        public String asString(Charset charset) throws FetchException {
            return mReplicaBlob.asString(charset);
        }

        public OutputStream openOutputStream() throws PersistException {
            Blob masterBlob = masterBlob();
            if (masterBlob == null) {
                return mReplicaBlob.openOutputStream();
            } else {
                return openOutputStream(masterBlob, 0, DEFAULT_BUFFER_SIZE);
            }
        }

        public OutputStream openOutputStream(long pos) throws PersistException {
            Blob masterBlob = masterBlob();
            if (masterBlob == null) {
                return mReplicaBlob.openOutputStream(pos);
            } else {
                return openOutputStream(masterBlob, pos, DEFAULT_BUFFER_SIZE);
            }
        }

        public OutputStream openOutputStream(long pos, int bufferSize) throws PersistException {
            Blob masterBlob = masterBlob();
            if (masterBlob == null) {
                return mReplicaBlob.openOutputStream(pos, bufferSize);
            } else {
                return openOutputStream(masterBlob, pos, bufferSize);
            }
        }

        private OutputStream openOutputStream(Blob masterBlob, long pos, int bufferSize)
            throws PersistException
        {
            if (bufferSize < DEFAULT_BUFFER_SIZE) {
                bufferSize = DEFAULT_BUFFER_SIZE;
            }

            OutputStream masterOut = masterBlob.openOutputStream(pos, 0);
            OutputStream replicaOut = mReplicaBlob.openOutputStream(pos, 0);

            return new BufferedOutputStream(new Copier(masterOut, replicaOut), bufferSize);
        }

        public void setLength(long length) throws PersistException {
            Blob masterBlob = masterBlob();
            if (masterBlob != null) {
                masterBlob.setLength(length);
            }
            mReplicaBlob.setLength(length);
        }

        public Object getLocator() {
            return mReplicaBlob.getLocator();
        }

        @Override
        public int hashCode() {
            return mReplicaBlob.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof BlobReplicationTrigger.Replicated) {
                Replicated other = (Replicated) obj;
                return parent() == other.parent() &&
                    mReplicaBlob.equals(other.mReplicaBlob);
            }
            return false;
        }

        @Override
        public String toString() {
            Object locator = getLocator();
            return locator == null ? super.toString() : ("ReplicatedBlob@" + locator);
        }

        BlobReplicationTrigger parent() {
            return BlobReplicationTrigger.this;
        }

        /**
         * Returns null if not supported.
         */
        private Blob masterBlob() throws PersistException {
            Blob masterBlob = mMasterBlob;

            if (mMasterBlobLoaded) {
                return masterBlob;
            }

            S master = mMasterStorage.prepare();
            mReplica.copyPrimaryKeyProperties(master);

            try {
                // FIXME: handle missing master with resync
                master.load();

                if (master.isPropertySupported(mBlobName)) {
                    masterBlob = (Blob) master.getPropertyValue(mBlobName);
                    if (masterBlob == null) {
                        // FIXME: perform resync, but still throw exception
                        throw new PersistNoneException("Master Blob is null: " + mBlobName);
                    }
                }

                mMasterBlob = masterBlob;
                mMasterBlobLoaded = true;

                return masterBlob;
            } catch (FetchException e) {
                throw e.toPersistException();
            }
        }
    }

    private static class Copier extends OutputStream {
        private final OutputStream mReplicaOut;
        private final OutputStream mMasterOut;

        Copier(OutputStream master, OutputStream replica) {
            mMasterOut = master;
            mReplicaOut = replica;
        }

        @Override
        public void write(int b) throws IOException {
            mMasterOut.write(b);
            mReplicaOut.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            mMasterOut.write(b, off, len);
            mReplicaOut.write(b, off, len);
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
