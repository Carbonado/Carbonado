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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.List;

import org.cojen.util.KeyFactory;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.lob.AbstractBlob;
import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.BlobClob;
import com.amazon.carbonado.lob.Clob;
import com.amazon.carbonado.lob.Lob;

import com.amazon.carbonado.sequence.SequenceValueGenerator;
import com.amazon.carbonado.sequence.SequenceValueProducer;

/**
 * Complete Lob support for repositories, although repository is responsible
 * for binding Lob properties to this engine. Lobs are referenced by locators,
 * which are non-zero long integers. A zero locator is equivalent to null.
 *
 * @author Brian S O'Neill
 * @see #getSupportTrigger(Class, int)
 */
public class LobEngine {
    public static <S extends Storable> boolean hasLobs(Class<S> type) {
        StorableInfo<S> info = StorableIntrospector.examine(type);
        for (StorableProperty<? extends S> prop : info.getAllProperties().values()) {
            if (Lob.class.isAssignableFrom(prop.getType())) {
                return true;
            }
        }
        return false;
    }

    static IOException toIOException(RepositoryException e) {
        IOException ioe = new IOException(e.getMessage());
        ioe.initCause(e);
        return ioe;
    }

    final Repository mRepo;
    final Storage<StoredLob> mLobStorage;
    final Storage<StoredLob.Block> mLobBlockStorage;
    final SequenceValueProducer mLocatorSequence;

    private Map mTriggers;

    /**
     * @param lobRepo storage for Lobs - should not be replicated
     * @param locatorRepo storage for producing unique values for Lob locators
     * - should be root repository
     * @since 1.2
     */
    public LobEngine(Repository lobRepo, Repository locatorRepo) throws RepositoryException {
        // Cannot reliably use sequences provided by Lob repository, since
        // LobEngine is used internally by repositories.
        this(lobRepo, new SequenceValueGenerator(locatorRepo, StoredLob.class.getName()));
    }

    /**
     * @param lobRepo storage for Lobs - should not be replicated
     * @param locatorSequenceProducer source of unique values for Lob locators
     * @since 1.2
     */
    public LobEngine(Repository lobRepo, SequenceValueProducer locatorSequenceProducer)
        throws RepositoryException
    {
        mRepo = lobRepo;
        mLobStorage = lobRepo.storageFor(StoredLob.class);
        mLobBlockStorage = lobRepo.storageFor(StoredLob.Block.class);
        mLocatorSequence = locatorSequenceProducer;
    }

    /**
     * Returns a new Blob whose length is zero.
     *
     * @param blockSize block size (in <i>bytes</i>) to use
     * @return new empty Blob
     */
    public Blob createNewBlob(int blockSize) throws PersistException {
        StoredLob lob = mLobStorage.prepare();
        lob.setLocator(mLocatorSequence.nextLongValue());
        lob.setBlockSize(blockSize);
        lob.setLength(0);
        lob.insert();
        return new BlobImpl(lob.getLocator());
    }

    /**
     * Returns a new Clob whose length is zero.
     *
     * @param blockSize block size (in <i>bytes</i>) to use
     * @return new empty Clob
     */
    public Clob createNewClob(int blockSize) throws PersistException {
        StoredLob lob = mLobStorage.prepare();
        lob.setLocator(mLocatorSequence.nextLongValue());
        lob.setBlockSize(blockSize);
        lob.setLength(0);
        lob.insert();
        return new ClobImpl(lob.getLocator());
    }

    /**
     * Returns the locator for the given Lob, or zero if null.
     *
     * @throws ClassCastException if Lob is unrecognized
     */
    public long getLocator(Lob lob) {
        if (lob == null) {
            return 0;
        }
        Long locator = (Long) lob.getLocator();
        return locator == null ? 0 : locator;
    }

    /**
     * Deletes Lob data, freeing up all space consumed by it.
     */
    public void deleteLob(long locator) throws PersistException {
        if (locator == 0) {
            return;
        }

        Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
        try {
            StoredLob lob = mLobStorage.prepare();
            lob.setLocator(locator);
            if (lob.tryDelete()) {
                try {
                    mLobBlockStorage.query("locator = ?").with(lob.getLocator()).deleteAll();
                } catch (FetchException e) {
                    throw e.toPersistException();
                }
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    /**
     * Deletes Lob data, freeing up all space consumed by it.
     */
    public void deleteLob(Lob lob) throws PersistException {
        deleteLob(getLocator(lob));
    }

    /**
     * Loads a Blob value, without checking if it exists or not.
     *
     * @param locator lob locator as returned by getLocator
     * @return Blob value or null
     */
    public Blob getBlobValue(long locator) {
        if (locator == 0) {
            return null;
        }
        return new BlobImpl(locator);
    }

    /**
     * Loads a Clob value, without checking if it exists or not.
     *
     * @param locator lob locator as returned by getLocator
     * @return Clob value or null
     */
    public Clob getClobValue(long locator) {
        if (locator == 0) {
            return null;
        }
        return new ClobImpl(locator);
    }

    /**
     * Stores a value into a Blob, replacing anything that was there
     * before. Passing null deletes the Blob, which is a convenience for
     * auto-generated code that may call this method.
     *
     * @param locator lob locator as created by createNewBlob
     * @param data source of data for Blob, which may be null to delete
     * @throws IllegalArgumentException if locator is zero
     */
    public void setBlobValue(long locator, Blob data) throws PersistException, IOException {
        if (data == null) {
            deleteLob(locator);
            return;
        }

        if (locator == 0) {
            throw new IllegalArgumentException("Cannot use locator zero");
        }

        if (data instanceof BlobImpl) {
            BlobImpl impl = (BlobImpl) data;
            if (impl.getEnclosing() == this && impl.mLocator == locator) {
                // Blob is ours and locator is the same, so nothing to do.
                return;
            }
        }

        try {
            setBlobValue(locator, data.openInputStream(0, 0));
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    /**
     * Stores a value into a Blob, replacing anything that was there
     * before. Passing null deletes the Blob, which is a convenience for
     * auto-generated code that may call this method.
     *
     * @param locator lob locator as created by createNewBlob
     * @param data source of data for Blob, which may be null to delete
     * @throws IllegalArgumentException if locator is zero
     */
    public void setBlobValue(long locator, InputStream data) throws PersistException, IOException {
        if (data == null) {
            deleteLob(locator);
            return;
        }

        if (locator == 0) {
            throw new IllegalArgumentException("Cannot use locator zero");
        }

        Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
        txn.setForUpdate(true);
        try {
            StoredLob lob = mLobStorage.prepare();
            lob.setLocator(locator);
            try {
                lob.load();
            } catch (FetchNoneException e) {
                throw new PersistNoneException("Lob deleted: " + this);
            }

            Output out = new Output(lob, 0, txn);

            byte[] buffer = new byte[lob.getBlockSize()];

            long total = 0;
            int amt;
            try {
                while ((amt = data.read(buffer)) > 0) {
                    out.write(buffer, 0, amt);
                    total += amt;
                }
            } finally {
                data.close();
            }

            // Close but don't commit the transaction. This close is explicitly
            // not put into a finally block in order for an exception to cause
            // the transaction to rollback.
            out.close(false);

            if (total < lob.getLength()) {
                // Adjust length after closing stream to avoid OptimisticLockException.
                new BlobImpl(lob).setLength(total);
            }

            txn.commit();
        } catch (IOException e) {
            if (e.getCause() instanceof RepositoryException) {
                RepositoryException re = (RepositoryException) e.getCause();
                throw re.toPersistException();
            }
            throw e;
        } catch (FetchException e) {
            throw e.toPersistException();
        } finally {
            txn.exit();
        }
    }

    /**
     * Stores a value into a Clob, replacing anything that was there
     * before. Passing null deletes the Clob, which is a convenience for
     * auto-generated code that may call this method.
     *
     * @param locator lob locator as created by createNewClob
     * @param data source of data for Clob, which may be null to delete
     * @throws IllegalArgumentException if locator is zero
     */
    public void setClobValue(long locator, Clob data) throws PersistException, IOException {
        if (data == null) {
            deleteLob(locator);
            return;
        }

        if (locator == 0) {
            throw new IllegalArgumentException("Cannot use locator zero");
        }

        if (data instanceof ClobImpl) {
            BlobImpl impl = ((ClobImpl) data).getWrappedBlob();
            if (impl.getEnclosing() == this && impl.mLocator == locator) {
                // Blob is ours and locator is the same, so nothing to do.
                return;
            }
        }

        try {
            setClobValue(locator, data.openReader(0, 0));
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    /**
     * Stores a value into a Clob, replacing anything that was there
     * before. Passing null deletes the Clob, which is a convenience for
     * auto-generated code that may call this method.
     *
     * @param locator lob locator as created by createNewClob
     * @param data source of data for Clob, which may be null to delete
     * @throws IllegalArgumentException if locator is zero
     */
    public void setClobValue(long locator, Reader data) throws PersistException, IOException {
        if (data == null) {
            deleteLob(locator);
            return;
        }

        if (locator == 0) {
            throw new IllegalArgumentException("Cannot use locator zero");
        }

        Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
        txn.setForUpdate(true);
        try {
            StoredLob lob = mLobStorage.prepare();
            lob.setLocator(locator);
            try {
                lob.load();
            } catch (FetchNoneException e) {
                throw new PersistNoneException("Lob deleted: " + this);
            }

            ClobImpl clob = new ClobImpl(lob);
            Writer writer = clob.openWriter(0, 0);

            char[] buffer = new char[lob.getBlockSize() >> 1];

            long total = 0;
            int amt;
            try {
                while ((amt = data.read(buffer)) > 0) {
                    writer.write(buffer, 0, amt);
                    total += amt;
                }
            } finally {
                data.close();
            }
            writer.close();

            if (total < lob.getLength()) {
                clob.setLength(total);
            }

            txn.commit();
        } catch (IOException e) {
            if (e.getCause() instanceof RepositoryException) {
                RepositoryException re = (RepositoryException) e.getCause();
                throw re.toPersistException();
            }
            throw e;
        } catch (FetchException e) {
            throw e.toPersistException();
        } finally {
            txn.exit();
        }
    }

    /**
     * Returns a Trigger for binding to this LobEngine. Storage implementations
     * which use LobEngine must install this Trigger. Trigger instances are
     * cached, so subsequent calls for the same trigger return the same
     * instance.
     *
     * @param type type of Storable to create trigger for
     * @param blockSize block size to use
     * @return support trigger or null if storable type has no lob properties
     */
    public synchronized <S extends Storable> Trigger<S>
        getSupportTrigger(Class<S> type, int blockSize)
    {
        Object key = KeyFactory.createKey(new Object[] {type, blockSize});

        Trigger<S> trigger = (mTriggers == null) ? null : (Trigger<S>) mTriggers.get(key);

        if (trigger == null) {
            StorableInfo<S> info = StorableIntrospector.examine(type);

            List<LobProperty<?>> lobProperties = null;

            for (StorableProperty<? extends S> prop : info.getAllProperties().values()) {
                if (Blob.class.isAssignableFrom(prop.getType())) {
                    if (lobProperties == null) {
                        lobProperties = new ArrayList<LobProperty<?>>();
                    }
                    lobProperties.add(new BlobProperty(this, prop.getName()));
                } else if (Clob.class.isAssignableFrom(prop.getType())) {
                    if (lobProperties == null) {
                        lobProperties = new ArrayList<LobProperty<?>>();
                    }
                    lobProperties.add(new ClobProperty(this, prop.getName()));
                }
            }

            if (lobProperties != null) {
                trigger = new LobEngineTrigger<S>(this, type, blockSize, lobProperties);
            }

            if (mTriggers == null) {
                mTriggers = new SoftValuedHashMap();
            }

            mTriggers.put(key, trigger);
        }

        return trigger;
    }

    private class BlobImpl extends AbstractBlob implements Lob {
        final Long mLocator;
        final StoredLob mStoredLob;

        BlobImpl(long locator) {
            super(mRepo);
            mLocator = locator;
            mStoredLob = null;
        }

        BlobImpl(StoredLob lob) {
            super(mRepo);
            mLocator = lob.getLocator();
            mStoredLob = lob;
        }

        public InputStream openInputStream() throws FetchException {
            return openInputStream(0);
        }

        public InputStream openInputStream(long pos) throws FetchException {
            if (pos < 0) {
                throw new IllegalArgumentException("Position is negative: " + pos);
            }
            StoredLob lob = mStoredLob;
            Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
            if (lob == null) {
                lob = mLobStorage.prepare();
                lob.setLocator(mLocator);
                try {
                    lob.load();
                } catch (FetchException e) {
                    try {
                        txn.exit();
                    } catch (PersistException e2) {
                        // Don't care.
                    }
                    if (e instanceof FetchNoneException) {
                        throw new FetchNoneException("Lob deleted: " + this);
                    }
                    throw e;
                }
            }
            return new Input(lob, pos, txn);
        }

        public InputStream openInputStream(long pos, int bufferSize) throws FetchException {
            return openInputStream(pos);
        }

        public long getLength() throws FetchException {
            StoredLob lob = mStoredLob;
            if (lob == null) {
                lob = mLobStorage.prepare();
                lob.setLocator(mLocator);
                try {
                    lob.load();
                } catch (FetchNoneException e) {
                    throw new FetchNoneException("Lob deleted: " + this);
                }
            }
            return lob.getLength();
        }

        public OutputStream openOutputStream() throws PersistException {
            return openOutputStream(0);
        }

        public OutputStream openOutputStream(long pos) throws PersistException {
            if (pos < 0) {
                throw new IllegalArgumentException("Position is negative: " + pos);
            }
            StoredLob lob = mStoredLob;
            Transaction txn = mRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
            txn.setForUpdate(true);
            try {
                if (lob == null) {
                    lob = mLobStorage.prepare();
                    lob.setLocator(mLocator);
                    try {
                        lob.load();
                    } catch (FetchNoneException e) {
                        throw new PersistNoneException("Lob deleted: " + this);
                    } catch (FetchException e) {
                        throw e.toPersistException();
                    }
                }
                return new Output(lob, pos, txn);
            } catch (PersistException e) {
                try {
                    txn.exit();
                } catch (PersistException e2) {
                    // Don't care.
                }
                throw e;
            }
        }

        public OutputStream openOutputStream(long pos, int bufferSize) throws PersistException {
            return openOutputStream(pos);
        }

        public void setLength(long length) throws PersistException {
            if (length < 0) {
                throw new IllegalArgumentException("Length is negative: " + length);
            }

            Transaction txn = mRepo.enterTransaction();
            try {
                StoredLob lob = mStoredLob;
                if (lob == null) {
                    lob = mLobStorage.prepare();
                    lob.setLocator(mLocator);
                    txn.setForUpdate(true);
                    try {
                        lob.load();
                    } catch (FetchNoneException e) {
                        throw new PersistNoneException("Lob deleted: " + this);
                    }
                    txn.setForUpdate(false);
                }

                long oldLength = lob.getLength();

                if (length == oldLength) {
                    return;
                }

                long oldBlockCount = lob.getBlockCount();
                lob.setLength(length);

                if (length < oldLength) {
                    // Free unused blocks.
                    long newBlockCount = lob.getBlockCount();
                    if (newBlockCount < oldBlockCount) {
                        mLobBlockStorage.query("locator = ? & blockNumber >= ?")
                            .with(lob.getLocator())
                            // Subtract 0x80000000 such that block zero is
                            // physically stored with the smallest integer.
                            .with(((int) newBlockCount) - 0x80000000)
                            .deleteAll();
                    }

                    // Clear space in last block.
                    int lastBlockLength = lob.getLastBlockLength();
                    if (lastBlockLength != 0) {
                        StoredLob.Block block = mLobBlockStorage.prepare();
                        block.setLocator(mLocator);
                        // Subtract 0x80000000 such that block zero is
                        // physically stored with the smallest
                        // integer. Subtract one more to convert one-based
                        // count to zero-based index.
                        block.setBlockNumber(((int) newBlockCount) - 0x80000001);
                        txn.setForUpdate(true);
                        if (block.tryLoad()) {
                            byte[] data = block.getData();
                            if (data.length > lastBlockLength) {
                                byte[] newData = new byte[lastBlockLength];
                                System.arraycopy(data, 0, newData, 0, lastBlockLength);
                                block.setData(newData);
                                block.update();
                            }
                        }
                        txn.setForUpdate(false);
                    }
                }

                lob.update();
                txn.commit();
            } catch (FetchException e) {
                throw e.toPersistException();
            } finally {
                txn.exit();
            }
        }

        public Long getLocator() {
            return mLocator;
        }

        @Override
        public int hashCode() {
            return mLocator.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof BlobImpl) {
                BlobImpl other = (BlobImpl) obj;
                return LobEngine.this == other.getEnclosing() && mLocator.equals(other.mLocator);
            }
            return false;
        }

        @Override
        public String toString() {
            return "Blob@" + getLocator();
        }

        LobEngine getEnclosing() {
            return LobEngine.this;
        }
    }

    private class ClobImpl extends BlobClob implements Lob {
        ClobImpl(long locator) {
            super(new BlobImpl(locator));
        }

        ClobImpl(StoredLob lob) {
            super(new BlobImpl(lob));
        }

        @Override
        public Long getLocator() {
            return ((BlobImpl) super.getWrappedBlob()).getLocator();
        }

        @Override
        public int hashCode() {
            return super.getWrappedBlob().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof ClobImpl) {
                return getWrappedBlob().equals(((ClobImpl) obj).getWrappedBlob());
            }
            return false;
        }

        @Override
        public String toString() {
            return "Clob@" + getLocator();
        }

        // Override to gain permission.
        @Override
        protected BlobImpl getWrappedBlob() {
            return (BlobImpl) super.getWrappedBlob();
        }
    }

    private class Input extends InputStream {
        private final long mLocator;
        private final int mBlockSize;
        private final long mLength;

        private long mPos;
        private int mBlockNumber;
        private int mBlockPos;

        private Transaction mTxn;
        private Cursor<StoredLob.Block> mCursor;
        private StoredLob.Block mStoredBlock;

        Input(StoredLob lob, long pos, Transaction txn) throws FetchException {
            mLocator = lob.getLocator();
            mBlockSize = lob.getBlockSize();
            mLength = lob.getLength();

            mPos = pos;
            mBlockNumber = ((int) (pos / mBlockSize)) - 0x80000000;
            mBlockPos = (int) (pos % mBlockSize);

            mTxn = txn;

            mCursor = mLobBlockStorage.query("locator = ? & blockNumber >= ?")
                .with(mLocator).with(mBlockNumber)
                .fetch();
        }

        @Override
        public synchronized int read() throws IOException {
            if (mCursor == null) {
                throw new IOException("Closed");
            }
            if (mPos >= mLength) {
                return -1;
            }

            byte[] block = getBlockData();
            int blockPos = mBlockPos;

            int b;
            if (block == null || blockPos >= block.length) {
                b = 0;
            } else {
                b = block[blockPos] & 0xff;
            }

            mPos++;
            if (++blockPos >= mBlockSize) {
                mBlockNumber++;
                blockPos = 0;
            }
            mBlockPos = blockPos;

            return b;
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            return read(bytes, 0, bytes.length);
        }

        @Override
        public synchronized int read(byte[] bytes, int offset, int length) throws IOException {
            if (length <= 0) {
                return 0;
            }
            if (mCursor == null) {
                throw new IOException("Closed");
            }
            int avail = Math.min((int) (mLength - mPos), mBlockSize - mBlockPos);
            if (avail <= 0) {
                return -1;
            }
            if (length > avail) {
                length = avail;
            }

            byte[] block = getBlockData();
            int blockPos = mBlockPos;

            if (block == null) {
                Arrays.fill(bytes, offset, offset + length, (byte) 0);
            } else {
                int blockAvail = block.length - blockPos;
                if (blockAvail >= length) {
                    System.arraycopy(block, blockPos, bytes, offset, length);
                } else {
                    System.arraycopy(block, blockPos, bytes, offset, blockAvail);
                    Arrays.fill(bytes, offset + blockAvail, offset + length, (byte) 0);
                }
            }

            mPos += length;
            if ((blockPos += length) >= mBlockSize) {
                mBlockNumber++;
                blockPos = 0;
            }
            mBlockPos = blockPos;

            return length;
        }

        @Override
        public synchronized long skip(long n) throws IOException {
            if (n <= 0) {
                return 0;
            }
            if (mCursor == null) {
                throw new IOException("Closed");
            }
            long oldPos = mPos;
            if (n > Integer.MAX_VALUE) {
                n = Integer.MAX_VALUE;
            }
            long newPos = oldPos + n;
            if (newPos >= mLength) {
                newPos = mLength;
                n = newPos - oldPos;
                if (n <= 0) {
                    return 0;
                }
            }
            // Note: could open a new cursor here, but we'd potentially lose
            // the thread-local transaction. The next call to getBlockData will
            // detect that the desired block number differs from the actual one
            // and will skip one block at a time until cursor is at the correct
            // position.
            mPos = newPos;
            mBlockNumber = ((int) (newPos / mBlockSize)) - 0x80000000;
            mBlockPos = (int) (newPos % mBlockSize);
            return n;
        }

        @Override
        public synchronized void close() throws IOException {
            if (mTxn != null) {
                try {
                    // This should also cause the cursor to close.
                    mTxn.exit();
                } catch (PersistException e) {
                    throw toIOException(e);
                }
                mTxn = null;
            }
            if (mCursor != null) {
                try {
                    mCursor.close();
                } catch (FetchException e) {
                    throw toIOException(e);
                }
                mCursor = null;
                mStoredBlock = null;
            }
        }

        // Caller must be synchronized and have checked if stream is closed
        private byte[] getBlockData() throws IOException {
            while (mStoredBlock == null || mBlockNumber > mStoredBlock.getBlockNumber()) {
                try {
                    if (!mCursor.hasNext()) {
                        mStoredBlock = null;
                        return null;
                    }
                    mStoredBlock = mCursor.next();
                } catch (FetchException e) {
                    try {
                        close();
                    } catch (IOException e2) {
                        // Don't care.
                    }
                    throw toIOException(e);
                }
            }
            if (mBlockNumber < mStoredBlock.getBlockNumber()) {
                return null;
            }
            return mStoredBlock.getData();
        }
    }

    private class Output extends OutputStream {
        private final StoredLob mStoredLob;

        private long mPos;
        private int mBlockNumber;
        private int mBlockPos;

        private Transaction mTxn;
        private StoredLob.Block mStoredBlock;
        private byte[] mBlockData;
        private int mBlockLength;
        private boolean mDoInsert;

        Output(StoredLob lob, long pos, Transaction txn) throws PersistException {
            mStoredLob = lob;

            mPos = pos;
            mBlockNumber = ((int) (pos / lob.getBlockSize())) - 0x80000000;
            mBlockPos = (int) (pos % lob.getBlockSize());

            mTxn = txn;
        }

        @Override
        public synchronized void write(int b) throws IOException {
            if (mTxn == null) {
                throw new IOException("Closed");
            }

            prepareBlockData();

            int blockPos = mBlockPos;
            if (blockPos >= mBlockData.length) {
                byte[] newBlockData = new byte[mStoredLob.getBlockSize()];
                System.arraycopy(mBlockData, 0, newBlockData, 0, mBlockData.length);
                mBlockData = newBlockData;
            }
            mBlockData[blockPos++] = (byte) b;
            if (blockPos > mBlockLength) {
                mBlockLength = blockPos;
            }
            if (blockPos >= mStoredLob.getBlockSize()) {
                mBlockNumber++;
                blockPos = 0;
            }
            mBlockPos = blockPos;
            mPos++;
        }

        @Override
        public void write(byte[] bytes) throws IOException {
            write(bytes, 0, bytes.length);
        }

        @Override
        public synchronized void write(byte[] bytes, int offset, int length) throws IOException {
            if (length <= 0) {
                return;
            }
            if (mTxn == null) {
                throw new IOException("Closed");
            }

            while (length > 0) {
                prepareBlockData();

                int avail = mStoredLob.getBlockSize() - mBlockPos;
                if (avail > length) {
                    avail = length;
                }

                if ((mBlockPos + avail) >= mBlockData.length) {
                    byte[] newBlockData = new byte[mStoredLob.getBlockSize()];
                    System.arraycopy(mBlockData, 0, newBlockData, 0, mBlockData.length);
                    mBlockData = newBlockData;
                }

                System.arraycopy(bytes, offset, mBlockData, mBlockPos, avail);
                offset += avail;
                length -= avail;
                mBlockPos += avail;
                if (mBlockPos > mBlockLength) {
                    mBlockLength = mBlockPos;
                }
                if (mBlockPos >= mStoredLob.getBlockSize()) {
                    mBlockNumber++;
                    mBlockPos = 0;
                }
                mPos += avail;
            }
        }

        @Override
        public synchronized void flush() throws IOException {
            if (mTxn == null) {
                throw new IOException("Closed");
            }
            try {
                updateBlock();
            } catch (PersistException e) {
                try {
                    close();
                } catch (IOException e2) {
                    // Don't care.
                }
                throw toIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            close(true);
        }

        synchronized void close(boolean commit) throws IOException {
            if (mTxn != null) {
                try {
                    updateBlock();
                    if (mPos > mStoredLob.getLength()) {
                        mStoredLob.setLength(mPos);
                        mStoredLob.update();
                    }
                    if (commit) {
                        mTxn.commit();
                    }
                } catch (PersistException e) {
                    throw toIOException(e);
                } finally {
                    if (commit) {
                        try {
                            mTxn.exit();
                        } catch (PersistException e) {
                            throw toIOException(e);
                        }
                    }
                }
                mTxn = null;
            }
        }

        // Caller must be synchronized and have checked if stream is closed
        private void updateBlock() throws PersistException {
            if (mStoredBlock != null) {
                byte[] blockData = mBlockData;
                if (blockData.length != mBlockLength) {
                    byte[] truncated = new byte[mBlockLength];
                    System.arraycopy(blockData, 0, truncated, 0, truncated.length);
                    blockData = truncated;
                }
                mStoredBlock.setData(blockData);
                if (mDoInsert) {
                    mStoredBlock.insert();
                    mDoInsert = false;
                } else {
                    mStoredBlock.update();
                }
            }
        }

        // Caller must be synchronized and have checked if stream is closed
        private void prepareBlockData() throws IOException {
            if (mStoredBlock == null || mBlockNumber > mStoredBlock.getBlockNumber()) {
                try {
                    updateBlock();

                    mStoredBlock = mLobBlockStorage.prepare();
                    mStoredBlock.setLocator(mStoredLob.getLocator());
                    mStoredBlock.setBlockNumber(mBlockNumber);
                    try {
                        if (mStoredBlock.tryLoad()) {
                            mBlockData = mStoredBlock.getData();
                            mBlockLength = mBlockData.length;
                            mDoInsert = false;
                        } else {
                            mBlockData = new byte[mStoredLob.getBlockSize()];
                            mBlockLength = 0;
                            mDoInsert = true;
                        }
                    } catch (FetchException e) {
                        throw e.toPersistException();
                    }
                } catch (PersistException e) {
                    try {
                        close();
                    } catch (IOException e2) {
                        // Don't care.
                    }
                    throw toIOException(e);
                }
            }
        }
    }
}
