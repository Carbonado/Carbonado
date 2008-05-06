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

package com.amazon.carbonado.sequence;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;

/**
 * General purpose implementation of a sequence value generator.
 *
 * @author Brian S O'Neill
 * @author bcastill
 * @see com.amazon.carbonado.Sequence
 * @see StoredSequence
 * @since 1.2
 */
public class SequenceValueGenerator extends AbstractSequenceValueProducer {
    public static final int DEFAULT_RESERVE_AMOUNT = 100;
    public static final int DEFAULT_INITIAL_VALUE = 1;
    public static final int DEFAULT_INCREMENT = 1;

    private final Repository mRepository;
    private final Storage<StoredSequence> mStorage;
    private final StoredSequence mStoredSequence;
    private final int mIncrement;
    private final int mReserveAmount;

    private boolean mHasReservedValues;
    private long mNextValue;

    /**
     * Construct a new SequenceValueGenerator which might create persistent
     * sequence data if it does not exist. The initial sequence value is one,
     * and the increment is one.
     *
     * @param repo repository to persist sequence data
     * @param name name of sequence
     */
    public SequenceValueGenerator(Repository repo, String name)
        throws RepositoryException
    {
        this(repo, name, DEFAULT_INITIAL_VALUE, DEFAULT_INCREMENT);
    }

    /**
     * Construct a new SequenceValueGenerator which might create persistent
     * sequence data if it does not exist.
     *
     * @param repo repository to persist sequence data
     * @param name name of sequence
     * @param initialValue initial sequence value, if sequence needs to be created
     * @param increment amount to increment sequence by
     */
    public SequenceValueGenerator(Repository repo, String name, long initialValue, int increment)
        throws RepositoryException
    {
        this(repo, name, initialValue, increment, DEFAULT_RESERVE_AMOUNT);
    }

    /**
     * Construct a new SequenceValueGenerator which might create persistent
     * sequence data if it does not exist.
     *
     * @param repo repository to persist sequence data
     * @param name name of sequence
     * @param initialValue initial sequence value, if sequence needs to be created
     * @param increment amount to increment sequence by
     * @param reserveAmount amount of sequence values to reserve
     */
    public SequenceValueGenerator(Repository repo, String name,
                                  long initialValue, int increment, int reserveAmount)
        throws RepositoryException
    {
        if (repo == null || name == null || increment < 1 || reserveAmount < 1) {
            throw new IllegalArgumentException();
        }

        mRepository = repo;

        mIncrement = increment;
        mReserveAmount = reserveAmount;

        mStorage = repo.storageFor(StoredSequence.class);

        mStoredSequence = mStorage.prepare();
        mStoredSequence.setName(name);

        Transaction txn = repo.enterTopTransaction(null);
        txn.setForUpdate(true);
        try {
            if (!mStoredSequence.tryLoad()) {
                // Create a new sequence.

                mStoredSequence.setInitialValue(initialValue);
                // Start as small as possible to allow signed long comparisons to work.
                mStoredSequence.setNextValue(Long.MIN_VALUE);

                // Try to transfer values from a deprecated sequence.
                com.amazon.carbonado.spi.StoredSequence oldSequence;
                try {
                    oldSequence = repo
                        .storageFor(com.amazon.carbonado.spi.StoredSequence.class).prepare();
                    oldSequence.setName(name);
                    if (oldSequence.tryLoad()) {
                        mStoredSequence.setInitialValue(oldSequence.getInitialValue());
                        mStoredSequence.setNextValue(oldSequence.getNextValue());
                    } else {
                        oldSequence = null;
                    }
                } catch (RepositoryException e) {
                    // Okay, perhaps no old sequence.
                    oldSequence = null;
                }

                if (mStoredSequence.tryInsert()) {
                    if (oldSequence != null) {
                        try {
                            // Get rid of deprecated sequence.
                            oldSequence.tryDelete();
                        } catch (RepositoryException e) {
                            // Oh well.
                        }
                    }
                } else {
                    // A race condition likely. Load again.
                    mStoredSequence.load();
                }
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    /**
     * Reset the sequence.
     *
     * @param initialValue first value produced by sequence
     */
    public void reset(int initialValue) throws FetchException, PersistException {
        synchronized (mStoredSequence) {
            Transaction txn = mRepository.enterTopTransaction(null);
            txn.setForUpdate(true);
            try {
                boolean doUpdate = mStoredSequence.tryLoad();
                mStoredSequence.setInitialValue(initialValue);
                // Start as small as possible to allow signed long comparisons to work.
                mStoredSequence.setNextValue(Long.MIN_VALUE);
                if (doUpdate) {
                    mStoredSequence.update();
                } else {
                    mStoredSequence.insert();
                }
                txn.commit();
                mHasReservedValues = false;
            } finally {
                txn.exit();
            }
        }
    }

    /**
     * Returns the next value from the sequence, which may wrap negative if all
     * positive values are exhausted. When sequence wraps back to initial
     * value, the sequence is fully exhausted, and an exception is thrown to
     * indicate this.
     *
     * <p>Note: this method throws PersistException even for fetch failures
     * since this method is called by insert operations. Insert operations can
     * only throw a PersistException.
     *
     * @throws PersistException for fetch/persist failure or if sequence is exhausted.
     */
    public long nextLongValue() throws PersistException {
        try {
            synchronized (mStoredSequence) {
                return nextUnadjustedValue() + Long.MIN_VALUE + mStoredSequence.getInitialValue();
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    /**
     * Returns the next value from the sequence, which may wrap negative if all
     * positive values are exhausted. When sequence wraps back to initial
     * value, the sequence is fully exhausted, and an exception is thrown to
     * indicate this.
     *
     * <p>Note: this method throws PersistException even for fetch failures
     * since this method is called by insert operations. Insert operations can
     * only throw a PersistException.
     *
     * @throws PersistException for fetch/persist failure or if sequence is
     * exhausted for int values.
     */
    @Override
    public int nextIntValue() throws PersistException {
        try {
            synchronized (mStoredSequence) {
                long initial = mStoredSequence.getInitialValue();
                if (initial >= 0x100000000L) {
                    throw new PersistException
                        ("Sequence initial value too large to support 32-bit ints: " +
                         mStoredSequence.getName() + ", initial: " + initial);
                }
                long next = nextUnadjustedValue();
                if (next >= Long.MIN_VALUE + 0x100000000L) {
                    // Everytime we throw this exception, a long sequence value
                    // has been lost. This seems fairly benign.
                    throw new PersistException
                        ("Sequence exhausted for 32-bit ints: " + mStoredSequence.getName() +
                         ", next: " + (next + Long.MIN_VALUE + initial));
                }
                return (int) (next + Long.MIN_VALUE + initial);
            }
        } catch (FetchException e) {
            throw e.toPersistException();
        }
    }

    /**
     * Allow any unused reserved values to be returned for re-use. If the
     * repository is shared by other processes, then reserved values might not
     * be returnable.
     *
     * <p>This method should be called during the shutdown process of a
     * repository, although calling it does not invalidate this
     * SequenceValueGenerator. If getNextValue is called again, it will reserve
     * values again.
     *
     * @return true if reserved values were returned
     */
    public boolean returnReservedValues() throws FetchException, PersistException {
        synchronized (mStoredSequence) {
            if (mHasReservedValues) {
                Transaction txn = mRepository.enterTopTransaction(null);
                txn.setForUpdate(true);
                try {
                    // Compare known StoredSequence with current persistent
                    // one. If same, then reserved values can be returned.
                    StoredSequence current = mStorage.prepare();
                    current.setName(mStoredSequence.getName());
                    if (current.tryLoad() && current.equals(mStoredSequence)) {
                        mStoredSequence.setNextValue(mNextValue + mIncrement);
                        mStoredSequence.update();
                        txn.commit();
                        mHasReservedValues = false;
                        return true;
                    }
                } finally {
                    txn.exit();
                }
            }
        }
        return false;
    }

    // Assumes caller has synchronized on mStoredSequence
    private long nextUnadjustedValue() throws FetchException, PersistException {
        if (mHasReservedValues) {
            long next = mNextValue + mIncrement;
            mNextValue = next;
            if (next < mStoredSequence.getNextValue()) {
                return next;
            }
            mHasReservedValues = false;
        }

        Transaction txn = mRepository.enterTopTransaction(null);
        txn.setForUpdate(true);
        try {
            // Assume that StoredSequence is stale, so reload.
            mStoredSequence.load();
            long next = mStoredSequence.getNextValue();
            long nextStored = next + mReserveAmount * mIncrement;

            if (next >= 0 && nextStored < 0) {
                // Wrapped around. There might be just a few values left.
                long avail = (Long.MAX_VALUE - next) / mIncrement;
                if (avail > 0) {
                    nextStored = next + avail * mIncrement;
                } else {
                    // Throw a PersistException since sequences are applied during
                    // insert operations, and inserts can only throw PersistExceptions.
                    throw new PersistException
                        ("Sequence exhausted: " + mStoredSequence.getName());
                }
            }

            mStoredSequence.setNextValue(nextStored);
            mStoredSequence.update();

            txn.commit();

            mNextValue = next;
            mHasReservedValues = true;
            return next;
        } finally {
            txn.exit();
        }
    }
}
