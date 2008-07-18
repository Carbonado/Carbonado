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

package com.amazon.carbonado.repo.sleepycat;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.raw.RawCursor;
import com.amazon.carbonado.raw.RawUtil;

import com.amazon.carbonado.txn.TransactionScope;

/**
 *
 *
 * @author Brian S O'Neill
 */
abstract class BDBCursor<Txn, S extends Storable> extends RawCursor<S> {
    private static final byte[] NO_DATA = new byte[0];

    private final TransactionScope<Txn> mScope;
    private final BDBStorage<Txn, S> mStorage;
    /**
     * @param scope
     * @param startBound specify the starting key for the cursor, or null if first
     * @param inclusiveStart true if start bound is inclusive
     * @param endBound specify the ending key for the cursor, or null if last
     * @param inclusiveEnd true if end bound is inclusive
     * @param maxPrefix maximum expected common initial bytes in start and end bound
     * @param reverse when true, iteration is reversed
     * @param storage
     * @throws IllegalArgumentException if any bound is null but is not inclusive
     * @throws ClassCastException if lock is not an object passed by
     * {@link BDBStorage#openCursor BDBStorage.openCursor}
     */
    protected BDBCursor(TransactionScope<Txn> scope,
                        byte[] startBound, boolean inclusiveStart,
                        byte[] endBound, boolean inclusiveEnd,
                        int maxPrefix,
                        boolean reverse,
                        BDBStorage<Txn, S> storage)
        throws FetchException
    {
        super(scope.getLock(),
              startBound, inclusiveStart,
              endBound, inclusiveEnd,
              maxPrefix, reverse);

        mScope = scope;
        mStorage = storage;
        scope.register(storage.getStorableType(), this);
    }

    void open() throws FetchException {
        try {
            cursor_open(mScope.getTxn(), mScope.getIsolationLevel());
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    public void close() throws FetchException {
        try {
            super.close();
        } finally {
            mScope.unregister(mStorage.getStorableType(), this);
        }
    }

    @Override
    protected void release() throws FetchException {
        try {
            cursor_close();
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    protected byte[] getCurrentKey() throws FetchException {
        if (searchKey_getPartial()) {
            throw new IllegalStateException();
        }
        return searchKey_getDataCopy();
    }

    @Override
    protected byte[] getCurrentValue() throws FetchException {
        if (data_getPartial()) {
            throw new IllegalStateException();
        }
        return data_getDataCopy();
    }

    @Override
    protected void disableKeyAndValue() {
        searchKey_setPartial(true);
        data_setPartial(true);
    }

    @Override
    protected void disableValue() {
        data_setPartial(true);
    }

    @Override
    protected void enableKeyAndValue() throws FetchException {
        searchKey_setPartial(false);
        data_setPartial(false);
        if (!hasCurrent()) {
            throw new FetchException("Current key and value missing");
        }
    }

    protected boolean hasCurrent() throws FetchException {
        try {
            return cursor_getCurrent();
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    protected S instantiateCurrent() throws FetchException {
        return mStorage.instantiate(primaryKey_getData(), data_getData());
    }

    @Override
    protected boolean toFirst() throws FetchException {
        try {
            return cursor_getFirst();
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    protected boolean toFirst(byte[] key) throws FetchException {
        try {
            searchKey_setData(key);
            return cursor_getSearchKeyRange();
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    protected boolean toLast() throws FetchException {
        try {
            return cursor_getLast();
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    protected boolean toLast(byte[] key) throws FetchException {
        try {
            // BDB cursor doesn't support "search for exact or less than", so
            // emulate it here. Add one to the key value, search, and then
            // back up.

            // This destroys the caller's key value. This method's
            // documentation indicates that the byte array may be altered.
            if (!RawUtil.increment(key)) {
                // This point is reached upon overflow, because key looked like:
                // 0xff, 0xff, 0xff, 0xff...
                // So moving to the absolute last is just fine.
                return cursor_getLast();
            }

            // Search for next record...
            searchKey_setData(key);
            if (cursor_getSearchKeyRange()) {
                // ...and back up.
                if (!cursor_getPrev()) {
                    return false;
                }
            } else {
                // Search found nothing, so go to the end.
                if (!cursor_getLast()) {
                    return false;
                }
            }

            // No guarantee that the currently matched key is correct, since
            // additional records may have been inserted after the search
            // operation finished.

            key = searchKey_getData();

            do {
                if (compareKeysPartially(searchKey_getData(), key) <= 0) {
                    return true;
                }
                // Keep backing up until the found key is equal or smaller than
                // what was requested.
            } while (cursor_getPrevNoDup());

            return false;
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    protected boolean toNext() throws FetchException {
        try {
            return cursor_getNext();
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    @Override
    protected boolean toPrevious() throws FetchException {
        try {
            return cursor_getPrev();
        } catch (Exception e) {
            throw mStorage.toFetchException(e);
        }
    }

    /**
     * If the given byte array is less than or equal to given size, it is
     * simply returned. Otherwise, a new array is allocated and the data is
     * copied.
     */
    protected static byte[] getData(byte[] data, int size) {
        if (data == null) {
            return NO_DATA;
        }
        if (data.length <= size) {
            return data;
        }
        byte[] newData = new byte[size];
        System.arraycopy(data, 0, newData, 0, size);
        return newData;
    }

    /**
     * Returns a copy of the data array.
     */
    protected static byte[] getDataCopy(byte[] data, int size) {
        if (data == null) {
            return NO_DATA;
        }
        byte[] newData = new byte[size];
        System.arraycopy(data, 0, newData, 0, size);
        return newData;
    }

    @Override
    protected void handleNoSuchElement() throws FetchException {
        // Might not be any more elements because storage is closed.
        mStorage.checkClosed();
    }

    protected abstract byte[] searchKey_getData();

    protected abstract byte[] searchKey_getDataCopy();

    protected abstract void searchKey_setData(byte[] data);

    protected abstract void searchKey_setPartial(boolean partial);

    protected abstract boolean searchKey_getPartial();

    protected abstract byte[] data_getData();

    protected abstract byte[] data_getDataCopy();

    protected abstract void data_setPartial(boolean partial);

    protected abstract boolean data_getPartial();

    protected abstract byte[] primaryKey_getData();

    protected abstract void cursor_open(Txn txn, IsolationLevel level) throws Exception;

    protected abstract void cursor_close() throws Exception;

    protected abstract boolean cursor_getCurrent() throws Exception;

    protected abstract boolean cursor_getFirst() throws Exception;

    protected abstract boolean cursor_getLast() throws Exception;

    protected abstract boolean cursor_getSearchKeyRange() throws Exception;

    protected abstract boolean cursor_getNext() throws Exception;

    protected abstract boolean cursor_getNextDup() throws Exception;

    protected abstract boolean cursor_getPrev() throws Exception;

    protected abstract boolean cursor_getPrevNoDup() throws Exception;
}
