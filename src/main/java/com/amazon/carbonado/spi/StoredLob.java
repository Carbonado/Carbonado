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

import com.amazon.carbonado.Alias;
import com.amazon.carbonado.Independent;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Version;

import com.amazon.carbonado.constraint.IntegerConstraint;

/**
 * Can be used internally by repositories for supporting Lobs.
 *
 * @author Brian S O'Neill
 * @see LobEngine
 */
@PrimaryKey("locator")
@Independent
@Alias("CARBONADO_LOB")
public abstract class StoredLob implements Storable<StoredLob> {
    public abstract long getLocator();
    public abstract void setLocator(long locator);

    public abstract int getBlockSize();
    @IntegerConstraint(min=1)
    public abstract void setBlockSize(int size);

    public abstract long getLength();
    @IntegerConstraint(min=0)
    public abstract void setLength(long length);

    @Version
    public abstract int getVersion();
    public abstract void setVersion(int version);

    /**
     * Returns number of blocks required to store Lob.
     */
    public long getBlockCount() {
        int blockSize = getBlockSize();
        return (getLength() + (blockSize - 1)) / blockSize;
    }

    /**
     * Returns expected length of last block. If zero, last block should be
     * full, unless the total length of Lob is zero.
     */
    public int getLastBlockLength() {
        return (int) (getLength() % getBlockSize());
    }

    /**
     * Blocks stored here.
     */
    @PrimaryKey({"locator", "+blockNumber"})
    public static abstract class Block implements Storable<Block> {
        public abstract long getLocator();
        public abstract void setLocator(long locator);

        /**
         * First block number is logically zero, but subtract 0x80000000 to get
         * actual number. This effectively makes the block number unsigned.
         */
        public abstract int getBlockNumber();
        public abstract void setBlockNumber(int number);

        public abstract byte[] getData();
        public abstract void setData(byte[] data);

        @Version
        public abstract int getVersion();
        public abstract void setVersion(int version);
    }
}
