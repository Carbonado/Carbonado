/*
 * Copyright 2009 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.layout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

/**
 * Extra options encoded with a Storable layout.
 *
 * @author Brian S O'Neill
 */
public class LayoutOptions {
    /**
     * Data is compression algorithm name, encoded by DataOutput.writeUTF.
     */
    static final byte COMPRESSION_TYPE = 1;

    private final Map<Byte, Object> mData;

    private boolean mReadOnly;

    public LayoutOptions() {
        mData = new HashMap<Byte, Object>(1);
    }

    /**
     * @return null if not compressed
     */
    public synchronized String getCompressionType() {
        return (String) mData.get(COMPRESSION_TYPE);
    }

    /**
     * @param type null if not compressed
     */
    public void setCompressionType(String type) {
        put(COMPRESSION_TYPE, type);
    }

    private synchronized void put(byte op, Object value) {
        if (mReadOnly) {
            throw new IllegalStateException("Options are read only");
        }
        if (value == null) {
            mData.remove(op);
        } else {
            mData.put(op, value);
        }
    }

    synchronized void readOnly() {
        mReadOnly = true;
    }

    /**
     * @return null if empty
     */
    synchronized byte[] encode() {
        if (mData.isEmpty()) {
            return null;
        }

        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(bout);

            for (Map.Entry<Byte, Object> entry : mData.entrySet()) {
                switch (entry.getKey()) {
                default:
                    break;
                case COMPRESSION_TYPE:
                    dout.write(COMPRESSION_TYPE);
                    dout.writeUTF((String) entry.getValue());
                }
            }

            dout.close();
            return bout.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * @param source can be null if empty
     */
    synchronized void decode(byte[] source) throws IOException {
        mData.clear();

        if (source == null || source.length == 0) {
            return;
        }

        ByteArrayInputStream bin = new ByteArrayInputStream(source);
        DataInputStream din = new DataInputStream(bin);

        while (bin.available() > 0) {
            byte op = din.readByte();
            switch (op) {
            default:
                throw new IOException("Unknown extra data type: " + op);
            case COMPRESSION_TYPE:
                mData.put(COMPRESSION_TYPE, din.readUTF());
                break;
            }
        }
    }
}
