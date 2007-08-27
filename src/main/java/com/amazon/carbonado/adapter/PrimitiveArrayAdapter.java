/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.adapter;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.nio.ByteBuffer;

/**
 * Allows arrays of primitive types to be encoded (big-endian) as byte arrays.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@AdapterDefinition(storageTypePreferences=byte[].class)
public @interface PrimitiveArrayAdapter {
    /**
     * Adapter implementation for {@link PrimitiveArrayAdapter}.
     */
    public static class Adapter {
        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with adapter
         * @param ann specific annotation that binds to this adapter class
         */
        public Adapter(Class<?> type, String propertyName, PrimitiveArrayAdapter ann) {
        }

        public Adapter() {
        }

        public byte[] adaptToByteArray(short[] src) {
            if (src == null) {
                return null;
            }
            byte[] dst = new byte[src.length << 1];
            ByteBuffer.wrap(dst).asShortBuffer().put(src);
            return dst;
        }

        public byte[] adaptToByteArray(char[] src) {
            if (src == null) {
                return null;
            }
            byte[] dst = new byte[src.length << 1];
            ByteBuffer.wrap(dst).asCharBuffer().put(src);
            return dst;
        }

        public byte[] adaptToByteArray(int[] src) {
            if (src == null) {
                return null;
            }
            byte[] dst = new byte[src.length << 2];
            ByteBuffer.wrap(dst).asIntBuffer().put(src);
            return dst;
        }

        public byte[] adaptToByteArray(long[] src) {
            if (src == null) {
                return null;
            }
            byte[] dst = new byte[src.length << 3];
            ByteBuffer.wrap(dst).asLongBuffer().put(src);
            return dst;
        }

        public byte[] adaptToByteArray(float[] src) {
            if (src == null) {
                return null;
            }
            byte[] dst = new byte[src.length << 2];
            ByteBuffer.wrap(dst).asFloatBuffer().put(src);
            return dst;
        }

        public byte[] adaptToByteArray(double[] src) {
            if (src == null) {
                return null;
            }
            byte[] dst = new byte[src.length << 3];
            ByteBuffer.wrap(dst).asDoubleBuffer().put(src);
            return dst;
        }

        /**
         * Packs the given boolean array into a byte array, big-endian fashion.
         */
        public byte[] adaptToByteArray(boolean[] src) {
            if (src == null) {
                return null;
            }

            int srcLength = src.length;
            byte[] dst = new byte[(srcLength + 7) >> 3];
            int dstOffset = 0;

            int i = 0;
            while (i + 8 <= srcLength) {
                dst[dstOffset++] = (byte)
                    ((src[i++] ? 0x80 : 0) |
                     (src[i++] ? 0x40 : 0) |
                     (src[i++] ? 0x20 : 0) |
                     (src[i++] ? 0x10 : 0) |
                     (src[i++] ? 0x08 : 0) |
                     (src[i++] ? 0x04 : 0) |
                     (src[i++] ? 0x02 : 0) |
                     (src[i++] ? 0x01 : 0));
            }

            if (i < srcLength) {
                int accum = 0;
                while (i < srcLength) {
                    accum = (accum << 1) | (src[i++] ? 1 : 0);
                }
                accum <<= 8 - (srcLength & 7);
                dst[dstOffset] = (byte) accum;
            }

            return dst;
        }

        public short[] adaptToShortArray(byte[] src) {
            if (src == null) {
                return null;
            }
            short[] dst = new short[(src.length + 1) >> 1];
            ByteBuffer.wrap(src).asShortBuffer().get(dst);
            return dst;
        }

        public char[] adaptToCharArray(byte[] src) {
            if (src == null) {
                return null;
            }
            char[] dst = new char[(src.length + 1) >> 1];
            ByteBuffer.wrap(src).asCharBuffer().get(dst);
            return dst;
        }

        public int[] adaptToIntArray(byte[] src) {
            if (src == null) {
                return null;
            }
            int[] dst = new int[(src.length + 3) >> 2];
            ByteBuffer.wrap(src).asIntBuffer().get(dst);
            return dst;
        }

        public long[] adaptToLongArray(byte[] src) {
            if (src == null) {
                return null;
            }
            long[] dst = new long[(src.length + 7) >> 3];
            ByteBuffer.wrap(src).asLongBuffer().get(dst);
            return dst;
        }

        public float[] adaptToFloatArray(byte[] src) {
            if (src == null) {
                return null;
            }
            float[] dst = new float[(src.length + 3) >> 2];
            ByteBuffer.wrap(src).asFloatBuffer().get(dst);
            return dst;
        }

        public double[] adaptToDoubleArray(byte[] src) {
            if (src == null) {
                return null;
            }
            double[] dst = new double[(src.length + 7) >> 3];
            ByteBuffer.wrap(src).asDoubleBuffer().get(dst);
            return dst;
        }

        /**
         * Unpacks a boolean array from a byte array, big-endian fashion.
         */
        public boolean[] adaptToBooleanArray(byte[] src) {
            if (src == null) {
                return null;
            }

            int srcLength = src.length;
            boolean[] dst = new boolean[srcLength << 3];
            int dstOffset = 0;

            for (int i=0; i<srcLength; i++) {
                byte b = src[i];
                dst[dstOffset++] = (b & 0x80) != 0;
                dst[dstOffset++] = (b & 0x40) != 0;
                dst[dstOffset++] = (b & 0x20) != 0;
                dst[dstOffset++] = (b & 0x10) != 0;
                dst[dstOffset++] = (b & 0x08) != 0;
                dst[dstOffset++] = (b & 0x04) != 0;
                dst[dstOffset++] = (b & 0x02) != 0;
                dst[dstOffset++] = (b & 0x01) != 0;
            }

            return dst;
        }
    }
}
