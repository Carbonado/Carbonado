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

package com.amazon.carbonado.spi.raw;

import com.amazon.carbonado.CorruptEncodingException;

import static com.amazon.carbonado.spi.raw.DataEncoder.*;

/**
 * A very low-level class that decodes key components encoded by methods of
 * {@link DataEncoder}.
 *
 * @author Brian S O'Neill
 */
public class DataDecoder {
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * Decodes a signed integer from exactly 4 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed integer value
     */
    public static int decodeInt(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int value = (src[srcOffset] << 24) | ((src[srcOffset + 1] & 0xff) << 16) |
                ((src[srcOffset + 2] & 0xff) << 8) | (src[srcOffset + 3] & 0xff);
            return value ^ 0x80000000;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Integer object from exactly 1 or 5 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Integer object or null
     */
    public static Integer decodeIntegerObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeInt(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed long from exactly 8 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed long value
     */
    public static long decodeLong(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return
                (((long)(((src[srcOffset    ]       ) << 24) |
                         ((src[srcOffset + 1] & 0xff) << 16) |
                         ((src[srcOffset + 2] & 0xff) << 8 ) |
                         ((src[srcOffset + 3] & 0xff)      )) ^ 0x80000000 ) << 32) |
                (((long)(((src[srcOffset + 4]       ) << 24) |
                         ((src[srcOffset + 5] & 0xff) << 16) |
                         ((src[srcOffset + 6] & 0xff) << 8 ) |
                         ((src[srcOffset + 7] & 0xff)      )) & 0xffffffffL)      );
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Long object from exactly 1 or 9 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Long object or null
     */
    public static Long decodeLongObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeLong(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed byte from exactly 1 byte.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed byte value
     */
    public static byte decodeByte(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return (byte)(src[srcOffset] ^ 0x80);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Byte object from exactly 1 or 2 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Byte object or null
     */
    public static Byte decodeByteObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeByte(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed short from exactly 2 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed short value
     */
    public static short decodeShort(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return (short)(((src[srcOffset] << 8) | (src[srcOffset + 1] & 0xff)) ^ 0x8000);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Short object from exactly 1 or 3 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Short object or null
     */
    public static Short decodeShortObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeShort(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a char from exactly 2 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return char value
     */
    public static char decodeChar(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return (char)((src[srcOffset] << 8) | (src[srcOffset + 1] & 0xff));
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a Character object from exactly 1 or 3 bytes. If null is
     * returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Character object or null
     */
    public static Character decodeCharacterObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeChar(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a boolean from exactly 1 byte.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return boolean value
     */
    public static boolean decodeBoolean(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return src[srcOffset] == (byte)128;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a Boolean object from exactly 1 byte.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Boolean object or null
     */
    public static Boolean decodeBooleanObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            switch (src[srcOffset]) {
            case NULL_BYTE_LOW: case NULL_BYTE_HIGH:
                return null;
            case (byte)128:
                return Boolean.TRUE;
            default:
                return Boolean.FALSE;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a float from exactly 4 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return float value
     */
    public static float decodeFloat(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        int bits = decodeFloatBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x80000000 : 0xffffffff;
        return Float.intBitsToFloat(bits);
    }

    /**
     * Decodes a Float object from exactly 4 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Float object or null
     */
    public static Float decodeFloatObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        int bits = decodeFloatBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x80000000 : 0xffffffff;
        return bits == 0x7fffffff ? null : Float.intBitsToFloat(bits);
    }

    protected static int decodeFloatBits(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return (src[srcOffset] << 24) | ((src[srcOffset + 1] & 0xff) << 16) |
                ((src[srcOffset + 2] & 0xff) << 8) | (src[srcOffset + 3] & 0xff);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a double from exactly 8 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return double value
     */
    public static double decodeDouble(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        long bits = decodeDoubleBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x8000000000000000L : 0xffffffffffffffffL;
        return Double.longBitsToDouble(bits);
    }

    /**
     * Decodes a Double object from exactly 8 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Double object or null
     */
    public static Double decodeDoubleObj(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        long bits = decodeDoubleBits(src, srcOffset);
        bits ^= (bits < 0) ? 0x8000000000000000L : 0xffffffffffffffffL;
        return bits == 0x7fffffffffffffffL ? null : Double.longBitsToDouble(bits);
    }

    protected static long decodeDoubleBits(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return
                (((long)(((src[srcOffset    ]       ) << 24) |
                         ((src[srcOffset + 1] & 0xff) << 16) |
                         ((src[srcOffset + 2] & 0xff) << 8 ) |
                         ((src[srcOffset + 3] & 0xff)      )) ) << 32) |
                (((long)(((src[srcOffset + 4]       ) << 24) |
                         ((src[srcOffset + 5] & 0xff) << 16) |
                         ((src[srcOffset + 6] & 0xff) << 8 ) |
                         ((src[srcOffset + 7] & 0xff)      )) & 0xffffffffL));
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes the given byte array.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded byte array is stored in element 0, which may be null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     */
    public static int decode(byte[] src, int srcOffset, byte[][] valueRef)
        throws CorruptEncodingException
    {
        try {
            final int originalOffset = srcOffset;

            int b = src[srcOffset++] & 0xff;
            if (b >= 0xf8) {
                valueRef[0] = null;
                return 1;
            }

            int valueLength;
            if (b <= 0x7f) {
                valueLength = b;
            } else if (b <= 0xbf) {
                valueLength = ((b & 0x3f) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xdf) {
                valueLength = ((b & 0x1f) << 16) | ((src[srcOffset++] & 0xff) << 8) |
                    (src[srcOffset++] & 0xff);
            } else if (b <= 0xef) {
                valueLength = ((b & 0x0f) << 24) | ((src[srcOffset++] & 0xff) << 16) |
                    ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            } else {
                valueLength = ((b & 0x07) << 24) | ((src[srcOffset++] & 0xff) << 16) |
                    ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            }

            if (valueLength == 0) {
                valueRef[0] = EMPTY_BYTE_ARRAY;
            } else {
                byte[] value = new byte[valueLength];
                System.arraycopy(src, srcOffset, value, 0, valueLength);
                valueRef[0]= value;
            }

            return srcOffset - originalOffset + valueLength;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes an encoded string from the given byte array.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded string is stored in element 0, which may be null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     */
    public static int decodeString(byte[] src, int srcOffset, String[] valueRef)
        throws CorruptEncodingException
    {
        try {
            final int originalOffset = srcOffset;

            int b = src[srcOffset++] & 0xff;
            if (b >= 0xf8) {
                valueRef[0] = null;
                return 1;
            }

            int valueLength;
            if (b <= 0x7f) {
                valueLength = b;
            } else if (b <= 0xbf) {
                valueLength = ((b & 0x3f) << 8) | (src[srcOffset++] & 0xff);
            } else if (b <= 0xdf) {
                valueLength = ((b & 0x1f) << 16) | ((src[srcOffset++] & 0xff) << 8) |
                    (src[srcOffset++] & 0xff);
            } else if (b <= 0xef) {
                valueLength = ((b & 0x0f) << 24) | ((src[srcOffset++] & 0xff) << 16) |
                    ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            } else {
                valueLength = ((src[srcOffset++] & 0xff) << 24) |
                    ((src[srcOffset++] & 0xff) << 16) |
                    ((src[srcOffset++] & 0xff) << 8) | (src[srcOffset++] & 0xff);
            }

            if (valueLength == 0) {
                valueRef[0] = "";
                return srcOffset - originalOffset;
            }

            char[] value = new char[valueLength];
            int valueOffset = 0;

            while (valueOffset < valueLength) {
                int c = src[srcOffset++] & 0xff;
                switch (c >> 5) {
                case 0: case 1: case 2: case 3:
                    // 0xxxxxxx
                    value[valueOffset++] = (char)c;
                    break;
                case 4: case 5:
                    // 10xxxxxx xxxxxxxx
                    value[valueOffset++] = (char)(((c & 0x3f) << 8) | (src[srcOffset++] & 0xff));
                    break;
                case 6:
                    // 110xxxxx xxxxxxxx xxxxxxxx
                    c = ((c & 0x1f) << 16) | ((src[srcOffset++] & 0xff) << 8)
                        | (src[srcOffset++] & 0xff);
                    if (c >= 0x10000) {
                        // Split into surrogate pair.
                        c -= 0x10000;
                        value[valueOffset++] = (char)(0xd800 | ((c >> 10) & 0x3ff));
                        value[valueOffset++] = (char)(0xdc00 | (c & 0x3ff));
                    } else {
                        value[valueOffset++] = (char)c;
                    }
                    break;
                default:
                    // 111xxxxx
                    // Illegal.
                    throw new CorruptEncodingException
                        ("Corrupt encoded string data (source offset = "
                         + (srcOffset - 1) + ')');
                }
            }

            valueRef[0] = new String(value);

            return srcOffset - originalOffset;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes the given byte array which was encoded by {@link
     * DataEncoder#encodeSingle}.
     *
     * @param prefixPadding amount of extra bytes to skip from start of encoded byte array
     * @param suffixPadding amount of extra bytes to skip at end of encoded byte array
     */
    public static byte[] decodeSingle(byte[] src, int prefixPadding, int suffixPadding)
        throws CorruptEncodingException
    {
        try {
            int length = src.length - suffixPadding - prefixPadding;
            if (length == 0) {
                return EMPTY_BYTE_ARRAY;
            }
            if (prefixPadding <= 0 && suffixPadding <= 0) {
                return src;
            }
            byte[] dst = new byte[length];
            System.arraycopy(src, prefixPadding, dst, 0, length);
            return dst;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes the given byte array which was encoded by {@link
     * DataEncoder#encodeSingleNullable}.
     */
    public static byte[] decodeSingleNullable(byte[] src) throws CorruptEncodingException {
        return decodeSingleNullable(src, 0, 0);
    }

    /**
     * Decodes the given byte array which was encoded by {@link
     * DataEncoder#encodeSingleNullable}.
     *
     * @param prefixPadding amount of extra bytes to skip from start of encoded byte array
     * @param suffixPadding amount of extra bytes to skip at end of encoded byte array
     */
    public static byte[] decodeSingleNullable(byte[] src, int prefixPadding, int suffixPadding)
        throws CorruptEncodingException
    {
        try {
            byte b = src[prefixPadding];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            int length = src.length - suffixPadding - 1 - prefixPadding;
            if (length == 0) {
                return EMPTY_BYTE_ARRAY;
            }
            byte[] value = new byte[length];
            System.arraycopy(src, 1 + prefixPadding, value, 0, length);
            return value;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }
}
