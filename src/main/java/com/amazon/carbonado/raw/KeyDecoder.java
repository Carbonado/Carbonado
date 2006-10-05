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

package com.amazon.carbonado.raw;

import com.amazon.carbonado.CorruptEncodingException;

import static com.amazon.carbonado.raw.KeyEncoder.*;

/**
 * A very low-level class that decodes key components encoded by methods of
 * {@link KeyEncoder}.
 *
 * @author Brian S O'Neill
 */
public class KeyDecoder extends DataDecoder {

    /**
     * Decodes a signed integer from exactly 4 bytes, as encoded for descending
     * order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed integer value
     */
    public static int decodeIntDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        return ~decodeInt(src, srcOffset);
    }

    /**
     * Decodes a signed Integer object from exactly 1 or 5 bytes, as encoded
     * for descending order. If null is returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Integer object or null
     */
    public static Integer decodeIntegerObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeIntDesc(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed long from exactly 8 bytes, as encoded for descending
     * order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed long value
     */
    public static long decodeLongDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        return ~decodeLong(src, srcOffset);
    }

    /**
     * Decodes a signed Long object from exactly 1 or 9 bytes, as encoded for
     * descending order. If null is returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Long object or null
     */
    public static Long decodeLongObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeLongDesc(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed byte from exactly 1 byte, as encoded for descending
     * order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed byte value
     */
    public static byte decodeByteDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return (byte)(src[srcOffset] ^ 0x7f);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Byte object from exactly 1 or 2 bytes, as encoded for
     * descending order. If null is returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Byte object or null
     */
    public static Byte decodeByteObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeByteDesc(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed short from exactly 2 bytes, as encoded for descending
     * order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed short value
     */
    public static short decodeShortDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return (short)(((src[srcOffset] << 8) | (src[srcOffset + 1] & 0xff)) ^ 0x7fff);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a signed Short object from exactly 1 or 3 bytes, as encoded for
     * descending order. If null is returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return signed Short object or null
     */
    public static Short decodeShortObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeShortDesc(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a char from exactly 2 bytes, as encoded for descending order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return char value
     */
    public static char decodeCharDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return (char)~((src[srcOffset] << 8) | (src[srcOffset + 1] & 0xff));
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a Character object from exactly 1 or 3 bytes, as encoded for
     * descending order. If null is returned, then 1 byte was read.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Character object or null
     */
    public static Character decodeCharacterObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            int b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return decodeCharDesc(src, srcOffset + 1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a boolean from exactly 1 byte, as encoded for descending order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return boolean value
     */
    public static boolean decodeBooleanDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            return src[srcOffset] == 127;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a Boolean object from exactly 1 byte, as encoded for descending
     * order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Boolean object or null
     */
    public static Boolean decodeBooleanObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        try {
            switch (src[srcOffset]) {
            case NULL_BYTE_LOW: case NULL_BYTE_HIGH:
                return null;
            case (byte)127:
                return Boolean.TRUE;
            default:
                return Boolean.FALSE;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes a float from exactly 4 bytes, as encoded for descending order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return float value
     */
    public static float decodeFloatDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        int bits = decodeFloatBits(src, srcOffset);
        if (bits >= 0) {
            bits ^= 0x7fffffff;
        }
        return Float.intBitsToFloat(bits);
    }

    /**
     * Decodes a Float object from exactly 4 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Float object or null
     */
    public static Float decodeFloatObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        int bits = decodeFloatBits(src, srcOffset);
        if (bits >= 0) {
            bits ^= 0x7fffffff;
        }
        return bits == 0x7fffffff ? null : Float.intBitsToFloat(bits);
    }

    /**
     * Decodes a double from exactly 8 bytes, as encoded for descending order.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return double value
     */
    public static double decodeDoubleDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        long bits = decodeDoubleBits(src, srcOffset);
        if (bits >= 0) {
            bits ^= 0x7fffffffffffffffL;
        }
        return Double.longBitsToDouble(bits);
    }

    /**
     * Decodes a Double object from exactly 8 bytes.
     *
     * @param src source of encoded bytes
     * @param srcOffset offset into source array
     * @return Double object or null
     */
    public static Double decodeDoubleObjDesc(byte[] src, int srcOffset)
        throws CorruptEncodingException
    {
        long bits = decodeDoubleBits(src, srcOffset);
        if (bits >= 0) {
            bits ^= 0x7fffffffffffffffL;
        }
        return bits == 0x7fffffffffffffffL ? null : Double.longBitsToDouble(bits);
    }

    /**
     * Decodes the given byte array as originally encoded for ascending order.
     * The decoding stops when any kind of terminator or illegal byte has been
     * read. The decoded bytes are stored in valueRef.
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
            return decode(src, srcOffset, valueRef, 0);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes the given byte array as originally encoded for descending order.
     * The decoding stops when any kind of terminator or illegal byte has been
     * read. The decoded bytes are stored in valueRef.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded byte array is stored in element 0, which may be null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     */
    public static int decodeDesc(byte[] src, int srcOffset, byte[][] valueRef)
        throws CorruptEncodingException
    {
        try {
            return decode(src, srcOffset, valueRef, -1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * @param xorMask 0 for normal decoding, -1 for descending decoding
     */
    private static int decode(byte[] src, int srcOffset, byte[][] valueRef, int xorMask) {
        // Scan ahead, looking for terminator.
        int srcEnd = srcOffset;
        while (true) {
            byte b = src[srcEnd++];
            if (-32 <= b && b < 32) {
                if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                    if ((srcEnd - 1) <= srcOffset) {
                        valueRef[0] = null;
                        return 1;
                    }
                }
                break;
            }
        }

        if (srcEnd - srcOffset == 1) {
            valueRef[0] = EMPTY_BYTE_ARRAY;
            return 1;
        }

        // Value is decoded from base-32768.

        int valueLength = ((srcEnd - srcOffset - 1) * 120) >> 7;
        byte[] value = new byte[valueLength];
        int valueOffset = 0;

        final int originalOffset = srcOffset;

        int accumBits = 0;
        int accum = 0;

        while (true) {
            int d = (src[srcOffset++] ^ xorMask) & 0xff;
            int b;
            if (srcOffset == srcEnd ||
                (b = (src[srcOffset++] ^ xorMask) & 0xff) < 32 || b > 223) {
                // Handle special case where one byte was emitted for digit.
                d -= 32;
                // To produce digit, multiply d by 192 and add 191 to adjust
                // for missing remainder. The lower bits are discarded anyhow.
                d = (d << 7) + (d << 6) + 191;

                // Shift decoded digit into accumulator.
                accumBits += 15;
                accum = (accum << 15) | d;

                break;
            }

            d -= 32;
            // To produce digit, multiply d by 192 and add in remainder.
            d = ((d << 7) + (d << 6)) + b - 32;

            // Shift decoded digit into accumulator.
            accumBits += 15;
            accum = (accum << 15) | d;

            if (accumBits == 15) {
                value[valueOffset++] = (byte)(accum >> 7);
            } else {
                value[valueOffset++] = (byte)(accum >> (accumBits - 8));
                accumBits -= 8;
                value[valueOffset++] = (byte)(accum >> (accumBits - 8));
            }
            accumBits -= 8;
        }

        if (accumBits >= 8 && valueOffset < valueLength) {
            value[valueOffset] = (byte)(accum >> (accumBits - 8));
        }

        valueRef[0] = value;

        return srcOffset - originalOffset;
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
            return decodeString(src, srcOffset, valueRef, 0);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes an encoded string from the given byte array as originally
     * encoded for descending order.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param valueRef decoded string is stored in element 0, which may be null
     * @return amount of bytes read from source
     * @throws CorruptEncodingException if source data is corrupt
     */
    public static int decodeStringDesc(byte[] src, int srcOffset, String[] valueRef)
        throws CorruptEncodingException
    {
        try {
            return decodeString(src, srcOffset, valueRef, -1);
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * @param xorMask 0 for normal decoding, -1 for descending decoding
     */
    private static int decodeString(byte[] src, int srcOffset, String[] valueRef, int xorMask)
        throws CorruptEncodingException
    {
        // Scan ahead, looking for terminator.
        int srcEnd = srcOffset;
        while (true) {
            byte b = src[srcEnd++];
            if (-2 <= b && b < 2) {
                if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                    if ((srcEnd - 1) <= srcOffset) {
                        valueRef[0] = null;
                        return 1;
                    }
                }
                break;
            }
        }

        if (srcEnd - srcOffset == 1) {
            valueRef[0] = "";
            return 1;
        }

        // Allocate a character array which may be longer than needed once
        // bytes are decoded into characters.
        char[] value = new char[srcEnd - srcOffset];
        int valueOffset = 0;

        final int originalOffset = srcOffset;

        while (srcOffset < srcEnd) {
            int c = (src[srcOffset++] ^ xorMask) & 0xff;
            switch (c >> 5) {
            case 0: case 1: case 2: case 3:
                // 0xxxxxxx
                value[valueOffset++] = (char)(c - 2);
                break;
            case 4: case 5:
                // 10xxxxxx xxxxxxxx

                c = c & 0x3f;
                // Multiply by 192, add in remainder, remove offset of 2, and de-normalize.
                value[valueOffset++] =
                    (char)((c << 7) + (c << 6) + ((src[srcOffset++] ^ xorMask) & 0xff) + 94);

                break;
            case 6:
                // 110xxxxx xxxxxxxx xxxxxxxx

                c = c & 0x1f;
                // Multiply by 192, add in remainder...
                c = (c << 7) + (c << 6) + ((src[srcOffset++] ^ xorMask) & 0xff) - 32;
                // ...multiply by 192, add in remainder, remove offset of 2, and de-normalize.
                c = (c << 7) + (c << 6) + ((src[srcOffset++] ^ xorMask) & 0xff) + 12382;

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

        valueRef[0] = new String(value, 0, valueOffset - 1);

        return srcEnd - originalOffset;
    }

    /**
     * Decodes the given byte array which was encoded by {@link
     * KeyEncoder#encodeSingleDesc}.
     */
    public static byte[] decodeSingleDesc(byte[] src) throws CorruptEncodingException {
        return decodeSingleDesc(src, 0, 0);
    }

    /**
     * Decodes the given byte array which was encoded by {@link
     * KeyEncoder#encodeSingleDesc}.
     *
     * @param prefixPadding amount of extra bytes to skip from start of encoded byte array
     * @param suffixPadding amount of extra bytes to skip at end of encoded byte array
     */
    public static byte[] decodeSingleDesc(byte[] src, int prefixPadding, int suffixPadding)
        throws CorruptEncodingException
    {
        try {
            int length = src.length - suffixPadding - prefixPadding;
            if (length == 0) {
                return EMPTY_BYTE_ARRAY;
            }
            byte[] dst = new byte[length];
            while (--length >= 0) {
                dst[length] = (byte) (~src[prefixPadding + length]);
            }
            return dst;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }

    /**
     * Decodes the given byte array which was encoded by {@link
     * KeyEncoder#encodeSingleNullableDesc}.
     */
    public static byte[] decodeSingleNullableDesc(byte[] src) throws CorruptEncodingException {
        return decodeSingleNullableDesc(src, 0, 0);
    }

    /**
     * Decodes the given byte array which was encoded by {@link
     * KeyEncoder#encodeSingleNullableDesc}.
     *
     * @param prefixPadding amount of extra bytes to skip from start of encoded byte array
     * @param suffixPadding amount of extra bytes to skip at end of encoded byte array
     */
    public static byte[] decodeSingleNullableDesc(byte[] src, int prefixPadding, int suffixPadding)
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
            byte[] dst = new byte[length];
            while (--length >= 0) {
                dst[length] = (byte) (~src[1 + prefixPadding + length]);
            }
            return dst;
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptEncodingException(null, e);
        }
    }
}
