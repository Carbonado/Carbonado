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

package com.amazon.carbonado.raw;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.SupportException;

/**
 * Raw-level data compression using gzip.
 *
 * @author Olga Kuznetsova
 * @author Brian S O'Neill
 */
public class GzipCompressor {
    // NOTE: Class has to be public since it is accessed by generated code.

    final private static ThreadLocal<Deflater> cLocalDeflater = new ThreadLocal<Deflater>();
    final private static ThreadLocal<Inflater> cLocalInflater = new ThreadLocal<Inflater>();

    /**
     * Encodes into compressed form.
     *
     * @param value value to compress
     * @param prefix prefix of byte array to preserve
     * @return compressed value
     * @throws SupportException thrown if compression failed
     */
    public static byte[] compress(byte[] value, int prefix) throws SupportException {
        Deflater compressor = cLocalDeflater.get();
        if (compressor == null) {
            cLocalDeflater.set(compressor = new Deflater());
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream(value.length);

        try {
            bos.write(value, 0, prefix);
            DeflaterOutputStream dout = new DeflaterOutputStream(bos, compressor);
            dout.write(value, prefix, value.length - prefix);
            dout.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new SupportException(e);
        } finally {
            compressor.reset();
        }
    }

    /**
     * Decodes from compressed form.
     *
     * @param value value to decompress
     * @param prefix prefix of byte array to preserve
     * @return decompressed value
     * @throws CorruptEncodingException thrown if value cannot be decompressed
     */
    public static byte[] decompress(byte[] value, int prefix) throws CorruptEncodingException {
        Inflater inflater = cLocalInflater.get();
        if (inflater == null) {
            cLocalInflater.set(inflater = new Inflater());
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream(value.length * 2);

        try {
            bos.write(value, 0, prefix);
            InflaterOutputStream ios = new InflaterOutputStream(bos, inflater);
            ios.write(value, prefix, value.length - prefix);
            ios.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new CorruptEncodingException(e);
        } finally {
            inflater.reset();
        }
    }
}
