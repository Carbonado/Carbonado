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

import java.io.*;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;
import com.amazon.carbonado.lob.*;

import com.amazon.carbonado.repo.toy.ToyRepository;
import com.amazon.carbonado.stored.*;

/**
 * Test case for {@link StorableSerializer}.
 *
 * @author Brian S O'Neill
 */
public class TestStorableSerializer extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestStorableSerializer.class);
    }

    private Repository mRepository;

    public TestStorableSerializer(String name) {
        super(name);
    }

    protected void setUp() {
        mRepository = new ToyRepository();
    }

    protected void tearDown() {
        mRepository.close();
        mRepository = null;
    }

    public void testReadAndWrite() throws Exception {
        Storage<StorableTestBasic> storage = mRepository.storageFor(StorableTestBasic.class);
        StorableTestBasic stb = storage.prepare();
        stb.setId(50);
        stb.setStringProp("hello");
        stb.setIntProp(100);
        stb.setLongProp(999);
        stb.setDoubleProp(2.718281828d);

        StorableSerializer<StorableTestBasic> serializer = 
            StorableSerializer.forType(StorableTestBasic.class);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        serializer.write(stb, (DataOutput) dout);
        dout.flush();

        byte[] bytes = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);

        StorableTestBasic stb2 = serializer.read(storage, (DataInput) din);

        assertEquals(stb, stb2);
    }

    /*
    public void testReadAndWriteLobs() throws Exception {
        Storage<StorableWithLobs> storage = mRepository.storageFor(StorableWithLobs.class);
        StorableWithLobs s = storage.prepare();
        s.setBlobValue(new ByteArrayBlob("Hello Blob".getBytes()));
        s.setClobValue(new StringClob("Hello Clob"));

        StorableSerializer<StorableWithLobs> serializer = 
            StorableSerializer.forType(StorableWithLobs.class);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        serializer.write(s, (DataOutput) dout);
        dout.flush();

        byte[] bytes = bout.toByteArray();

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);

        StorableWithLobs s2 = serializer.read(storage, (DataInput) din);

        assertEquals(s, s2);
    }
    */
}
