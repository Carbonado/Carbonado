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

package com.amazon.carbonado.sample;

import java.io.File;
import java.io.InputStream;

import org.joda.time.DateTime;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storage;

import com.amazon.carbonado.lob.FileBlob;

import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

import com.amazon.carbonado.stored.FileInfo;

/**
 * Test program which implements storage for a simple indexed file system.
 *
 * @author Brian S O'Neill
 */
public class FileInfoTest {
    public static Storage<FileInfo> openStorage(String envHome) throws Exception {
        BDBRepositoryBuilder builder = new BDBRepositoryBuilder();
        builder.setName("Simple file system");
        builder.setEnvironmentHome(envHome);
        builder.setTransactionNoSync(true);
        //builder.setProduct("DB");

        Repository repository = builder.build();
        return repository.storageFor(FileInfo.class);
    }

    /**
     * @param args [0] - BDB environment home, [1] - optional directory to read
     * files from for populating the simple file system
     */
    public static void main(String[] args) throws Exception {
        Storage<FileInfo> storage = openStorage(args[0]);
        if (args.length > 1) {
            populate(storage, new File(args[1]), null);
        }
    }

    private static void populate(Storage<FileInfo> storage, File file, FileInfo parent)
        throws Exception
    {
        FileInfo info = storage.prepare();
        if (parent != null) {
            info.setParentID(parent.getID());
        }
        info.setName(file.getName());
        info.setLastModified(new DateTime(file.lastModified()));

        if (file.isFile()) {
            info.setDirectory(false);
            info.setLength(file.length());
            info.setFileData(new FileBlob(file));
            info.insert();
        } else if (file.isDirectory()) {
            info.setDirectory(true);
            info.setLength(-1);
            info.insert();
            File[] children = file.listFiles();
            if (children != null) {
                for (int i=0; i<children.length; i++) {
                    populate(storage, children[i], info);
                }
            }
        }
    }
}
