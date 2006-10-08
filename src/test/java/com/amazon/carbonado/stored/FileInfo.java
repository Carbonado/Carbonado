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

package com.amazon.carbonado.stored;

import java.io.*;

import org.joda.time.DateTime;

import com.amazon.carbonado.*;
import com.amazon.carbonado.lob.*;
import com.amazon.carbonado.adapter.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@Indexes({
    @Index("name"),
    @Index({"length", "lastModified"}),
    @Index("lastModified"),
    @Index("parentID")
})
@Alias("CBN_TEST_FILE_INFO")
@AlternateKeys({
    @Key({"parentID", "name"})
})
@PrimaryKey("ID")
public abstract class FileInfo implements Storable {
    @Sequence("com.amazon.carbonado.storables.FileInfo")
    public abstract int getID();
    public abstract void setID(int value);

    @Nullable
    public abstract Integer getParentID();
    public abstract void setParentID(Integer id);

    @Nullable
    @Join(internal="parentID", external="ID")
    public abstract FileInfo getParent() throws FetchException;
    public abstract void setParent(FileInfo value);

    @Join(internal="ID", external="parentID")
    public abstract Query<FileInfo> getChildren() throws FetchException;

    @Alias("FILE_NAME")
    public abstract String getName();
    public abstract void setName(String value);

    @YesNoAdapter
    public abstract boolean isDirectory();
    public abstract void setDirectory(boolean value);

    @Alias("FILE_LENGTH")
    public abstract long getLength();
    public abstract void setLength(long value);

    @Nullable
    public abstract DateTime getLastModified();
    public abstract void setLastModified(DateTime value);

    @Version
    @Alias("RECORD_VERSION_NUMBER")
    public abstract int getVersionNumber();
    public abstract void setVersionNumber(int version);

    @Nullable
    public abstract Blob getFileData();
    public abstract void setFileData(Blob data);

    public String getFullPath() throws FetchException {
        FileInfo parent;
        try {
            parent = getParent();
        } catch (FetchNoneException e) {
            parent = null;
        }
        if (parent == null) {
            return getName();
        } else {
            return parent.getFullPath() + '/' + getName();
        }
    }
}
