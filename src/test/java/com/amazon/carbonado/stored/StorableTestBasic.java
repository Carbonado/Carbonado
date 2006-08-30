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

import java.util.Random;

import org.joda.time.DateTime;

import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;

/**
 * StorableTestBasic
 *
 * @author Don Schneider
 */
@PrimaryKey("id")
public abstract class StorableTestBasic implements Storable {
    public abstract int getId();
    public abstract void setId(int id);

    // Basic coverage of the primitives
    public abstract String getStringProp();
    public abstract void setStringProp(String aStringThing);

    public abstract int getIntProp();
    public abstract void setIntProp(int anInt);

    public abstract long getLongProp();
    public abstract void setLongProp(long aLong);

    public abstract double getDoubleProp();
    public abstract void setDoubleProp(double aDouble);

    @Nullable
    public abstract DateTime getDate();
    public abstract void setDate(DateTime aDate);

    public void initPrimaryKeyProperties() {
        setId(10);
    }

    public void initBasicProperties() {
        setStringProp("foo");
        setIntProp(10);
        setLongProp(120);
        setDoubleProp(1.2);
    }

    public void initPropertiesRandomly(int id) {
        setId(id);

        Random random = new Random(1000);

        setIntProp(random.nextInt());
        setLongProp(random.nextLong());
        setDoubleProp(random.nextDouble());
        setStringProp("imaString_" + id % 10);
    }

    public void initPropertiesPredictably(int id) {
        setId(id);

        setIntProp(id*10);
        setLongProp(id*10);
        setDoubleProp(id/2.0);
        setStringProp("string-" + id % 100);
    }

    public static void insertBunches(Repository repository, int count)
            throws RepositoryException
    {
        insertBunches(repository, count, 0, true);
    }


    public static void insertBunches(Repository repository,
                                     int count, int startId,
                                     boolean doRandom)
        throws RepositoryException
    {
        Storage<StorableTestBasic> storage = repository.storageFor(StorableTestBasic.class);
        StorableTestBasic s;

        for (int i = 0; i < count; i ++) {
            s = storage.prepare();
            if (doRandom) {
                s.initPropertiesRandomly(i);
            } else {
                s.initPropertiesPredictably(i+startId);
            }
            s.insert();
        }
    }
}
