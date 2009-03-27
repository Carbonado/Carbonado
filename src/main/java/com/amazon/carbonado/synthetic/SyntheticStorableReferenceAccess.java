/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.synthetic;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

import java.util.Comparator;
import java.util.Iterator;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.cursor.SortedCursor;

import com.amazon.carbonado.util.ThrowUnchecked;

/**
 * Provides access to the generated storable reference class and utility
 * methods.
 *
 * @author Brian S O'Neill
 * @see SyntheticStorableReferenceBuilder
 * @since 1.2.1
 */
public class SyntheticStorableReferenceAccess<S extends Storable> {
    private final Class<S> mMasterClass;
    private final Class<? extends Storable> mReferenceClass;

    private final Comparator<? extends Storable> mComparator;

    private final Method mCopyFromMasterMethod;
    private final Method mIsConsistentMethod;
    private final Method mCopyToMasterPkMethod;

    SyntheticStorableReferenceAccess(Class<S> masterClass,
                                     Class<? extends Storable> referenceClass,
                                     SyntheticStorableReferenceBuilder builder)
    {
        mMasterClass = masterClass;
        mReferenceClass = referenceClass;

        // We need a comparator which follows the same order as the generated
        // storable.
        SyntheticKey pk = builder.mPrimaryKey;
        String[] orderBy = new String[pk.getPropertyCount()];
        int i=0;
        Iterator<String> it = pk.getProperties();
        while (it.hasNext()) {
            orderBy[i++] = it.next();
        }
        mComparator = SortedCursor.createComparator(referenceClass, orderBy);

        try {
            mCopyFromMasterMethod =
                referenceClass.getMethod(builder.mCopyFromMasterMethodName, masterClass);

            mIsConsistentMethod =
                referenceClass.getMethod(builder.mIsConsistentMethodName, masterClass);

            mCopyToMasterPkMethod =
                referenceClass.getMethod(builder.mCopyToMasterPkMethodName, masterClass);
        } catch (NoSuchMethodException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    /**
     * Returns the storable class which is referenced.
     */
    public Class<S> getMasterClass() {
        return mMasterClass;
    }

    /**
     * Returns the generated storable reference class.
     */
    public Class<? extends Storable> getReferenceClass() {
        return mReferenceClass;
    }

    /**
     * Returns a comparator for ordering storable reference instances. This
     * order matches the primary key of the master storable.
     */
    public Comparator<? extends Storable> getComparator() {
        return mComparator;
    }

    /**
     * Sets all the primary key properties of the given master, using the
     * applicable properties of the given reference.
     *
     * @param reference source of property values
     * @param master master whose primary key properties will be set
     */
    public void copyToMasterPrimaryKey(Storable reference, S master) throws FetchException {
        try {
            mCopyToMasterPkMethod.invoke(reference, master);
        } catch (Exception e) {
            ThrowUnchecked.fireFirstDeclaredCause(e, FetchException.class);
        }
    }

    /**
     * Sets all the properties of the given reference, using the applicable
     * properties of the given master.
     *
     * @param reference reference whose properties will be set
     * @param master source of property values
     */
    public void copyFromMaster(Storable reference, S master) throws FetchException {
        try {
            mCopyFromMasterMethod.invoke(reference, master);
        } catch (Exception e) {
            ThrowUnchecked.fireFirstDeclaredCause(e, FetchException.class);
        }
    }

    /**
     * Returns true if the properties of the given reference match those
     * contained in the master, excluding any version property. This will
     * always return true after a call to copyFromMaster.
     *
     * @param reference reference whose properties will be tested
     * @param master source of property values
     */
    public boolean isConsistent(Storable reference, S master) throws FetchException {
        try {
            return (Boolean) mIsConsistentMethod.invoke(reference, master);
        } catch (Exception e) {
            ThrowUnchecked.fireFirstDeclaredCause(e, FetchException.class);
            // Not reached.
            return false;
        }
    }
}
