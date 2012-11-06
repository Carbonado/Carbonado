/*
 * Copyright 2012 Amazon Technologies, Inc. or its affiliates.
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

import java.util.Arrays;

import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Transaction;

import com.amazon.carbonado.cursor.FetchAheadCursor;
import com.amazon.carbonado.cursor.FilteredCursor;

/**
 * Synchronizes layout metadata between two repositories. Both source and
 * destination might be updated.
 *
 * @author Brian S O'Neill
 */
public class LayoutSync {
    private final Repository mSource;
    private final Repository mDestination;

    public LayoutSync(Repository source, Repository destination) {
        mSource = source;
        mDestination = destination;
    }

    /**
     * @return true if any changes to source were made
     */
    public boolean run() throws RepositoryException {
        if (doRun()) {
            // Second pass.
            doRun();
            return true;
        }
        return false;
    }

    /**
     * @return true if a second pass should be run
     */
    private boolean doRun() throws RepositoryException {
        Storage<StoredLayout> srcLayoutStorage = mSource.storageFor(StoredLayout.class);
        Storage<StoredLayout> dstLayoutStorage = mDestination.storageFor(StoredLayout.class);

        Cursor<StoredLayout> srcLayouts = srcLayoutStorage.query().orderBy("layoutID").fetch();
        try {
            Cursor<StoredLayout> dstLayouts = dstLayoutStorage.query().orderBy("layoutID").fetch();
            try {
                return doRun(srcLayouts, dstLayouts);
            } finally {
                dstLayouts.close();
            }
        } finally {
            srcLayouts.close();
        }
    }

    /**
     * @return true if another pass should be run
     */
    private boolean doRun(Cursor<StoredLayout> srcLayouts, Cursor<StoredLayout> dstLayouts) 
        throws RepositoryException
    {
        // Fetch ahead to prevent BDB cursor deadlocks.
        srcLayouts = new FetchAheadCursor<StoredLayout>(srcLayouts, 1000);
        dstLayouts = new FetchAheadCursor<StoredLayout>(dstLayouts, 1000);

        boolean doAgain = false;

        StoredLayout src = null;
        StoredLayout dst = null;

        while (true) {
            if (src == null) {
                if (!srcLayouts.hasNext()) {
                    return doAgain;
                }
                src = srcLayouts.next();
            }

            if (dst == null && dstLayouts.hasNext()) {
                dst = dstLayouts.next();
            }

            if (dst == null || src.getLayoutID() < dst.getLayoutID()) {
                // Insert missing layout. If a generation conflict, do over in
                // second pass after all source generations have been inserted.
                doAgain |= !tryInsert(src);
                src = null;
                continue;
            }

            if (src.getLayoutID() > dst.getLayoutID()) {
                // Skip layout which only exists in destination.
                dst = null;
                continue;
            }

            if (src.getGeneration() != dst.getGeneration()) {
                // Same layouts, but with different generation. Create a new
                // non-conflicting generation to replace both.
                LogFactory.getLog(LayoutSync.class).error
                    ("Unable to synchronize layouts: " + src + " != " + dst);
                /*
                createNewGen(src, dst);
                doAgain = true;
                */
            }

            src = null;
            dst = null;
        }
    }

    /**
     * @return false if generation conflict
     */
    private boolean tryInsert(StoredLayout src) throws RepositoryException {
        Storage<StoredLayout> dstLayoutStorage = mDestination.storageFor(StoredLayout.class);

        Storage<StoredLayoutProperty> srcPropStorage =
            mSource.storageFor(StoredLayoutProperty.class);
        Storage<StoredLayoutProperty> dstPropStorage =
            mDestination.storageFor(StoredLayoutProperty.class);

        Transaction txn = mDestination.enterTransaction();
        try {
            txn.setForUpdate(true);
            StoredLayout dst = dstLayoutStorage.prepare();
            src.copyAllProperties(dst);
            if (!dst.tryInsert()) {
                return false;
            }

            Cursor<StoredLayoutProperty> srcProps =
                srcPropStorage.query("layoutID = ?").with(src.getLayoutID()).fetch();
            while (srcProps.hasNext()) {
                StoredLayoutProperty srcProp = srcProps.next();
                StoredLayoutProperty dstProp = dstPropStorage.prepare();
                srcProp.copyAllProperties(dstProp);
                if (!dstProp.tryInsert()) {
                    dstProp.tryDelete();
                    dstProp.insert();
                }
            }

            txn.commit();
        } finally {
            txn.exit();
        }

        return true;
    }

    /*
    private void createNewGen(StoredLayout src, StoredLayout dst) throws RepositoryException {
        long layoutID = src.getLayoutID();
        if (layoutID != dst.getLayoutID()) {
            throw new AssertionError();
        }

        String storableTypeName = src.getStorableTypeName();
        if (!storableTypeName.equals(dst.getStorableTypeName())) {
            // Assume that there's never any hashcode collision.
            throw new AssertionError();
        }

        Transaction dstTxn = mDestination.enterTransaction();
        try {
            dstTxn.setForUpdate(true);

            Transaction srcTxn = mSource.enterTransaction();
            try {
                srcTxn.setForUpdate(true);

                // New layout generation which is not used by src or dst.
                int newGen = nextGen(storableTypeName);

                int oldSrcGen = src.getGeneration();
                int oldDstGen = dst.getGeneration();

                long now = System.currentTimeMillis();

                // Remap source layout to new generation.
                doCreateNewGen(now, mSource, layoutID, oldSrcGen, newGen);

                // Remap destination layout to new generation.
                doCreateNewGen(now, mDestination, layoutID, oldDstGen, newGen);

                // Check if source has a layout generation matching the
                // conflicting destination layout. It cannot be used anymore.
                Cursor<StoredLayout> srcLayouts =
                    findLayouts(mSource, storableTypeName, oldDstGen);
                while (srcLayouts.hasNext()) {
                    src = srcLayouts.next();
                    newGen = nextGen(storableTypeName);
                    doCreateNewGen(now, mSource, src.getLayoutID(), oldDstGen, newGen);
                }

                // Check if destination has a layout generation matching the
                // conflicting source layout. It cannot be used anymore.
                Cursor<StoredLayout> dstLayouts =
                    findLayouts(mDestination, storableTypeName, oldSrcGen);
                while (dstLayouts.hasNext()) {
                    dst = dstLayouts.next();
                    newGen = nextGen(storableTypeName);
                    doCreateNewGen(now, mDestination, dst.getLayoutID(), oldSrcGen, newGen);
                }

                srcTxn.commit();
            } finally {
                srcTxn.exit();
            }

            dstTxn.commit();
        } finally {
            dstTxn.exit();
        }
    }
    */

    /*
    private void doCreateNewGen(long now, Repository repo, long layoutID, int oldGen, int newGen)
        throws RepositoryException
    {
        Storage<StoredLayout> layoutStorage =
            repo.storageFor(StoredLayout.class);
        Storage<StoredLayoutEquivalence> equivStorage =
            repo.storageFor(StoredLayoutEquivalence.class);

        StoredLayout newLayout = layoutStorage.prepare();
        newLayout.setLayoutID(layoutID);
        newLayout.load();
        Layout.fillInCreationInfo(newLayout);
        newLayout.setGeneration(newGen);
        // Consistent timestamp for all records.
        newLayout.setCreationTimestamp(now);
        newLayout.update();

        StoredLayoutEquivalence equiv = equivStorage.prepare();
        equiv.setStorableTypeName(newLayout.getStorableTypeName());
        equiv.setGeneration(oldGen);
        equiv.setMatchedGeneration(newGen);

        equiv.insert();
    }

    private int nextGen(String storableTypeName) throws RepositoryException {
        return Math.max(nextGen(mSource, storableTypeName),
                        nextGen(mDestination, storableTypeName));
    }

    private int nextGen(Repository repo, String storableTypeName)
        throws RepositoryException
    {
        return LayoutFactory.nextGeneration(repo, storableTypeName);
    }

    static Cursor<StoredLayout> findLayouts(Repository repo,
                                            String storableTypeName, int generation)
        throws RepositoryException
    {
        return Layout.findLayouts(repo, storableTypeName, generation);
    }
    */
}
