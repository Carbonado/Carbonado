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

package com.amazon.carbonado.cursor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.lang.reflect.UndeclaredThrowableException;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.spi.RAFInputStream;
import com.amazon.carbonado.spi.RAFOutputStream;

/**
 * Sort buffer implemented via a merge sort algorithm. If there are too many
 * storables to fit in the reserved memory buffer, they are sorted and
 * serialized to temporary files.
 *
 * <p>The following system properties can be set to change the default
 * performance characteristics of the merge sort. Each property name must be
 * prefixed with "com.amazon.carbonado.cursor.MergeSortBuffer."
 *
 * <pre>
 * Property            Default    Notes
 * ------------------- ---------- ----------------------------------------------
 * maxArrayCapacity    8192       Larger value greatly improves performance, but
 *                                more memory is used for each running sort.
 *
 * maxOpenFileCount    100        Larger value may reduce the amount of file
 *                                merges, but there is an increased risk of
 *                                running out of file descriptors.
 *
 * outputBufferSize    10000      Larger value may improve performance of file
 *                                writing, but not by much.
 *
 * tmpdir                         Merge sort files by default are placed in the
 *                                Java temp directory. Override to place them
 *                                somewhere else.
 * </pre>
 *
 * @author Brian S O'Neill
 * @see SortedCursor
 */
public class MergeSortBuffer<S extends Storable> extends AbstractCollection<S>
    implements SortBuffer<S>
{
    private static final int MIN_ARRAY_CAPACITY = 64;

    // Bigger means better performance, but more memory is used.
    private static final int MAX_ARRAY_CAPACITY;
    private static final int DEFAULT_MAX_ARRAY_CAPACITY = 8192;

    // Bigger means better performance, but more file handles may be used.
    private static final int MAX_OPEN_FILE_COUNT;
    private static final int DEFAULT_MAX_OPEN_FILE_COUNT = 100;

    // Bigger may improve write performance, but not by much.
    private static final int OUTPUT_BUFFER_SIZE;
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 10000;

    private static final String TEMP_DIR;

    static {
        String prefix = MergeSortBuffer.class.getName() + '.';

        MAX_ARRAY_CAPACITY = Integer.getInteger(prefix + "maxArrayCapacity",
                                                DEFAULT_MAX_ARRAY_CAPACITY);

        MAX_OPEN_FILE_COUNT = Integer.getInteger(prefix + "maxOpenFileCount",
                                                 DEFAULT_MAX_OPEN_FILE_COUNT);

        OUTPUT_BUFFER_SIZE = Integer.getInteger(prefix + "outputBufferSize",
                                                DEFAULT_OUTPUT_BUFFER_SIZE);

        // Null means use system temp dir.
        String tempDir = System.getProperty(prefix + "tmpdir", null);

        if (tempDir != null) {
            File f = new File(tempDir);
            if (!f.exists() || !f.isDirectory() || !f.canRead() || !f.canWrite()) {
                tempDir = null;
            }
        }

        TEMP_DIR = tempDir;
    }

    private final String mTempDir;
    private final int mMaxArrayCapacity;

    private Preparer<S> mPreparer;

    private S[] mElements;
    private int mSize;
    private int mTotalSize;

    private WorkFilePool mWorkFilePool;
    private List<RandomAccessFile> mFilesInUse;

    private Comparator<S> mComparator;

    private volatile boolean mStop;

    /**
     * @since 1.2
     */
    public MergeSortBuffer() {
        this(null, TEMP_DIR, MAX_ARRAY_CAPACITY);
    }

    /**
     * @param storage storage for elements; if null use first Storable to
     * prepare reloaded Storables
     */
    public MergeSortBuffer(Storage<S> storage) {
        this(storage, TEMP_DIR, MAX_ARRAY_CAPACITY);
    }

    /**
     * @param storage storage for elements; if null use first Storable to
     * prepare reloaded Storables
     * @param tempDir directory to store temp files for merging, or null for default
     */
    public MergeSortBuffer(Storage<S> storage, String tempDir) {
        this(storage, tempDir, MAX_ARRAY_CAPACITY);
    }

    /**
     * @param storage storage for elements; if null use first Storable to
     * prepare reloaded Storables
     * @param tempDir directory to store temp files for merging, or null for default
     * @param maxArrayCapacity maximum amount of storables to keep in an array
     * before serializing to a file
     * @throws IllegalArgumentException if storage is null
     */
    @SuppressWarnings("unchecked")
    public MergeSortBuffer(Storage<S> storage, String tempDir, int maxArrayCapacity) {
        mTempDir = tempDir;
        mMaxArrayCapacity = maxArrayCapacity;

        if (storage != null) {
            mPreparer = new FromStorage(storage);
        }

        int cap = Math.min(MIN_ARRAY_CAPACITY, maxArrayCapacity);
        mElements = (S[]) new Storable[cap];
    }

    public void prepare(Comparator<S> comparator) {
        if (comparator == null) {
            throw new IllegalArgumentException();
        }
        clear();
        mComparator = comparator;
    }

    @Override
    public boolean add(S storable) {
        if (mPreparer == null) {
            mPreparer = new FromStorable(storable);
        }

        Comparator<S> comparator = comparator();

        arrayPrep:
        if (mSize >= mElements.length) {
            if (mElements.length < mMaxArrayCapacity) {
                // Increase array capacity.
                int newCap = mElements.length * 2;
                if (newCap > mMaxArrayCapacity) {
                    newCap = mMaxArrayCapacity;
                }
                S[] newElements = (S[]) new Storable[newCap];
                System.arraycopy(mElements, 0, newElements, 0, mElements.length);
                mElements = newElements;
                break arrayPrep;
            }

            // Sort current in-memory results and serialize to a temp file.

            // Make sure everything is set up to use temp files.
            {
                if (mWorkFilePool == null) {
                    mWorkFilePool = WorkFilePool.getInstance(mTempDir);
                    mFilesInUse = new ArrayList<RandomAccessFile>();
                }
            }

            Arrays.sort(mElements, comparator);

            RandomAccessFile raf;
            try {
                raf = mWorkFilePool.acquireWorkFile(this);
                OutputStream out =
                    new BufferedOutputStream(new RAFOutputStream(raf), OUTPUT_BUFFER_SIZE);

                if (mFilesInUse.size() < (MAX_OPEN_FILE_COUNT - 1)) {
                    mFilesInUse.add(raf);
                    int count = 0;
                    for (S element : mElements) {
                        // Check every so often if interrupted.
                        interruptCheck(++count);
                        element.writeTo(out);
                    }
                } else {
                    // Merge files together.

                    // Determine the average length per file in use.
                    long totalLength = 0;
                    int fileCount = mFilesInUse.size();
                    for (int i=0; i<fileCount; i++) {
                        totalLength += mFilesInUse.get(i).length();
                    }

                    // Compute average with ceiling rounding mode.
                    long averageLength = (totalLength + fileCount) / fileCount;

                    // For any file whose length is above average, don't merge
                    // it. The goal is to evenly distribute file growth.

                    List<RandomAccessFile> filesToExclude = new ArrayList<RandomAccessFile>();
                    List<RandomAccessFile> filesToMerge = new ArrayList<RandomAccessFile>();

                    long mergedLength = 0;
                    for (int i=0; i<fileCount; i++) {
                        RandomAccessFile fileInUse = mFilesInUse.get(i);
                        long fileLength = fileInUse.length();
                        if (fileLength > averageLength) {
                            filesToExclude.add(fileInUse);
                        } else {
                            filesToMerge.add(fileInUse);
                            mergedLength += fileLength;
                        }
                    }

                    mFilesInUse.add(raf);

                    // Pre-allocate space, in an attempt to improve performance
                    // as well as error out earlier, should the disk be full.
                    raf.setLength(mergedLength);

                    int count = 0;
                    Iterator<S> it = iterator(filesToMerge);
                    while (it.hasNext()) {
                        // Check every so often if interrupted.
                        interruptCheck(++count);
                        S element = it.next();
                        element.writeTo(out);
                    }

                    mWorkFilePool.releaseWorkFiles(filesToMerge);
                    mFilesInUse = filesToExclude;
                    mFilesInUse.add(raf);
                }

                out.flush();

                // Truncate any data from last time file was used.
                raf.setLength(raf.getFilePointer());
                // Reset to start of file in preparation for reading later.
                raf.seek(0);
            } catch (SupportException e) {
                throw new UndeclaredThrowableException(e);
            } catch (IOException e) {
                throw new UndeclaredThrowableException(e);
            }

            mSize = 0;
        }

        mElements[mSize++] = storable;
        mTotalSize++;
        return true;
    }

    @Override
    public int size() {
        return mTotalSize;
    }

    @Override
    public Iterator<S> iterator() {
        return iterator(mFilesInUse);
    }

    private Iterator<S> iterator(List<RandomAccessFile> filesToMerge) {
        Comparator<S> comparator = comparator();

        if (mWorkFilePool == null) {
            return new ObjectArrayIterator<S>(mElements, 0, mSize);
        }

        // Merge with the files. Use a priority queue to decide which is the
        // next buffer to pull an element from.

        PriorityQueue<Iter<S>> pq = new PriorityQueue<Iter<S>>(1 + mFilesInUse.size());
        pq.add(new ArrayIter<S>(comparator, mElements, mSize));
        for (RandomAccessFile raf : filesToMerge) {
            try {
                raf.seek(0);
            } catch (IOException e) {
                throw new UndeclaredThrowableException(e);
            }

            InputStream in = new BufferedInputStream(new RAFInputStream(raf));

            pq.add(new InputIter<S>(comparator, mPreparer, in));
        }

        return new Merger<S>(pq);
    }

    @Override
    public void clear() {
        if (mPreparer instanceof FromStorable) {
            mPreparer = null;
        }

        if (mTotalSize > 0) {
            mSize = 0;
            mTotalSize = 0;
            if (mWorkFilePool != null && mFilesInUse != null) {
                mWorkFilePool.releaseWorkFiles(mFilesInUse);
                mFilesInUse.clear();
            }
        }
    }

    public void sort() {
        // Sort current in-memory results. Anything residing in files has
        // already been sorted.
        Arrays.sort(mElements, 0, mSize, comparator());
    }

    public void close() {
        clear();
        if (mWorkFilePool != null) {
            mWorkFilePool.unregisterWorkFileUser(this);
        }
    }

    void stop() {
        mStop = true;
    }

    private Comparator<S> comparator() {
        Comparator<S> comparator = mComparator;
        if (comparator == null) {
            throw new IllegalStateException("Buffer was not prepared with a Comparator");
        }
        return comparator;
    }

    private void interruptCheck(int count) {
        if ((count & ~0xff) == 0 && (mStop || Thread.interrupted())) {
            close();
            throw new UndeclaredThrowableException(new FetchInterruptedException());
        }
    }

    private static interface Preparer<S extends Storable> {
        S prepare();
    }

    private static class FromStorage<S extends Storable> implements Preparer<S> {
        private final Storage<S> mStorage;

        FromStorage(Storage<S> storage) {
            if (storage == null) {
                throw new IllegalArgumentException();
            }
            mStorage = storage;
        }

        public S prepare() {
            return mStorage.prepare();
        }
    }

    private static class FromStorable<S extends Storable> implements Preparer<S> {
        private final S mStorable;

        FromStorable(S storable) {
            if (storable == null) {
                throw new IllegalArgumentException();
            }
            mStorable = (S) storable.prepare();
        }

        public S prepare() {
            return (S) mStorable.prepare();
        }
    }

    /**
     * Simple interator interface that supports peeking at next element.
     */
    private abstract static class Iter<S extends Storable> implements Comparable<Iter<S>> {
        private final Comparator<S> mComparator;

        protected Iter(Comparator<S> comparator) {
            mComparator = comparator;
        }

        /**
         * Returns null if iterator is exhausted.
         */
        abstract S peek();

        /**
         * Returns null if iterator is exhausted.
         */
        abstract S next();

        public int compareTo(Iter<S> iter) {
            S thisPeek = peek();
            S thatPeek = iter.peek();
            if (thisPeek == null) {
                if (thatPeek == null) {
                    return 0;
                }
                // Null is low in order to rise to top of priority queue. This
                // Iter will then be tossed out of the priority queue.
                return -1;
            } else if (thatPeek == null) {
                return 1;
            }
            return mComparator.compare(thisPeek, thatPeek);
        }
    }

    /**
     * Iterator that reads from an array.
     */
    private static class ArrayIter<S extends Storable> extends Iter<S> {
        private final S[] mArray;
        private final int mSize;
        private int mPos;

        ArrayIter(Comparator<S> comparator, S[] array, int size) {
            super(comparator);
            mArray = array;
            mSize = size;
        }

        @Override
        S peek() {
            int pos = mPos;
            if (pos >= mSize) {
                return null;
            }
            return mArray[pos];
        }

        @Override
        S next() {
            int pos = mPos;
            if (pos >= mSize) {
                return null;
            }
            S next = mArray[pos];
            mPos = pos + 1;
            return next;
        }
    }

    /**
     * Iterator that reads from an input stream of serialized Storables.
     */
    private static class InputIter<S extends Storable> extends Iter<S> {
        private final Preparer<S> mPreparer;
        private InputStream mIn;

        private S mNext;

        InputIter(Comparator<S> comparator, Preparer<S> preparer, InputStream in) {
            super(comparator);
            mPreparer = preparer;
            mIn = in;
        }

        @Override
        S peek() {
            if (mNext != null) {
                return mNext;
            }
            if (mIn != null) {
                try {
                    S next = mPreparer.prepare();
                    next.readFrom(mIn);
                    mNext = next;
                } catch (EOFException e) {
                    mIn = null;
                } catch (SupportException e) {
                    throw new UndeclaredThrowableException(e);
                } catch (IOException e) {
                    throw new UndeclaredThrowableException(e);
                }
            }
            return mNext;
        }

        @Override
        S next() {
            S next = peek();
            mNext = null;
            return next;
        }
    }

    private static class Merger<S extends Storable> implements Iterator<S> {
        private final PriorityQueue<Iter<S>> mPQ;

        private S mNext;

        Merger(PriorityQueue<Iter<S>> pq) {
            mPQ = pq;
        }

        public boolean hasNext() {
            if (mNext == null) {
                while (true) {
                    Iter<S> iter = mPQ.poll();
                    if (iter == null) {
                        return false;
                    }
                    if ((mNext = iter.next()) != null) {
                        // Iter is not exhausted, so put it back in to be used
                        // again. Adding it back causes it to be inserted in
                        // the proper order, based on the next element it has
                        // to offer.
                        mPQ.add(iter);
                        return true;
                    }
                }
            }
            return true;
        }

        public S next() {
            if (hasNext()) {
                S next = mNext;
                mNext = null;
                return next;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class ObjectArrayIterator<E> implements Iterator<E> {
        private final E[] mElements;
        private final int mEnd;
        private int mIndex;

        public ObjectArrayIterator(E[] elements, int start, int end) {
            mElements = elements;
            mEnd = end;
            mIndex = start;
        }

        public boolean hasNext() {
            return mIndex < mEnd;
        }

        public E next() {
            if (mIndex >= mEnd) {
                throw new NoSuchElementException();
            }
            return mElements[mIndex++];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
