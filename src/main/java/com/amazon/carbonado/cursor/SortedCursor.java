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

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.cojen.util.BeanComparator;
import org.cojen.util.BeanIntrospector;
import org.cojen.util.BeanProperty;

import org.cojen.classfile.TypeDesc;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.util.Comparators;

/**
 * Wraps another Cursor and ensures the results are sorted. If the elements in
 * the source cursor are already partially sorted, a handled comparator can be
 * passed in which specifies the partial ordering. Elements are then processed
 * in smaller chunks rather than sorting the entire set. The handled comparator
 * can represent ascending or descending order of source elements.
 *
 * @author Brian S O'Neill
 */
public class SortedCursor<S> extends AbstractCursor<S> {
    /**
     * Convenience method to create a comparator which orders storables by the
     * given order-by properties. The property names may be prefixed with '+'
     * or '-' to indicate ascending or descending order. If the prefix is
     * omitted, ascending order is assumed.
     *
     * @param type type of storable to create comparator for
     * @param orderProperties list of properties to order by
     * @throws IllegalArgumentException if any property is null or not a member
     * of storable type
     */
    public static <S> Comparator<S> createComparator(Class<S> type, String... orderProperties) {
        BeanComparator bc = BeanComparator.forClass(type);

        if (Storable.class.isAssignableFrom(type)) {
            StorableInfo info = StorableIntrospector.examine((Class) type);
            for (String property : orderProperties) {
                bc = orderBy(bc, OrderedProperty.parse(info, property));
            }
        } else {
            for (String property : orderProperties) {
                Class propertyType;
                {
                    String name = property;
                    if (name.startsWith("+") || name.startsWith("-")) {
                        name = name.substring(1);
                    }
                    propertyType = propertyType(type, name);
                }
                bc = orderBy(bc, property, propertyType, Direction.ASCENDING);
            }
        }

        return bc;
    }

    /**
     * @return null if unknown
     */
    private static Class propertyType(Class enclosingType, String propertyName) {
        Map<String, BeanProperty> properties = BeanIntrospector.getAllProperties(enclosingType);
        int dotIndex = propertyName.indexOf('.');
        if (dotIndex < 0) {
            BeanProperty bp = properties.get(propertyName);
            return bp == null ? null : bp.getType();
        } else {
            String parentName = propertyName.substring(0, dotIndex);
            BeanProperty bp = properties.get(parentName);
            if (bp == null) {
                return null;
            }
            return propertyType(bp.getType(), propertyName.substring(dotIndex + 1));
        }
    }

    /**
     * Convenience method to create a comparator which orders storables by the
     * given properties.
     *
     * @param properties list of properties to order by
     * @throws IllegalArgumentException if no properties or if any property is null
     */
    public static <S extends Storable> Comparator<S>
        createComparator(OrderedProperty<S>... properties)
    {
        if (properties == null || properties.length == 0 || properties[0] == null) {
            throw new IllegalArgumentException();
        }

        Class<S> type = properties[0].getChainedProperty().getPrimeProperty().getEnclosingType();

        BeanComparator bc = BeanComparator.forClass(type);

        for (OrderedProperty<S> property : properties) {
            if (property == null) {
                throw new IllegalArgumentException();
            }
            bc = orderBy(bc, property);
        }

        return bc;
    }


    /**
     * Convenience method to create a comparator which orders storables by the
     * given properties.
     *
     * @param properties list of properties to order by
     * @throws IllegalArgumentException if no properties or if any property is null
     */
    public static <S extends Storable> Comparator<S>
        createComparator(List<OrderedProperty<S>> properties)
    {
        if (properties == null || properties.size() == 0 || properties.get(0) == null) {
            throw new IllegalArgumentException();
        }

        Class<S> type =
            properties.get(0).getChainedProperty().getPrimeProperty().getEnclosingType();

        BeanComparator bc = BeanComparator.forClass(type);

        for (OrderedProperty<S> property : properties) {
            if (property == null) {
                throw new IllegalArgumentException();
            }
            bc = orderBy(bc, property);
        }

        return bc;
    }

    private static BeanComparator orderBy(BeanComparator bc, OrderedProperty property) {
        ChainedProperty chained = property.getChainedProperty();
        return orderBy(bc, chainToBeanString(chained), chained.getType(), property.getDirection());
    }

    private static BeanComparator orderBy(BeanComparator bc,
                                          String property,
                                          Class type,
                                          Direction direction)
    {
        bc = bc.orderBy(property);

        if (type != null && type.isArray()) {
            TypeDesc td = TypeDesc.forClass(type);
            if (td.getRootComponentType() == TypeDesc.BYTE) {
                bc = bc.using(Comparators.arrayComparator(type, true));
            } else {
                Comparator c = Comparators.arrayComparator(type, false);
                if (c == null) {
                    throw new IllegalArgumentException("Cannot sort by property type of " +
                                                       type.getName() + " for " + property);
                }
                bc = bc.using(c);
            }
        } else {
            bc = bc.caseSensitive();
        }

        if (direction == Direction.DESCENDING) {
            bc = bc.reverse();
        }

        return bc;
    }

    /**
     * Creates a dotted name string using the bean property names.
     */
    private static String chainToBeanString(ChainedProperty property) {
        int count = property.getChainCount();
        if (count <= 0) {
            return property.getPrimeProperty().getBeanName();
        }

        StringBuilder b = new StringBuilder();
        b.append(property.getPrimeProperty().getBeanName());
        for (int i=0; i<count; i++) {
            b.append('.');
            b.append(property.getChainedProperty(i).getBeanName());
        }

        return b.toString();
    }

    /** Wrapped cursor */
    private final Cursor<S> mCursor;
    /** Buffer to store and sort results */
    private final SortBuffer<S> mChunkBuffer;
    /** Optional comparator which matches ordering already handled by wrapped cursor */
    private final Comparator<S> mChunkMatcher;
    /** Comparator to use for sorting chunks */
    private final Comparator<S> mChunkSorter;

    /** Iteration over current contents in mChunkBuffer */
    private Iterator<S> mChunkIterator;

    /**
     * First record in a chunk, according to chunk matcher. In order to tell if
     * the next chunk has been reached, a record has to be read from the
     * wrapped cursor. The record is "pushed back" here for use when the next
     * chunk is ready to be processed.
     */
    private S mNextChunkStart;

    /**
     * @param cursor cursor to wrap
     * @param buffer required buffer to hold results
     * @param handled optional comparator which represents how the results are
     * already sorted
     * @param finisher required comparator which finishes the sort
     */
    public SortedCursor(Cursor<S> cursor, SortBuffer<S> buffer,
                        Comparator<S> handled, Comparator<S> finisher) {
        if (cursor == null || finisher == null) {
            throw new IllegalArgumentException();
        }
        mCursor = cursor;
        mChunkBuffer =  buffer;
        mChunkMatcher = handled;
        mChunkSorter = finisher;
    }

    /**
     * @param cursor cursor to wrap
     * @param buffer required buffer to hold results
     * @param type type of storable to create cursor for
     * @param orderProperties list of properties to order by
     * @throws IllegalArgumentException if any property is null or not a member
     * of storable type
     */
    public SortedCursor(Cursor<S> cursor, SortBuffer<S> buffer,
                        Class<S> type, String... orderProperties) {
        this(cursor, buffer, null, createComparator(type, orderProperties));
    }

    /**
     * Returns a comparator representing the effective sort order of this cursor.
     */
    public Comparator<S> comparator() {
        if (mChunkMatcher == null) {
            return mChunkSorter;
        }
        return new Comparator<S>() {
            public int compare(S a, S b) {
                int result = mChunkMatcher.compare(a, b);
                if (result == 0) {
                    result = mChunkSorter.compare(a, b);
                }
                return result;
            }
        };
    }

    public void close() throws FetchException {
        mCursor.close();
        mChunkIterator = null;
        mChunkBuffer.close();
    }

    public boolean hasNext() throws FetchException {
        try {
            prepareNextChunk();
            try {
                if (mChunkIterator.hasNext()) {
                    return true;
                }
            } catch (UndeclaredThrowableException e) {
                throw toFetchException(e);
            }
        } catch (NoSuchElementException e) {
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
        close();
        return false;
    }

    public S next() throws FetchException {
        try {
            prepareNextChunk();
            try {
                return mChunkIterator.next();
            } catch (UndeclaredThrowableException e) {
                throw toFetchException(e);
            } catch (NoSuchElementException e) {
                try {
                    close();
                } catch (FetchException e2) {
                    // Don't care.
                }
                throw e;
            }
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
    }

    @Override
    public int skipNext(int amount) throws FetchException {
        if (amount <= 0) {
            if (amount < 0) {
                throw new IllegalArgumentException("Cannot skip negative amount: " + amount);
            }
            return 0;
        }

        try {
            int count = 0;
            while (--amount >= 0 && hasNext()) {
                next();
                count++;
            }

            return count;
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
    }

    private void prepareNextChunk() throws FetchException {
        if (mChunkIterator != null && (mChunkMatcher == null || mChunkIterator.hasNext())) {
            // Ready to go.
            return;
        }

        Cursor<S> cursor = mCursor;
        SortBuffer<S> buffer = mChunkBuffer;
        Comparator<S> matcher = mChunkMatcher;

        try {
            mChunkIterator = null;
            buffer.prepare(mChunkSorter);

            fill: {
                if (matcher == null) {
                    // Buffer up entire results and sort.
                    int count = 0;
                    while (cursor.hasNext()) {
                        // Check every so often if interrupted.
                        if ((++count & ~0xff) == 0 && Thread.interrupted()) {
                            throw new FetchInterruptedException();
                        }
                        buffer.add(cursor.next());
                    }
                    break fill;
                }

                // Read a chunk into the buffer. First record is compared against
                // subsequent records, to determine when the chunk end is reached.

                S chunkStart;
                if (mNextChunkStart != null) {
                    chunkStart = mNextChunkStart;
                    mNextChunkStart = null;
                } else if (cursor.hasNext()) {
                    chunkStart = cursor.next();
                } else {
                    break fill;
                }

                buffer.add(chunkStart);
                int count = 1;

                while (cursor.hasNext()) {
                    // Check every so often if interrupted.
                    if ((++count & ~0xff) == 0 && Thread.interrupted()) {
                        throw new FetchInterruptedException();
                    }
                    S next = cursor.next();
                    if (matcher.compare(chunkStart, next) != 0) {
                        // Save for reading next chunk later.
                        mNextChunkStart = next;
                        break;
                    }
                    buffer.add(next);
                }
            }

            if (buffer.size() > 1) {
                buffer.sort();
            }

            mChunkIterator = buffer.iterator();
        } catch (UndeclaredThrowableException e) {
            throw toFetchException(e);
        }
    }

    private FetchException toFetchException(UndeclaredThrowableException e) {
        Throwable cause = e.getCause();
        if (cause == null) {
            cause = e;
        }
        if (cause instanceof FetchException) {
            return (FetchException) cause;
        }
        return new FetchException(null, cause);
    }
}
