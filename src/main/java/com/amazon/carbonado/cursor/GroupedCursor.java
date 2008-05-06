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

import java.util.Comparator;
import java.util.NoSuchElementException;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;

/**
 * Abstract cursor for aggregation and finding distinct data. The source cursor
 * must be ordered in some fashion by the grouping properties. The arrangement of
 * properties must match, but it does not matter if they are ascending or
 * descending.
 *
 * @author Brian S O'Neill
 * @see SortedCursor
 * @param <S> source type, can be anything
 * @param <G> aggregate type, can be anything
 */
public abstract class GroupedCursor<S, G> extends AbstractCursor<G> {
    private final Cursor<S> mCursor;
    private final Comparator<S> mGroupComparator;

    private S mGroupLeader;
    private G mNextAggregate;

    /**
     * Create a GroupedCursor with an existing group comparator. The comparator
     * defines the ordering of the source cursor, and it should be a partial
     * odering. If group comparator defines a total ordering, then all groups
     * have one member.
     *
     * @param cursor source of elements which must be ordered properly
     * @param groupComparator comparator which defines ordering of source cursor
     */
    protected GroupedCursor(Cursor<S> cursor, Comparator<S> groupComparator) {
        if (cursor == null || groupComparator == null) {
            throw new IllegalArgumentException();
        }
        mCursor = cursor;
        mGroupComparator = groupComparator;
    }

    /**
     * Create a GroupedCursor using properties to define the group
     * comparator. The set of properties defines the ordering of the source
     * cursor, and it should be a partial ordering. If properties define a
     * total ordering, then all groups have one member.
     *
     * @param cursor source of elements which must be ordered properly
     * @param type type of storable to create cursor for
     * @param groupProperties list of properties to group by
     * @throws IllegalArgumentException if any property is null or not a member
     * of storable type
     */
    protected GroupedCursor(Cursor<S> cursor, Class<S> type, String... groupProperties) {
        if (cursor == null) {
            throw new IllegalArgumentException();
        }
        mCursor = cursor;
        mGroupComparator = SortedCursor.createComparator(type, groupProperties);
    }

    /**
     * Returns the comparator used to identify group boundaries.
     */
    public Comparator<S> comparator() {
        return mGroupComparator;
    }

    /**
     * This method is called for the first entry in a group. This method is not
     * called again until after finishGroup is called.
     *
     * @param groupLeader first entry in group
     */
    protected abstract void beginGroup(S groupLeader) throws FetchException;

    /**
     * This method is called when more entries are found for the current
     * group. This method is not called until after beginGroup has been
     * called. It may called multiple times until finishGroup is called.
     *
     * @param groupMember additional entry in group
     */
    protected abstract void addToGroup(S groupMember) throws FetchException;

    /**
     * This method is called when a group is finished, and it can return an
     * aggregate. Simply return null if aggregate should be filtered out.
     *
     * @return aggregate, or null to filter it out
     */
    protected abstract G finishGroup() throws FetchException;

    public void close() throws FetchException {
        mCursor.close();
        mGroupLeader = null;
        mNextAggregate = null;
    }

    public boolean hasNext() throws FetchException {
        if (mNextAggregate != null) {
            return true;
        }

        try {
            int count = 0;
            if (mCursor.hasNext()) {
                if (mGroupLeader == null) {
                    beginGroup(mGroupLeader = mCursor.next());
                }

                while (mCursor.hasNext()) {
                    S groupMember = mCursor.next();

                    if (mGroupComparator.compare(mGroupLeader, groupMember) == 0) {
                        addToGroup(groupMember);
                    } else {
                        G aggregate = finishGroup();
                        beginGroup(mGroupLeader = groupMember);
                        if (aggregate != null) {
                            mNextAggregate = aggregate;
                            return true;
                        }
                    }

                    interruptCheck(++count);
                }

                G aggregate = finishGroup();
                mGroupLeader = null;
                if (aggregate != null) {
                    mNextAggregate = aggregate;
                    return true;
                }
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

        if (mGroupLeader != null) {
            G aggregate = finishGroup();
            mGroupLeader = null;
            if (aggregate != null) {
                mNextAggregate = aggregate;
                return true;
            }
        }

        return false;
    }

    public G next() throws FetchException {
        try {
            if (hasNext()) {
                G next = mNextAggregate;
                mNextAggregate = null;
                return next;
            }
        } catch (FetchException e) {
            try {
                close();
            } catch (Exception e2) {
                // Don't care.
            }
            throw e;
        }
        throw new NoSuchElementException();
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
                interruptCheck(++count);
                mNextAggregate = null;
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

    private void interruptCheck(int count) throws FetchException {
        if ((count & ~0xff) == 0 && Thread.interrupted()) {
            close();
            throw new FetchInterruptedException();
        }
    }
}
