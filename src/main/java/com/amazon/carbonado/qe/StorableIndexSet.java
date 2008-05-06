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

package com.amazon.carbonado.qe;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableKey;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Manages a set of {@link StorableIndex} objects, intended for reducing the
 * set such that the minimal amount of physical indexes need to be defined for
 * a specific type of {@link Storable}.
 *
 * @author Brian S O'Neill
 */
public class StorableIndexSet<S extends Storable> extends TreeSet<StorableIndex<S>> {

    private static final long serialVersionUID = -5840661016235340456L;

    private static final Comparator<StorableIndex<?>> STORABLE_INDEX_COMPARATOR =
        new StorableIndexComparator();

    public StorableIndexSet() {
        super(STORABLE_INDEX_COMPARATOR);
    }

    /**
     * Copy constructor.
     */
    public StorableIndexSet(StorableIndexSet<S> set) {
        super(STORABLE_INDEX_COMPARATOR);
        addAll(set);
    }

    /**
     * Adds all the indexes of the given storable.
     *
     * @throws IllegalArgumentException if info is null
     */
    public void addIndexes(StorableInfo<S> info) {
        for (int i=info.getIndexCount(); --i>=0; ) {
            add(info.getIndex(i));
        }
    }

    /**
     * Adds all the indexes of the given storable.
     *
     * @param defaultDirection default ordering direction to apply to each
     * index property
     * @throws IllegalArgumentException if any argument is null
     */
    public void addIndexes(StorableInfo<S> info, Direction defaultDirection) {
        for (int i=info.getIndexCount(); --i>=0; ) {
            add(info.getIndex(i).setDefaultDirection(defaultDirection));
        }
    }

    /**
     * Adds all of the alternate keys of the given storable as indexes by
     * calling {@link #addKey addKey}.
     *
     * @throws IllegalArgumentException if info is null
     */
    public void addAlternateKeys(StorableInfo<S> info) {
        if (info == null) {
            throw new IllegalArgumentException();
        }
        for (int i=info.getAlternateKeyCount(); --i>=0; ) {
            addKey(info.getAlternateKey(i));
        }
    }

    /**
     * Adds the primary key of the given storable as indexes by calling {@link
     * #addKey addKey}. This method should not be called if the primary key
     * cannot be altered because persistent data is already stored against
     * it. Instead, the primary key index should be added as a normal index.
     *
     * <p>After adding the primary key via this method and after reducing the
     * set, call {@link #findPrimaryKeyIndex findPrimaryKeyIndex} to get the
     * best index to represent the primary key.
     *
     * @throws IllegalArgumentException if info is null
     */
    public void addPrimaryKey(StorableInfo<S> info) {
        if (info == null) {
            throw new IllegalArgumentException();
        }
        addKey(info.getPrimaryKey());
    }

    /**
     * Adds the key as a unique index, preserving the property arrangement.
     *
     * @throws IllegalArgumentException if key is null
     */
    @SuppressWarnings("unchecked")
    public void addKey(StorableKey<S> key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }
        add(new StorableIndex<S>(key, Direction.UNSPECIFIED));
    }

    /**
     * Reduces the size of the set by removing redundant indexes, and merges
     * others together.
     */
    public void reduce() {
        reduce(Direction.UNSPECIFIED);
    }

    /**
     * Reduces the size of the set by removing redundant indexes, and merges
     * others together.
     *
     * @param defaultDirection replace unspecified property directions with this
     */
    public void reduce(Direction defaultDirection) {
        List<StorableIndex<S>> group = new ArrayList<StorableIndex<S>>();
        Map<StorableIndex<S>, StorableIndex<S>> mergedReplacements =
            new TreeMap<StorableIndex<S>, StorableIndex<S>>(STORABLE_INDEX_COMPARATOR);

        Iterator<StorableIndex<S>> it = iterator();
        while (it.hasNext()) {
            StorableIndex<S> candidate = it.next();

            if (group.size() == 0 || isDifferentGroup(group.get(0), candidate)) {
                group.clear();
                group.add(candidate);
                continue;
            }

            if (isRedundant(group, candidate, mergedReplacements)) {
                it.remove();
            } else {
                group.add(candidate);
            }
        }

        // Now replace merged indexes.
        replaceEntries(mergedReplacements);

        setDefaultDirection(defaultDirection);
    }

    /**
     * Set the default direction for all index properties.
     *
     * @param defaultDirection replace unspecified property directions with this
     */
    public void setDefaultDirection(Direction defaultDirection) {
        // Apply default sort direction to those unspecified.
        if (defaultDirection != Direction.UNSPECIFIED) {
            Map<StorableIndex<S>, StorableIndex<S>> replacements = null;
            for (StorableIndex<S> index : this) {
                StorableIndex<S> replacement = index.setDefaultDirection(defaultDirection);
                if (replacement != index) {
                    if (replacements == null) {
                        replacements = new HashMap<StorableIndex<S>, StorableIndex<S>>();
                    }
                    replacements.put(index, replacement);
                }
            }
            replaceEntries(replacements);
        }
    }

    /**
     * Marks all indexes as clustered or non-clustered.
     *
     * @param clustered true to mark clustered; false to mark non-clustered
     * @see StorableIndex#isClustered()
     * @since 1.2
     */
    public void markClustered(boolean clustered) {
        Map<StorableIndex<S>, StorableIndex<S>> replacements = null;
        for (StorableIndex<S> index : this) {
            StorableIndex<S> replacement = index.clustered(clustered);
            if (replacement != index) {
                if (replacements == null) {
                    replacements = new HashMap<StorableIndex<S>, StorableIndex<S>>();
                }
                replacements.put(index, replacement);
            }
        }
        replaceEntries(replacements);
    }

    /**
     * Augment non-unique indexes with primary key properties, thus making them
     * unique.
     *
     * @throws IllegalArgumentException if info is null
     */
    public void uniquify(StorableInfo<S> info) {
        if (info == null) {
            throw new IllegalArgumentException();
        }
        uniquify(info.getPrimaryKey());
    }

    /**
     * Augment non-unique indexes with key properties, thus making them unique.
     *
     * @throws IllegalArgumentException if key is null
     */
    public void uniquify(StorableKey<S> key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }


        // Replace indexes which were are implied unique, even if they are not
        // declared as such.
        {
            Map<StorableIndex<S>, StorableIndex<S>> replacements = null;
            for (StorableIndex<S> index : this) {
                if (!index.isUnique() && isUniqueImplied(index)) {
                    if (replacements == null) {
                        replacements = new HashMap<StorableIndex<S>, StorableIndex<S>>();
                    }
                    replacements.put(index, index.unique(true));
                }
            }
            replaceEntries(replacements);
        }

        // Now augment with key properties.
        {
            Map<StorableIndex<S>, StorableIndex<S>> replacements = null;
            for (StorableIndex<S> index : this) {
                StorableIndex<S> replacement = index.uniquify(key);
                if (replacement != index) {
                    if (replacements == null) {
                        replacements = new HashMap<StorableIndex<S>, StorableIndex<S>>();
                    }
                    replacements.put(index, replacement);
                }
            }
            replaceEntries(replacements);
        }
    }

    /**
     * Finds the best index to represent the primary key. Should be called
     * after calling reduce. As long as the primary key was added via {@link
     * #addPrimaryKey addPrimaryKey}, this method should never return null.
     *
     * @throws IllegalArgumentException if info is null
     */
    public StorableIndex<S> findPrimaryKeyIndex(StorableInfo<S> info) {
        if (info == null) {
            throw new IllegalArgumentException();
        }
        return findKeyIndex(info.getPrimaryKey());
    }

    /**
     * Finds the best index to represent the given key. Should be called after
     * calling reduce. As long as the key was added via {@link #addKey addKey},
     * this method should never return null.
     *
     * @throws IllegalArgumentException if key is null
     */
    public StorableIndex<S> findKeyIndex(StorableKey<S> key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        Set<? extends OrderedProperty<S>> orderedProps = key.getProperties();

        Set<StorableProperty<S>> keyProps = new HashSet<StorableProperty<S>>();
        for (OrderedProperty<S> orderedProp : orderedProps) {
            keyProps.add(orderedProp.getChainedProperty().getPrimeProperty());
        }

        search: for (StorableIndex<S> index : this) {
            if (!index.isUnique() || index.getPropertyCount() != keyProps.size()) {
                continue search;
            }
            for (int i=index.getPropertyCount(); --i>=0; ) {
                if (!keyProps.contains(index.getProperty(i))) {
                    continue search;
                }
            }
            return index;
        }

        return null;
    }

    /**
     * Return true if index is unique or fully contains the members of a unique index.
     */
    private boolean isUniqueImplied(StorableIndex<S> candidate) {
        if (candidate.isUnique()) {
            return true;
        }
        if (this.size() <= 1) {
            return false;
        }

        Set<StorableProperty<S>> candidateProps = new HashSet<StorableProperty<S>>();
        for (int i=candidate.getPropertyCount(); --i>=0; ) {
            candidateProps.add(candidate.getProperty(i));
        }

        search: for (StorableIndex<S> index : this) {
            if (!index.isUnique()) {
                continue search;
            }
            for (int i=index.getPropertyCount(); --i>=0; ) {
                if (!candidateProps.contains(index.getProperty(i))) {
                    continue search;
                }
            }
            return true;
        }

        return false;
    }

    private boolean isDifferentGroup(StorableIndex<S> groupLeader, StorableIndex<S> candidate) {
        int count = candidate.getPropertyCount();
        if (count > groupLeader.getPropertyCount()) {
            return true;
        }
        for (int i=0; i<count; i++) {
            StorableProperty aProp = groupLeader.getProperty(i);
            StorableProperty bProp = candidate.getProperty(i);
            if (aProp.getName().compareTo(bProp.getName()) != 0) {
                return true;
            }
        }
        return candidate.isUnique() && (count < groupLeader.getPropertyCount());
    }

    /**
     * Returns true if candidate index is less qualified than an existing group
     * member, or if it was merged with another group member. If it was merged,
     * then an entry is placed in the merged map, and the given group list is
     * updated.
     */
    private boolean isRedundant(List<StorableIndex<S>> group, StorableIndex<S> candidate,
                                Map<StorableIndex<S>, StorableIndex<S>> mergedReplacements) {
        // All visited group members will have an equal or greater number of
        // properties. This is ensured by the ordering of the set.
        int count = candidate.getPropertyCount();

        ListIterator<StorableIndex<S>> it = group.listIterator();
        groupScan:
        while (it.hasNext()) {
            StorableIndex<S> member = it.next();

            boolean moreQualified = false;
            boolean canReverse = true;
            boolean reverse = false;

            for (int i=0; i<count; i++) {
                Direction candidateOrder = candidate.getPropertyDirection(i);
                if (candidateOrder == Direction.UNSPECIFIED) {
                    // Property direction is unspecified, so no need to compare
                    // direction. Move on to next property.
                    continue;
                }

                Direction memberOrder = member.getPropertyDirection(i);
                if (memberOrder == Direction.UNSPECIFIED) {
                    // Candidate index is more qualified because member
                    // property under examination hasn't specified a
                    // direction. Move on to next property to continue checking
                    // if a merge is possible.
                    moreQualified = true;
                    continue;
                }

                if (reverse) {
                    candidateOrder = candidateOrder.reverse();
                }

                if (candidateOrder == memberOrder) {
                    // Direction exactly matches, move on to next property.
                    canReverse = false;
                    continue;
                }

                // If this point is reached, then the direction would match if
                // one was reversed. For an index to fully match, all
                // properties must be reversed.

                if (canReverse) {
                    // Switch to reverse mode and move on to next property.
                    reverse = true;
                    canReverse = false;
                    continue;
                }

                // Match failed and merge is not possible.
                continue groupScan;
            }

            if (moreQualified) {
                // Candidate is more qualified than all members compared to so
                // far, but it can be merged. Once merged, it is redundant.
                Direction[] directions = member.getPropertyDirections();
                for (int i=0; i<count; i++) {
                    if (directions[i] == Direction.UNSPECIFIED) {
                        Direction direction = candidate.getPropertyDirection(i);
                        directions[i] = reverse ? direction.reverse() : direction;
                    }
                }

                StorableIndex<S> merged =
                    new StorableIndex<S>(member.getProperties(), directions)
                    .unique(member.isUnique());
                mergedReplacements.put(member, merged);
                it.set(merged);
            }

            // Candidate is redundant.
            return true;
        }

        return false;
    }

    private void replaceEntries(Map<StorableIndex<S>, StorableIndex<S>> replacements) {
        if (replacements != null) {
            for (Map.Entry<StorableIndex<S>, StorableIndex<S>> e : replacements.entrySet()) {
                remove(e.getKey());
                add(e.getValue());
            }
        }
    }

    /**
     * Orders indexes such that they are grouped by property names. Within
     * those groups, indexes are ordered most qualified to least qualified.
     */
    private static class StorableIndexComparator
        implements Comparator<StorableIndex<?>>, java.io.Serializable
    {
        private static final long serialVersionUID = 2204885249683067349L;

        public int compare(StorableIndex<?> a, StorableIndex<?> b) {
            if (a == b) {
                return 0;
            }

            int aCount = a.getPropertyCount();
            int bCount = b.getPropertyCount();

            int count = Math.min(aCount, bCount);

            for (int i=0; i<count; i++) {
                StorableProperty aProp = a.getProperty(i);
                StorableProperty bProp = b.getProperty(i);
                int result = aProp.getName().compareTo(bProp.getName());
                if (aProp.getName().compareTo(bProp.getName()) != 0) {
                    return result;
                }
            }

            // Index with more properties is first.
            if (aCount > bCount) {
                return -1;
            } else if (aCount < bCount) {
                return 1;
            }

            // Counts are the same, property names are the same. Unique indexes
            // are first, followed by index with more leading directions. Favor
            // ascending direction.

            for (int i=0; i<count; i++) {
                if (a.isUnique()) {
                    if (!b.isUnique()) {
                        return -1;
                    }
                } else if (b.isUnique()) {
                    return 1;
                }

                Direction aDirection = a.getPropertyDirection(i);
                Direction bDirection = b.getPropertyDirection(i);

                if (aDirection == bDirection) {
                    continue;
                }

                // These order in which these tests are performed must not be
                // altered without careful examination.

                if (aDirection == Direction.ASCENDING) {
                    return -1;
                }
                if (bDirection == Direction.ASCENDING) {
                    return 1;
                }
                if (aDirection == Direction.DESCENDING) {
                    return -1;
                }
                if (bDirection == Direction.DESCENDING) {
                    return 1;
                }
            }

            return 0;
        }
    }
}
