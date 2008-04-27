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

package com.amazon.carbonado;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Standard interface for building up configuration and opening a {@link
 * Repository} instance. All repository implementations should be constructable
 * via a builder that implements this interface. Builders should follow a
 * pattern where configuration is supplied via property access methods. With
 * this design, each item can have extensive documentation and optional
 * configuration can be ignored.
 *
 * <p>A builder design also offers advantages over constructors in that a
 * different repository can be built depending on the specific
 * configuration. This logic is hidden, making it easier to use repositories
 * that would otherwise require complex steps to construct.
 *
 * <p>RepositoryBuilders are not expected to be thread-safe, but the
 * Repositories they build are thread-safe.
 *
 * @author Brian S O'Neill
 */
public interface RepositoryBuilder {
    /**
     * Builds a repository instance.
     *
     * @throws ConfigurationException if there is a problem in the builder's configuration
     * @throws RepositoryException if there is a general problem opening the repository
     */
    Repository build() throws ConfigurationException, RepositoryException;

    /**
     * Builds a repository instance.
     *
     * <p>If the repository is being wrapped by a parent repository, the child
     * repository will need to know this fact for some operations to work
     * correctly. Since the parent repository is not built yet, a reference is
     * used instead.
     *
     * @param rootReference reference to root parent repository, to be set by
     * parent repository upon being built
     * @throws ConfigurationException if there is a problem in the builder's configuration
     * @throws RepositoryException if there is a general problem opening the repository
     */
    Repository build(AtomicReference<Repository> rootReference)
        throws ConfigurationException, RepositoryException;

    /**
     * Returns the name of the repository.
     */
    String getName();

    /**
     * Set name for the repository, which is required.
     */
    void setName(String name);

    /**
     * Returns true if repository should assume the role of master, which is
     * true by default. Repositories that link different repositories together
     * will designate only one as the master.
     *
     * <p>A master repository is responsible for {@link Version version} and
     * {@link Sequence sequence} properties. For insert operations, a master
     * repository must set these properties if they are uninitialized. For
     * updates, the version property is checked to see if an {@link
     * OptimisticLockException} should be thrown.
     *
     * @see com.amazon.carbonado.repo.replicated.ReplicatedRepositoryBuilder
     */
    boolean isMaster();

    /**
     * Set to false if repository should not assume the role of master. By
     * default, this option is true. Repositories that link different
     * repositories together will designate only one as the master.
     *
     * <p>A master repository is responsible for {@link Version version} and
     * {@link Sequence sequence} properties. For insert operations, a master
     * repository must set these properties if they are uninitialized. For
     * updates, the version property is checked to see if an {@link
     * OptimisticLockException} should be thrown.
     *
     * @see com.amazon.carbonado.repo.replicated.ReplicatedRepositoryBuilder
     */
    void setMaster(boolean b);

    /**
     * Optionally add a TriggerFactory which will be called upon to create an
     * initial trigger for each Storable type that the Repository supports. The
     * primary purpose of this method is to allow decorator repositories the
     * opportunity to register custom persistence code for each Storable.
     *
     * @return true if TriggerFactory was added, false if TriggerFactory was
     * not added because an equal TriggerFactory is already registered
     */
    boolean addTriggerFactory(TriggerFactory factory);

    /**
     * Remove a TriggerFactory which was added earlier.
     *
     * @return true if TriggerFactory instance was removed, false if not added
     */
    boolean removeTriggerFactory(TriggerFactory factory);

    /**
     * Returns all the TriggerFactories which were added.
     */
    Iterable<TriggerFactory> getTriggerFactories();
}
