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

import com.amazon.carbonado.filter.Filter;

/**
 * Access for a specific type of {@link Storable} from a {@link Repository}.
 *
 * <p>Storage instances are mutable, but they are thread-safe.
 *
 * @author Brian S O'Neill
 */
public interface Storage<S extends Storable> {
    /**
     * Returns the specific type of Storable managed by this object.
     */
    Class<S> getStorableType();

    /**
     * Prepares a new object for loading, inserting, updating, or deleting.
     *
     * @return a new data access object
     */
    S prepare();

    /**
     * Query for all Storable instances in this Storage.
     *
     * @see #query(String)
     * @throws FetchException if storage layer throws an exception
     */
    Query<S> query() throws FetchException;

    /**
     * Query for Storable instances against a filter expression. A filter tests
     * if property values match against specific values specified by '?'
     * placeholders. The simplest filter compares just one property, like
     * {@code "ID = ?"}. Filters can also contain several kinds of relational
     * operators, boolean logic operators, sub-properties, and parentheses. A
     * more complex example might be {@code "income < ? | (name = ? & address.zipCode != ?)"}.
     * <p>
     * When querying for a single Storable instance by its primary key, it is
     * generally more efficient to call {@link #prepare()}, set primary key
     * properties, and then call {@link Storable#load()}. For example, consider
     * an object with a primary key consisting only of the property "ID". It
     * can be queried as:
     * <pre>
     * Storage&lt;UserInfo&gt; users;
     * UserInfo user = users.query("ID = ?").with(123456).loadOne();
     * </pre>
     * The above code will likely open a Cursor in order to verify that just
     * one object was loaded. Instead, do this:
     * <pre>
     * Storage&lt;UserInfo&gt; users;
     * UserInfo user = users.prepare();
     * user.setID(123456);
     * user.load();
     * </pre>
     * The complete syntax for query filters follows. Note that:
     * <ul>
     * <li> literals are not allowed
     * <li> logical 'and' operator has precedence over 'or'
     * <li> logical 'not' operator has precedence over 'and'
     * <li> '?' placeholders can only appear after relational operators
     * </ul>
     * <pre>
     * Filter          = OrFilter
     * OrFilter        = AndFilter { "|" AndFilter }
     * AndFilter       = NotFilter { "&" NotFilter }
     * NotFilter       = [ "!" ] EntityFilter
     * EntityFilter    = PropertyFilter
     *                 = ChainedFilter
     *                 | "(" Filter ")"
     * PropertyFilter  = ChainedProperty RelOp "?"
     * RelOp           = "=" | "!=" | "&lt;" | "&gt;=" | "&gt;" | "&lt;="
     * ChainedFilter   = ChainedProperty "(" [ Filter ] ")"
     * ChainedProperty = Identifier
     *                 | InnerJoin "." ChainedProperty
     *                 | OuterJoin "." ChainedProperty
     * InnerJoin       = Identifier
     * OuterJoin       = "(" Identifier ")"
     * </pre>
     *
     * @param filter query filter expression
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalArgumentException if filter is null
     * @throws MalformedFilterException if expression is malformed
     * @throws UnsupportedOperationException if given filter is unsupported by repository
     */
    Query<S> query(String filter) throws FetchException;

    /**
     * Query for Storable instances against an explicitly constructed filter
     * object.
     *
     * @param filter query filter
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalArgumentException if filter is null
     * @throws UnsupportedOperationException if given filter is unsupported by repository
     */
    Query<S> query(Filter<S> filter) throws FetchException;

    /**
     * Attempts to quickly delete all Storables instances in this
     * Storage. Support for transactional truncation is not guaranteed.
     *
     * <p>If this Storage has any registered triggers which act on deletes, all
     * Storables are deleted via {@code query().deleteAll()} instead to ensure
     * these triggers get run.
     *
     * @since 1.2
     */
    void truncate() throws PersistException;

    /**
     * Register a trigger which will be called for overridden methods in the given
     * trigger implementation. The newly added trigger is invoked before and
     * after all other triggers. In other words, it is added at the outermost
     * nesting level.
     *
     * @return true if trigger was added, false if trigger was not added
     * because an equal trigger is already registered
     * @throws IllegalArgumentException if trigger is null
     */
    boolean addTrigger(Trigger<? super S> trigger);

    /**
     * Remove a trigger which was registered earlier.
     *
     * @return true if trigger instance was removed, false if not registered
     * @throws IllegalArgumentException if trigger is null
     */
    boolean removeTrigger(Trigger<? super S> trigger);
}
