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

import java.io.IOException;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;

/**
 * Supports complex retrieval and deletion of {@link Storable} objects.
 * Queries are immutable representations of an action \u2013 they do not
 * contain any Storable instances. The apparent mutators (with, et al) do not
 * actually modify the Query. Instead, they return another Query instance which
 * has the requested modification. To obtain an initial Query instance, call
 * one of the {@link Storage} query methods.
 *
 * <p>Query objects are usually compiled and cached, and the same instance can
 * be re-used for future queries. This is possible because queries are
 * immutable and naturally thread-safe.
 *
 * @author Brian S O'Neill
 */
public interface Query<S extends Storable> {
    /**
     * Returns the specific type of Storable managed by this object.
     */
    Class<S> getStorableType();

    /**
     * Returns the query's filter.
     */
    Filter<S> getFilter();

    /**
     * Returns the query's filter values, which is null if filter has no
     * parameters.
     */
    FilterValues<S> getFilterValues();

    /**
     * Returns the amount of blank parameters that need to be filled in. If
     * zero, then this query is ready to be used.
     */
    int getBlankParameterCount();

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(int value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(long value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(float value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(double value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(boolean value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(char value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(byte value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(short value);

    /**
     * Returns a copy of this Query with the next blank parameter filled in.
     *
     * @param value parameter value to fill in
     * @throws IllegalStateException if no blank parameters
     * @throws IllegalArgumentException if type doesn't match
     */
    Query<S> with(Object value);

    /**
     * Returns a copy of this Query with the next blank parameters filled in.
     *
     * @param values parameter values to fill in; if null or empty, this
     * Query instance is returned
     * @throws IllegalStateException if no blank parameters or if too many
     * parameter values supplied
     * @throws IllegalArgumentException if any type doesn't match
     */
    Query<S> withValues(Object... values);

    /**
     * Returns a new query which has another {@link Storage#query(String)
     * filter} logically "and"ed to this, potentially reducing the amount of
     * results.
     *
     * @param filter query filter expression
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalStateException if any blank parameters in this query, or
     * if this query is already guaranteed to fetch nothing
     * @throws IllegalArgumentException if filter is null
     * @throws MalformedFilterException if expression is malformed
     * @throws UnsupportedOperationException if given filter is unsupported by repository
     */
    Query<S> and(String filter) throws FetchException;

    /**
     * Returns a new query which has another {@link Storage#query(String)
     * filter} logically "and"ed to this, potentially reducing the amount of
     * results.
     *
     * @param filter query filter
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalStateException if any blank parameters in this query, or
     * if this query is already guaranteed to fetch nothing
     * @throws IllegalArgumentException if filter is null
     * @throws UnsupportedOperationException if given filter is unsupported by repository
     */
    Query<S> and(Filter<S> filter) throws FetchException;

    /**
     * Returns a new query which has another {@link Storage#query(String)
     * filter} logically "or"ed to this, potentially increasing the amount of
     * results.
     *
     * @param filter query filter expression
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalStateException if any blank parameters in this query, or
     * if this query is already guaranteed to fetch everything
     * @throws IllegalArgumentException if filter is null
     * @throws MalformedFilterException if expression is malformed
     * @throws UnsupportedOperationException if given filter is unsupported by repository
     */
    Query<S> or(String filter) throws FetchException;

    /**
     * Returns a new query which has another {@link Storage#query(String)
     * filter} logically "or"ed to this, potentially increasing the amount of
     * results.
     *
     * @param filter query filter
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalStateException if any blank parameters in this query, or
     * if this query is already guaranteed to fetch everything
     * @throws IllegalArgumentException if filter is null
     * @throws UnsupportedOperationException if given filter is unsupported by repository
     */
    Query<S> or(Filter<S> filter) throws FetchException;

    /**
     * Returns a new query which produces all the results not supplied in this
     * query. Any filled in parameters in this query are copied into the new
     * one.
     *
     * @throws FetchException if storage layer throws an exception
     * @throws UnsupportedOperationException if new query is unsupported by repository
     */
    Query<S> not() throws FetchException;

    /**
     * Returns a copy of this query ordered by a specific property value. The
     * property name may be prefixed with '+' or '-' to indicate ascending or
     * descending order. If the prefix is omitted, ascending order is assumed.
     *
     * <p>Note: Specification of ordering properties is not cumulative. Calling
     * this method will first remove any previous ordering properties.
     *
     * @param property name of property to order by
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalArgumentException if property is null or is not a member
     * of type S
     * @throws UnsupportedOperationException if given ordering, combined with
     * query filter, is unsupported by repository
     */
    Query<S> orderBy(String property) throws FetchException;

    /**
     * Returns a copy of this query ordered by specific property values. The
     * property names may be prefixed with '+' or '-' to indicate ascending or
     * descending order. If the prefix is omitted, ascending order is assumed.
     *
     * <p>Note: Specification of ordering properties is not cumulative. Calling
     * this method will first remove any previous ordering properties.
     *
     * @param properties names of properties to order by
     * @throws FetchException if storage layer throws an exception
     * @throws IllegalArgumentException if any property is null or is not a
     * member of type S
     * @throws UnsupportedOperationException if given ordering, combined with
     * query filter, is unsupported by repository
     */
    Query<S> orderBy(String... properties) throws FetchException;

    /**
     * Returns a query which fetches results for this query after a given
     * starting point, which is useful for re-opening a cursor. This is only
     * effective when query has been given an explicit {@link #orderBy
     * ordering}. If not a total ordering, then query may start at an earlier
     * position.
     *
     * <p>Note: The returned query can be very expensive to fetch from
     * repeatedly, if the query needs to perform a sort operation. Ideally, the
     * query ordering should match the natural ordering of an index or key.
     *
     * @param start storable to attempt to start after; if null, this query is
     * returned
     * @throws IllegalStateException if any blank parameters in this query
     * @throws FetchException if storage layer throws an exception
     * @since 1.2
     */
    <T extends S> Query<S> after(T start) throws FetchException;

    /**
     * Fetches results for this query. If any updates or deletes might be
     * performed on the results, consider enclosing the fetch in a
     * transaction. This allows the isolation level and "for update" mode to be
     * adjusted. Some repositories might otherwise deadlock.
     *
     * @return fetch results
     * @throws IllegalStateException if any blank parameters in this query
     * @throws FetchException if storage layer throws an exception
     * @see Repository#enterTransaction(IsolationLevel)
     */
    Cursor<S> fetch() throws FetchException;

    /**
     * Fetches a slice of results for this query, as defined by a numerical
     * range. A slice can be used to limit the number of results from a
     * query. It is strongly recommended that the query be given a total {@link
     * #orderBy ordering} in order for the slice results to be deterministic.
     *
     * @param from zero-based {@code from} record number, inclusive
     * @param to optional zero-based {@code to} record number, exclusive
     * @return fetch results
     * @throws IllegalStateException if any blank parameters in this query
     * @throws IllegalArgumentException if {@code from} is negative or if
     * {@code from} is more than {@code to}
     * @throws FetchException if storage layer throws an exception
     * @since 1.2
     */
    Cursor<S> fetchSlice(long from, Long to) throws FetchException;

    /**
     * Fetches results for this query after a given starting point, which is
     * useful for re-opening a cursor. This is only effective when query has
     * been given an explicit {@link #orderBy ordering}. If not a total
     * ordering, then cursor may start at an earlier position.
     *
     * <p>Note: This method can be very expensive to call repeatedly, if the
     * query needs to perform a sort operation. Ideally, the query ordering
     * should match the natural ordering of an index or key.
     *
     * <p>Calling {@code fetchAfter(s)} is equivalent to calling {@code
     * after(s).fetch()}.
     *
     * @param start storable to attempt to start after; if null, fetch all results
     * @return fetch results
     * @throws IllegalStateException if any blank parameters in this query
     * @throws FetchException if storage layer throws an exception
     * @see Repository#enterTransaction(IsolationLevel)
     * @see #after
     */
    <T extends S> Cursor<S> fetchAfter(T start) throws FetchException;

    /**
     * Attempts to load exactly one matching object. If the number of matching
     * records is zero or exceeds one, then an exception is thrown instead.
     *
     * @return a single fetched object
     * @throws IllegalStateException if any blank parameters in this query
     * @throws FetchNoneException if no matching record found
     * @throws FetchMultipleException if more than one matching record found
     * @throws FetchException if storage layer throws an exception
     */
    S loadOne() throws FetchException;

    /**
     * May return null if nothing found. Throws exception if record count is
     * more than one.
     *
     * @return null or a single fetched object
     * @throws IllegalStateException if any blank parameters in this query
     * @throws FetchMultipleException if more than one matching record found
     * @throws FetchException if storage layer throws an exception
     */
    S tryLoadOne() throws FetchException;

    /**
     * Deletes one matching object. If the number of matching records is zero or
     * exceeds one, then no delete occurs, and an exception is thrown instead.
     *
     * @throws IllegalStateException if any blank parameters in this query
     * @throws PersistNoneException if no matching record found
     * @throws PersistMultipleException if more than one record matches
     * @throws PersistException if storage layer throws an exception
     */
    void deleteOne() throws PersistException;

    /**
     * Deletes zero or one matching objects. If the number of matching records
     * exceeds one, then no delete occurs, and an exception is thrown instead.
     *
     * @return true if record existed and was deleted, or false if no match
     * @throws IllegalStateException if any blank parameters in this query
     * @throws PersistMultipleException if more than one record matches
     * @throws PersistException if storage layer throws an exception
     */
    boolean tryDeleteOne() throws PersistException;

    /**
     * Deletes zero or more matching objects. There is no guarantee that
     * deleteAll is an atomic operation. If atomic behavior is desired, wrap
     * the call in a transaction scope.
     *
     * @throws IllegalStateException if any blank parameters in this query
     * @throws PersistException if storage layer throws an exception
     */
    void deleteAll() throws PersistException;

    /**
     * Returns a count of all results matched by this query. Even though no
     * results are explicitly fetched, this method may still be expensive to
     * call. The actual performance will vary by repository and available indexes.
     *
     * @return count of matches
     * @throws IllegalStateException if any blank parameters in this query
     * @throws FetchException if storage layer throws an exception
     */
    long count() throws FetchException;

    /**
     * Returns true if any results are matched by this query.
     *
     * @return true if any matches
     * @throws IllegalStateException if any blank parameters in this query
     * @throws FetchException if storage layer throws an exception
     * @since 1.2
     */
    boolean exists() throws FetchException;

    /**
     * Print the native query to standard out, which is useful for performance
     * analysis. Not all repositories have a native query format. An example
     * native format is SQL.
     *
     * @return false if not implemented
     */
    boolean printNative();

    /**
     * Prints the native query to any appendable, which is useful for
     * performance analysis. Not all repositories have a native query
     * format. An example native format is SQL.
     *
     * @param app append results here
     * @return false if not implemented
     */
    boolean printNative(Appendable app) throws IOException;

    /**
     * Prints the native query to any appendable, which is useful for
     * performance analysis. Not all repositories have a native query
     * format. An example native format is SQL.
     *
     * @param app append results here
     * @param indentLevel amount to indent text, zero for none
     * @return false if not implemented
     */
    boolean printNative(Appendable app, int indentLevel) throws IOException;

    /**
     * Prints the query excecution plan to standard out, which is useful for
     * performance analysis. There is no standard format for query plans, nor
     * is it a requirement that this method be implemented.
     *
     * @return false if not implemented
     */
    boolean printPlan();

    /**
     * Prints the query excecution plan to any appendable, which is useful for
     * performance analysis. There is no standard format for query plans, nor
     * is it a requirement that this method be implemented.
     *
     * @param app append results here
     * @return false if not implemented
     */
    boolean printPlan(Appendable app) throws IOException;

    /**
     * Prints the query excecution plan to any appendable, which is useful for
     * performance analysis. There is no standard format for query plans, nor
     * is it a requirement that this method be implemented.
     *
     * @param app append results here
     * @param indentLevel amount to indent text, zero for none
     * @return false if not implemented
     */
    boolean printPlan(Appendable app, int indentLevel) throws IOException;

    int hashCode();

    boolean equals(Object obj);

    /**
     * Returns a description of the query filter and any other arguments.
     */
    String toString();
}
