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

package com.amazon.carbonado.filter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.cojen.util.SoftValuedHashMap;
import org.cojen.util.WeakCanonicalSet;
import org.cojen.util.WeakIdentityMap;

import com.amazon.carbonado.MalformedFilterException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.StorableIntrospector;

import com.amazon.carbonado.util.Appender;

/**
 * An immutable tree structure representing a query result filter. Filters can
 * be created using a builder pattern, by expression parsing, or by a
 * combination of techniques. Filter instances are canonical, which means that
 * equivalent instances can be compared for equality using the '==' operator.
 *
 * <p>Any method that accepts a filter expression parses against the following
 * syntax:
 *
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
 * @author Brian S O'Neill
 */
public abstract class Filter<S extends Storable> implements Serializable, Appender {

    private static final Object OPEN_KEY = new Object();
    private static final Object CLOSED_KEY = new Object();

    // Collection of canonical filters.
    static WeakCanonicalSet cCanonical = new WeakCanonicalSet();

    // Map<(weak)Class<S>, Map<Object, (soft)Filter<S>>>
    private static Map cCache = new WeakIdentityMap();

    /**
     * Returns a cached filter instance that operates on the given type and
     * filter expression.
     *
     * @param type type of Storable that query is made against
     * @param expression query filter expression to parse
     * @return canonical Filter instance
     * @throws IllegalArgumentException if type or filter expression is null
     * @throws MalformedFilterException if filter expression is malformed
     */
    public static <S extends Storable> Filter<S> filterFor(Class<S> type, String expression) {
        Map<Object, Filter<S>> filterCache = getFilterCache(type);
        synchronized (filterCache) {
            Filter<S> filter = filterCache.get(expression);
            if (filter == null) {
                filter = new FilterParser<S>(type, expression).parseRoot();
                filterCache.put(expression, filter);
            }
            return filter;
        }
    }

    /**
     * Returns a cached filter instance that operates on the given type, which
     * allows all results to pass through.
     *
     * @param type type of Storable that query is made against
     * @return canonical Filter instance
     * @see OpenFilter
     */
    public static <S extends Storable> OpenFilter<S> getOpenFilter(Class<S> type) {
        Map<Object, Filter<S>> filterCache = getFilterCache(type);
        synchronized (filterCache) {
            Filter<S> filter = filterCache.get(OPEN_KEY);
            if (filter == null) {
                filter = OpenFilter.getCanonical(type);
                filterCache.put(OPEN_KEY, filter);
            }
            return (OpenFilter<S>) filter;
        }
    }

    /**
     * Returns a cached filter instance that operates on the given type, which
     * prevents any results from passing through.
     *
     * @param type type of Storable that query is made against
     * @return canonical Filter instance
     * @see ClosedFilter
     */
    public static <S extends Storable> ClosedFilter<S> getClosedFilter(Class<S> type) {
        Map<Object, Filter<S>> filterCache = getFilterCache(type);
        synchronized (filterCache) {
            Filter<S> filter = filterCache.get(CLOSED_KEY);
            if (filter == null) {
                filter = ClosedFilter.getCanonical(type);
                filterCache.put(CLOSED_KEY, filter);
            }
            return (ClosedFilter<S>) filter;
        }
    }

    @SuppressWarnings("unchecked")
    private static <S extends Storable> Map<Object, Filter<S>> getFilterCache(Class<S> type) {
        synchronized (cCache) {
            Map<Object, Filter<S>> filterCache = (Map<Object, Filter<S>>) cCache.get(type);
            if (filterCache == null) {
                filterCache = new SoftValuedHashMap();
                cCache.put(type, filterCache);
            }
            return filterCache;
        }
    }

    private final Class<S> mType;

    private transient int mHashCode;

    // Root FilterValues, built on demand, which is immutable.
    private transient volatile FilterValues<S> mFilterValues;

    // Tail of PropertyFilterList used by mFilterValues. mFilterValues
    // references the head.
    private transient volatile PropertyFilterList<S> mTailPropertyFilterList;

    /**
     * Package-private constructor to prevent subclassing outside this package.
     */
    Filter(Class<S> type) {
        mType = type;
    }

    /**
     * Returns the storable type that this filter operates on.
     */
    public Class<S> getStorableType() {
        return mType;
    }

    /**
     * Returns a FilterValues instance for assigning values to a
     * Filter. Returns null if Filter has no parameters.
     *
     * <p>Note: The returned FilterValues instance may reference a different
     * filter instance than this one. Call getFilter to retrieve it. The
     * difference is caused by the filter property values being {@link #bind bound}.
     */
    public FilterValues<S> initialFilterValues() {
        FilterValues<S> filterValues = mFilterValues;
        if (filterValues == null) {
            buildFilterValues();
            filterValues = mFilterValues;
        }
        return filterValues;
    }

    /**
     * Returns tail of linked list, and so it can only be traversed by getting
     * previous nodes.
     *
     * @return tail of PropertyFilterList, or null if no parameters
     */
    PropertyFilterList<S> getTailPropertyFilterList() {
        PropertyFilterList<S> tail = mTailPropertyFilterList;
        if (tail == null) {
            buildFilterValues();
            tail = mTailPropertyFilterList;
        }
        return tail;
    }

    private void buildFilterValues() {
        Filter<S> boundFilter = bind();

        if (boundFilter != this) {
            mFilterValues = boundFilter.initialFilterValues();
            mTailPropertyFilterList = boundFilter.getTailPropertyFilterList();
            return;
        }

        PropertyFilterList<S> list = accept(new PropertyFilterList.Builder<S>(), null);

        if (list != null) {
            // Since FilterValues instances are immutable, save this for re-use.
            mFilterValues = FilterValues.create(this, list);

            // PropertyFilterList can be saved for re-use because it too is
            // immutable (after PropertyFilterListBuilder has run).
            mTailPropertyFilterList = list.get(-1);
        }
    }

    /**
     * Returns a combined filter instance that accepts records which are only
     * accepted by this filter and the one given.
     *
     * @param expression query filter expression to parse
     * @return canonical Filter instance
     * @throws IllegalArgumentException if filter is null
     */
    public final Filter<S> and(String expression) {
        return and(new FilterParser<S>(mType, expression).parseRoot());
    }

    /**
     * Returns a combined filter instance that accepts records which are only
     * accepted by this filter and the one given.
     *
     * @return canonical Filter instance
     * @throws IllegalArgumentException if filter is null
     */
    public Filter<S> and(Filter<S> filter) {
        if (filter.isOpen()) {
            return this;
        }
        if (filter.isClosed()) {
            return filter;
        }
        return AndFilter.getCanonical(this, filter);
    }

    /**
     * Returns a combined filter instance that accepts records which are only
     * accepted by this filter and the one given.
     *
     * @param propertyName property name to match on, which may be a chained property
     * @param operator relational operator
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     */
    public final Filter<S> and(String propertyName, RelOp operator) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return and(PropertyFilter.getCanonical(prop, operator, 0));
    }

    /**
     * Returns a combined filter instance that accepts records which are only
     * accepted by this filter and the one given.
     *
     * @param propertyName property name to match on, which may be a chained property
     * @param operator relational operator
     * @param constantValue constant value to match
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     */
    public final Filter<S> and(String propertyName, RelOp operator, Object constantValue) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return and(PropertyFilter.getCanonical(prop, operator, 0).constant(constantValue));
    }

    /**
     * Returns a combined filter instance that accepts records which are only
     * accepted by this filter and the "exists" test applied to a join.
     *
     * @param propertyName join property name, which may be a chained property
     * @param subFilter sub-filter to apply to join, which may be null to test
     * for any existing
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     * @since 1.2
     */
    public final Filter<S> andExists(String propertyName, Filter<?> subFilter) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return and(ExistsFilter.build(prop, subFilter, false));
    }

    /**
     * Returns a combined filter instance that accepts records which are only
     * accepted by this filter and the "not exists" test applied to a join.
     *
     * @param propertyName join property name, which may be a chained property
     * @param subFilter sub-filter to apply to join, which may be null to test
     * for any not existing
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     * @since 1.2
     */
    public final Filter<S> andNotExists(String propertyName, Filter<?> subFilter) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return and(ExistsFilter.build(prop, subFilter, true));
    }

    /**
     * Returns a combined filter instance that accepts records which are
     * accepted either by this filter or the one given.
     *
     * @param expression query filter expression to parse
     * @return canonical Filter instance
     * @throws IllegalArgumentException if filter is null
     */
    public final Filter<S> or(String expression) {
        return or(new FilterParser<S>(mType, expression).parseRoot());
    }

    /**
     * Returns a combined filter instance that accepts records which are
     * accepted either by this filter or the one given.
     *
     * @return canonical Filter instance
     * @throws IllegalArgumentException if filter is null
     */
    public Filter<S> or(Filter<S> filter) {
        if (filter.isOpen()) {
            return filter;
        }
        if (filter.isClosed()) {
            return this;
        }
        return OrFilter.getCanonical(this, filter);
    }

    /**
     * Returns a combined filter instance that accepts records which are
     * accepted either by this filter or the one given.
     *
     * @param propertyName property name to match on, which may be a chained property
     * @param operator relational operator
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     */
    public final Filter<S> or(String propertyName, RelOp operator) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return or(PropertyFilter.getCanonical(prop, operator, 0));
    }

    /**
     * Returns a combined filter instance that accepts records which are
     * accepted either by this filter or the one given.
     *
     * @param propertyName property name to match on, which may be a chained property
     * @param operator relational operator
     * @param constantValue constant value to match
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     */
    public final Filter<S> or(String propertyName, RelOp operator, Object constantValue) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return or(PropertyFilter.getCanonical(prop, operator, 0).constant(constantValue));
    }

    /**
     * Returns a combined filter instance that accepts records which are
     * accepted either by this filter or the "exists" test applied to a join.
     *
     * @param propertyName one-to-many join property name, which may be a chained property
     * @param subFilter sub-filter to apply to join, which may be null to test
     * for any existing
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     * @since 1.2
     */
    public final Filter<S> orExists(String propertyName, Filter<?> subFilter) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return or(ExistsFilter.build(prop, subFilter, false));
    }

    /**
     * Returns a combined filter instance that accepts records which are
     * accepted either by this filter or the "not exists" test applied to a
     * join.
     *
     * @param propertyName one-to-many join property name, which may be a chained property
     * @param subFilter sub-filter to apply to join, which may be null to test
     * for any not existing
     * @return canonical Filter instance
     * @throws IllegalArgumentException if property is not found
     * @since 1.2
     */
    public final Filter<S> orNotExists(String propertyName, Filter<?> subFilter) {
        ChainedProperty<S> prop = new FilterParser<S>(mType, propertyName).parseChainedProperty();
        return or(ExistsFilter.build(prop, subFilter, true));
    }

    /**
     * Returns the logical negation of this filter.
     *
     * @return canonical Filter instance
     */
    public abstract Filter<S> not();

    /**
     * Returns an equivalent filter that is in disjunctive normal form. In this
     * form, all logical 'and' operations are performed before all logical 'or'
     * operations. This method often returns a filter with more terms than
     * before.
     *
     * <p>The tree is also normalized such that all terms in a common logical
     * operation are ordered left to right. For example, expressions of the
     * form {@code "(a = ? & b = ?) & (c = ? & d = ?)"} are converted to
     * {@code "(((a = ?) & (b = ?)) & c = ?) & d = ?"}.
     *
     * <p>Although the disjunctive normal filter may have more terms, it can be
     * used to extract values from a FilterValues instance created from this
     * filter. This works because the disjunctive normal filter is composed of
     * the same set of PropertyFilter instances.
     *
     * @return canonical Filter instance
     */
    public final Filter<S> disjunctiveNormalForm() {
        return bind().dnf();
    }

    /**
     * Splits the filter from its disjunctive normal form. Or'ng the filters
     * together produces the full disjunctive normal form.
     *
     * @return unmodifiable list of sub filters which don't perform any 'or'
     * operations
     * @since 1.1.1
     */
    public List<Filter<S>> disjunctiveNormalFormSplit() {
        final List<Filter<S>> list = new ArrayList<Filter<S>>();

        disjunctiveNormalForm().accept(new Visitor<S, Object, Object>() {
            @Override
            public Object visit(AndFilter<S> filter, Object param) {
                list.add(filter);
                return null;
            }

            @Override
            public Object visit(PropertyFilter<S> filter, Object param) {
                list.add(filter);
                return null;
            }

            @Override
            public Object visit(ExistsFilter<S> filter, Object param) {
                list.add(filter);
                return null;
            }
        }, null);

        return Collections.unmodifiableList(list);
    }

    final Filter<S> dnf() {
        Filter<S> filter = this;
        if (!filter.isDisjunctiveNormalForm()) {
            filter = filter.buildDisjunctiveNormalForm();
        }
        return filter.reduce();
    }

    /**
     * Returns an equivalent filter that is in conjunctive normal form. In this
     * form, all logical 'or' operations are performed before all logical 'and'
     * operations. This method often returns a filter with more terms than
     * before.
     *
     * <p>The tree is also normalized such that all terms in a common logical
     * operation are ordered left to right. For example, expressions of the
     * form {@code "(a = ? | b = ?) | (c = ? | d = ?)"} are converted to
     * {@code "(((a = ?) | (b = ?)) | c = ?) | d = ?"}.
     *
     * <p>Although the conjunctive normal filter may have more terms, it can be
     * used to extract values from a FilterValues instance created from this
     * filter. This works because the conjunctive normal filter is composed of
     * the same set of PropertyFilter instances.
     *
     * @return canonical Filter instance
     */
    public final Filter<S> conjunctiveNormalForm() {
        return bind().cnf();
    }

    /**
     * Splits the filter from its conjunctive normal form. And'ng the filters
     * together produces the full conjunctive normal form.
     *
     * @return unmodifiable list of sub filters which don't perform any 'and'
     * operations
     * @since 1.1.1
     */
    public List<Filter<S>> conjunctiveNormalFormSplit() {
        final List<Filter<S>> list = new ArrayList<Filter<S>>();

        conjunctiveNormalForm().accept(new Visitor<S, Object, Object>() {
            @Override
            public Object visit(OrFilter<S> filter, Object param) {
                list.add(filter);
                return null;
            }

            @Override
            public Object visit(PropertyFilter<S> filter, Object param) {
                list.add(filter);
                return null;
            }

            @Override
            public Object visit(ExistsFilter<S> filter, Object param) {
                list.add(filter);
                return null;
            }
        }, null);

        return Collections.unmodifiableList(list);
    }

    final Filter<S> cnf() {
        Filter<S> filter = this;
        if (!filter.isConjunctiveNormalForm()) {
            filter = filter.buildConjunctiveNormalForm();
        }
        return filter.reduce();
    }

    /**
     * Accept the given visitor subclass to traverse the filter tree.
     *
     * @param visitor visitor to traverse through the tree
     * @param param generic input parameter passed to visit methods
     * @return generic return value passed from visit methods
     */
    public abstract <R, P> R accept(Visitor<S, R, P> visitor, P param);

    /**
     * Walks through each property filter, assigning a bind ID to it. This step
     * is automatically performed for proper dnf/cnf conversion, and for
     * building FilterValues.
     *
     * @return canonical Filter instance with bound property filters
     */
    public abstract Filter<S> bind();

    /**
     * Undoes the effect of a bind operation. The returned filter might still
     * report itself as bound if it doesn't make a distinction between these
     * states.
     *
     * @return canonical Filter instance with unbound property filters
     */
    public abstract Filter<S> unbind();

    /**
     * Returns true if all property filters are known to be properly
     * bound. This is a side effect of calling {@link #bind}, {@link
     * #initialFilterValues}, {@link #disjunctiveNormalForm} or {@link
     * #conjunctiveNormalForm}.
     */
    public abstract boolean isBound();

    /**
     * Identify this filter node as bound. Should only be called by Binder.
     */
    abstract void markBound();

    /**
     * Returns an equivalent filter with redundant terms eliminated. The tree
     * is also normalized such that all terms in a common logical operation are
     * ordered left to right. For example, expressions of the form
     * {@code "(a = ? & b = ?) & (c = ? & d = ?)"} are converted to
     * {@code "(((a = ?) & (b = ?)) & c = ?)  & d = ?"}.
     *
     * @return canonical Filter instance
     */
    public final Filter<S> reduce() {
        return isReduced() ? this : accept(new Reducer<S>(), null);
    }

    /**
     * Prepends a join property to all properties of this filter. For example,
     * consider two Storable types, Person and Address. Person has a property
     * "homeAddress" which joins to Address. An Address filter, "city = ?", as
     * joined from Person's "homeAddress", becomes "homeAddress.city = ?".
     *
     * <pre>
     * Filter&lt;Address&gt; addressFilter = Filter.filterFor(Address.class, "city = ?");
     * Filter&lt;Person&gt; personFilter = addressFilter.asJoinedFrom(Person.class, "homeAddress");
     *
     * // Equivalent filter:
     * Filter&lt;Person&gt; personFilter2 = Filter.filterFor(Person.class, "homeAddress.city = ?");
     * </pre>
     *
     * @param type type of T which contains join property
     * @param joinProperty property of T which joins to this Filter's Storable type
     * @return filter for type T
     * @throws IllegalArgumentException if property does not exist or is not a
     * join to type S
     */
    public final <T extends Storable> Filter<T> asJoinedFrom(Class<T> type, String joinProperty) {
        return asJoinedFrom
            (ChainedProperty.parse(StorableIntrospector.examine(type), joinProperty));
    }

    /**
     * Prepends a join property to all properties of this filter. For example,
     * consider two Storable types, Person and Address. Person has a property
     * "homeAddress" which joins to Address. An Address filter, "city = ?", as
     * joined from Person's "homeAddress", becomes "homeAddress.city = ?".
     *
     * @param joinProperty property of T which joins to this Filter's Storable type
     * @return filter for type T
     * @throws IllegalArgumentException if property is not a join to type S
     */
    public final <T extends Storable> Filter<T> asJoinedFrom(ChainedProperty<T> joinProperty) {
        if (joinProperty.getType() != getStorableType()) {
            throw new IllegalArgumentException
                ("Property is not of type \"" + getStorableType().getName() + "\": " +
                 joinProperty);
        }
        return asJoinedFromAny(joinProperty);
    }

    /**
     * Allows join from any property type, including one-to-many joins.
     */
    public abstract <T extends Storable> Filter<T>
        asJoinedFromAny(ChainedProperty<T> joinProperty);

    /**
     * Removes a join property prefix from all applicable properties of this
     * filter. For example, consider two Storable types, Person and
     * Address. Person has a property "homeAddress" which joins to Address. A
     * Person filter might be "homeAddress.city = ? & lastName = ?". When not
     * joined from "homeAddress", it becomes "city = ?" on Address with a
     * remainder of "lastName = ?" on Person.
     *
     * <p>The resulting remainder filter (if any) is always logically and'd to
     * the not joined filter. In order to achieve this, the original filter is
     * first converted to conjunctive normal form. And as a side affect, both
     * the remainder and not joined filters are {@link #bind bound}.
     *
     * @param joinProperty property to not join from
     * @return not join result
     * @throws IllegalArgumentException if property does not exist or if
     * property does not refer to a Storable
     */
    public final NotJoined notJoinedFrom(String joinProperty) {
        return notJoinedFrom
            (ChainedProperty.parse(StorableIntrospector.examine(mType), joinProperty));
    }

    /**
     * Removes a join property prefix from all applicable properties of this
     * filter. For example, consider two Storable types, Person and
     * Address. Person has a property "homeAddress" which joins to Address. A
     * Person filter might be "homeAddress.city = ? & lastName = ?". When not
     * joined from "homeAddress", it becomes "city = ?" on Address with a
     * remainder of "lastName = ?" on Person.
     *
     * <p>The resulting remainder filter (if any) is always logically and'd to
     * the not joined filter. In order to achieve this, the original filter is
     * first converted to conjunctive normal form. And as a side affect, both
     * the remainder and not joined filters are {@link #bind bound}.
     *
     * @param joinProperty property to not join from
     * @return not join result
     * @throws IllegalArgumentException if property does not refer to a Storable
     */
    public final NotJoined notJoinedFrom(ChainedProperty<S> joinProperty) {
        if (!Storable.class.isAssignableFrom(joinProperty.getType())) {
            throw new IllegalArgumentException
                ("Join property type is not a Storable: " + joinProperty);
        }
        return notJoinedFromAny(joinProperty);
    }

    final NotJoined notJoinedFromAny(ChainedProperty<S> joinProperty) {
        NotJoined nj = conjunctiveNormalForm().notJoinedFromCNF(joinProperty);

        if (nj.getNotJoinedFilter().isOpen()) {
            // Remainder filter should be same as original, but it might have
            // expanded with conjunctive normal form. If so, restore to
            // original, but still bind it to ensure consistent side-effects.
            if (nj.getRemainderFilter() != this) {
                nj = new NotJoined(nj.getNotJoinedFilter(), bind());
            }
        }

        if (isDisjunctiveNormalForm()) {
            // Try to return filters which look similar to the original. The
            // conversion from disjunctive normal form to conjunctive normal
            // form may make major changes. If original was dnf, restore the
            // result filters to dnf.
            
            if (!(nj.getNotJoinedFilter().isDisjunctiveNormalForm()) ||
                !(nj.getRemainderFilter().isDisjunctiveNormalForm()))
            {
                nj = new NotJoined(nj.getNotJoinedFilter().disjunctiveNormalForm(),
                                   nj.getRemainderFilter().disjunctiveNormalForm());
            }
        }

        return nj;
    }

    /**
     * Should only be called on a filter in conjunctive normal form.
     */
    NotJoined notJoinedFromCNF(ChainedProperty<S> joinProperty) {
        return new NotJoined(getOpenFilter(joinProperty.getLastProperty().getJoinedType()), this);
    }

    /**
     * Returns true if filter allows all results to pass through.
     *
     * @since 1.2
     */
    public boolean isOpen() {
        return false;
    }

    /**
     * Returns true if filter prevents any results from passing through.
     *
     * @since 1.2
     */
    public boolean isClosed() {
        return false;
    }

    abstract Filter<S> buildDisjunctiveNormalForm();

    abstract Filter<S> buildConjunctiveNormalForm();

    abstract boolean isDisjunctiveNormalForm();

    abstract boolean isConjunctiveNormalForm();

    /**
     * Returns true if filter node is in a reduced form.
     */
    abstract boolean isReduced();

    /**
     * Identify this filter node as reduced. Should only be called by Reducer.
     */
    abstract void markReduced();

    @Override
    public final int hashCode() {
        int hashCode = mHashCode;
        if (hashCode == 0) {
            mHashCode = hashCode = generateHashCode();
        }
        return hashCode;
    }

    abstract int generateHashCode();

    @Override
    public abstract boolean equals(Object obj);

    /**
     * Returns the string value of this filter, which is also parsable.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        try {
            appendTo(buf);
        } catch (IOException e) {
            // Not gonna happen.
        }
        return buf.toString();
    }

    /**
     * Appends the string value of this filter into the given Appendable.
     */
    public void appendTo(Appendable app) throws IOException {
        appendTo(app, null);
    }

    /**
     * Appends the string value of this filter into the given Appendable.
     *
     * @param values optionally supply filter values
     */
    public abstract void appendTo(Appendable app, FilterValues<S> values)
        throws IOException;

    // Package-private in order to be inherited by subclasses.
    Object readResolve() {
        return cCanonical.put(this);
    }

    /**
     * Result from calling {@link Filter#notJoinedFrom}.
     */
    public class NotJoined {
        private final Filter<?> mNotJoined;
        private final Filter<S> mRemainder;

        NotJoined(Filter<?> notJoined, Filter<S> remainder) {
            mNotJoined = notJoined;
            mRemainder = remainder;
        }

        /**
         * Returns the filter which is no longer as from a join.
         *
         * @return not joined filter or open filter if none
         */
        public Filter<?> getNotJoinedFilter() {
            return mNotJoined;
        }

        /**
         * Returns the filter which could not be separated.
         *
         * @return remainder filter or open filter if none
         */
        public Filter<S> getRemainderFilter() {
            return mRemainder;
        }

        @Override
        public int hashCode() {
            return mNotJoined.hashCode() * 31 + mRemainder.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Filter.NotJoined) {
                NotJoined other = (NotJoined) obj;
                return mNotJoined.equals(other.mNotJoined) && mRemainder.equals(other.mRemainder);
            }
            return false;
        }

        @Override
        public String toString() {
            return "not joined: " + mNotJoined + ", remainder: " + mRemainder;
        }
    }
}
