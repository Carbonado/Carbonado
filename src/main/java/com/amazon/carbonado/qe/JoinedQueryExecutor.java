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

import java.io.IOException;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.cojen.classfile.ClassFile;
import org.cojen.classfile.CodeBuilder;
import org.cojen.classfile.LocalVariable;
import org.cojen.classfile.MethodInfo;
import org.cojen.classfile.Modifiers;
import org.cojen.classfile.TypeDesc;

import org.cojen.util.ClassInjector;
import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.cursor.MultiTransformedCursor;

import com.amazon.carbonado.filter.Filter;
import com.amazon.carbonado.filter.FilterValues;
import com.amazon.carbonado.filter.RelOp;

import com.amazon.carbonado.info.ChainedProperty;
import com.amazon.carbonado.info.OrderedProperty;
import com.amazon.carbonado.info.StorableIndex;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.util.QuickConstructorGenerator;

import com.amazon.carbonado.gen.CodeBuilderUtil;

/**
 * QueryExecutor which joins a <i>source</i> and <i>target</i> executor,
 * producing results of target type. The source executor is called once per
 * fetch (outer loop), but the target executor is called once per source result
 * (inner loop).
 *
 * @author Brian S O'Neill
 * @param <S> source type
 * @param <T> target type
 */
public class JoinedQueryExecutor<S extends Storable, T extends Storable>
    extends AbstractQueryExecutor<T>
{
    /**
     * Builds and returns a complex joined excutor against a chained property,
     * supporting multi-way joins. Filtering and ordering may also be supplied,
     * in order to better distribute work throughout the join.
     *
     * @param repoAccess used to create query executors for outer and inner loops
     * @param targetToSourceProperty join property of <i>target</i> type which maps
     * to instances of <i>source</i> type
     * @param targetFilter optional filter for fetching <i>target</i> instances
     * @param targetOrdering optional ordering to apply to <i>target</i> executor
     & @param hints optional hints
     * @throws IllegalArgumentException if any parameter is null or if join
     * property is not a Storable type
     * @throws RepositoryException from RepositoryAccess
     */
    public static <T extends Storable> QueryExecutor<T>
        build(RepositoryAccess repoAccess,
              ChainedProperty<T> targetToSourceProperty,
              Filter<T> targetFilter,
              OrderingList<T> targetOrdering,
              QueryHints hints)
        throws RepositoryException
    {
        if (targetOrdering == null) {
            targetOrdering = OrderingList.emptyList();
        }

        QueryExecutor<T> executor =
            buildJoin(repoAccess, targetToSourceProperty, targetFilter, targetOrdering, hints);

        OrderingList<T> handledOrdering = executor.getOrdering();

        // Apply sort if any remaining ordering properties.
        int handledCount = commonOrderingCount(handledOrdering, targetOrdering);

        OrderingList<T> remainderOrdering =
            targetOrdering.subList(handledCount, targetOrdering.size());

        if (remainderOrdering.size() > 0) {
            SortedQueryExecutor.Support<T> support = repoAccess
                .storageAccessFor(targetToSourceProperty.getPrimeProperty().getEnclosingType());
            executor = new SortedQueryExecutor<T>
                (support, executor, handledOrdering, remainderOrdering);
        }

        return executor;
    }

    private static <T extends Storable> JoinedQueryExecutor<?, T>
        buildJoin(RepositoryAccess repoAccess,
                  ChainedProperty<T> targetToSourceProperty,
                  Filter<T> targetFilter,
                  OrderingList<T> targetOrdering,
                  QueryHints hints)
        throws RepositoryException
    {
        StorableProperty<T> primeTarget = targetToSourceProperty.getPrimeProperty();

        Filter tailFilter;
        if (targetFilter == null) {
            tailFilter = null;
        } else {
            Filter<T>.NotJoined nj = targetFilter.notJoinedFrom(ChainedProperty.get(primeTarget));
            tailFilter = nj.getNotJoinedFilter();
            targetFilter = nj.getRemainderFilter();
        }

        // Determine the most ordering properties the source (outer loop
        // executor) can provide. It may use less if its selected index does
        // not provide the ordering for free.
        final OrderingList outerLoopOrdering = mostOrdering(primeTarget, targetOrdering);

        QueryExecutor outerLoopExecutor;
        if (targetToSourceProperty.getChainCount() > 0) {
            ChainedProperty tailProperty = targetToSourceProperty.tail();
            outerLoopExecutor = buildJoin
                (repoAccess, tailProperty, tailFilter, outerLoopOrdering, hints);
        } else {
            Class sourceType = targetToSourceProperty.getType();

            if (!Storable.class.isAssignableFrom(sourceType)) {
                throw new IllegalArgumentException
                    ("Property type is not a Storable: " + targetToSourceProperty);
            }

            StorageAccess sourceAccess = repoAccess.storageAccessFor(sourceType);

            OrderingList expectedOrdering =
                expectedOrdering(sourceAccess, tailFilter, outerLoopOrdering);

            QueryExecutorFactory outerLoopExecutorFactory = sourceAccess.getQueryExecutorFactory();

            outerLoopExecutor = outerLoopExecutorFactory
                .executor(tailFilter, expectedOrdering, hints);
        }

        if (targetOrdering.size() > 0) {
            // If outer loop handles some of the ordering, then it can be
            // removed from the target ordering. This simplifies or eliminates
            // a final sort operation.

            int handledCount =
                commonOrderingCount(outerLoopExecutor.getOrdering(), outerLoopOrdering);

            targetOrdering = targetOrdering.subList(handledCount, targetOrdering.size());
        }

        Class<T> targetType = primeTarget.getEnclosingType();
        StorageAccess<T> targetAccess = repoAccess.storageAccessFor(targetType);
        
        QueryExecutorFactory<T> innerLoopExecutorFactory = targetAccess.getQueryExecutorFactory();

        return new JoinedQueryExecutor<Storable, T>(outerLoopExecutor,
                                                    innerLoopExecutorFactory,
                                                    primeTarget,
                                                    targetFilter,
                                                    targetOrdering,
                                                    targetAccess);
    }

    private static final String INNER_LOOP_EX_FIELD_NAME = "innerLoopExecutor";
    private static final String INNER_LOOP_FV_FIELD_NAME = "innerLoopFilterValues";
    private static final String ACTIVE_SOURCE_FIELD_NAME = "active";

    private static final Map<StorableProperty, Class> cJoinerCursorClassCache;

    static {
        cJoinerCursorClassCache = new SoftValuedHashMap();
    }

    private static synchronized <S, T extends Storable> Joiner.Factory<S, T>
        getJoinerFactory(StorableProperty<T> targetToSourceProperty)
    {
        Class clazz = cJoinerCursorClassCache.get(targetToSourceProperty);

        if (clazz == null) {
            clazz = generateJoinerCursor(targetToSourceProperty);
            cJoinerCursorClassCache.put(targetToSourceProperty, clazz);
        }

        return (Joiner.Factory<S, T>) QuickConstructorGenerator
            .getInstance(clazz, Joiner.Factory.class);
    }

    private static <T extends Storable> Class<Cursor<T>>
        generateJoinerCursor(StorableProperty<T> targetToSourceProperty)
    {
        final Class<?> sourceType = targetToSourceProperty.getType();
        final Class<T> targetType = targetToSourceProperty.getEnclosingType();

        String packageName;
        {
            String name = targetType.getName();
            int index = name.lastIndexOf('.');
            if (index >= 0) {
                packageName = name.substring(0, index);
            } else {
                packageName = "";
            }
        }

        ClassLoader loader = targetType.getClassLoader();

        ClassInjector ci = ClassInjector.create(packageName + ".JoinedCursor", loader);
        ClassFile cf = new ClassFile(ci.getClassName(), MultiTransformedCursor.class);
        cf.markSynthetic();
        cf.setSourceFile(JoinedQueryExecutor.class.getName());
        cf.setTarget("1.5");

        final TypeDesc cursorType = TypeDesc.forClass(Cursor.class);
        final TypeDesc queryExecutorType = TypeDesc.forClass(QueryExecutor.class);
        final TypeDesc filterValuesType = TypeDesc.forClass(FilterValues.class);

        // Define fields for inner loop executor and filter values, which are
        // passed into the constructor.
        cf.addField(Modifiers.PRIVATE.toFinal(true), INNER_LOOP_EX_FIELD_NAME, queryExecutorType);
        cf.addField(Modifiers.PRIVATE.toFinal(true), INNER_LOOP_FV_FIELD_NAME, filterValuesType);

        // If target storable can set a reference to the joined source
        // storable, then stash a copy of it as we go. This way, when user of
        // target storable accesses the joined property, it costs nothing.
        boolean canSetSourceReference = targetToSourceProperty.getWriteMethod() != null;

        if (canSetSourceReference) {
            // Field to hold active source storable.
            cf.addField(Modifiers.PRIVATE, ACTIVE_SOURCE_FIELD_NAME,
                        TypeDesc.forClass(sourceType));
        }

        // Define constructor.
        {
            TypeDesc[] params = {cursorType, queryExecutorType, filterValuesType};

            MethodInfo mi = cf.addConstructor(Modifiers.PUBLIC, params);
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.loadLocal(b.getParameter(0)); // pass source cursor to superclass
            b.invokeSuperConstructor(new TypeDesc[] {cursorType});

            b.loadThis();
            b.loadLocal(b.getParameter(1));
            b.storeField(INNER_LOOP_EX_FIELD_NAME, queryExecutorType);

            b.loadThis();
            b.loadLocal(b.getParameter(2));
            b.storeField(INNER_LOOP_FV_FIELD_NAME, filterValuesType);

            b.returnVoid();
        }

        // Implement the transform method.
        {
            MethodInfo mi = cf.addMethod(Modifiers.PROTECTED, "transform", cursorType,
                                         new TypeDesc[] {TypeDesc.OBJECT});
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            LocalVariable sourceVar = b.createLocalVariable(null, TypeDesc.forClass(sourceType));
            b.loadLocal(b.getParameter(0));
            b.checkCast(TypeDesc.forClass(sourceType));
            b.storeLocal(sourceVar);

            if (canSetSourceReference) {
                b.loadThis();
                b.loadLocal(sourceVar);
                b.storeField(ACTIVE_SOURCE_FIELD_NAME, TypeDesc.forClass(sourceType));
            }

            // Prepare to call fetch on innerLoopExecutor.
            b.loadThis();
            b.loadField(INNER_LOOP_EX_FIELD_NAME, queryExecutorType);

            // Fill in values for innerLoopFilterValues.
            b.loadThis();
            b.loadField(INNER_LOOP_FV_FIELD_NAME, filterValuesType);

            int propCount = targetToSourceProperty.getJoinElementCount();
            for (int i=0; i<propCount; i++) {
                StorableProperty<?> external = targetToSourceProperty.getExternalJoinElement(i);
                b.loadLocal(sourceVar);
                b.invoke(external.getReadMethod());

                TypeDesc bindType = CodeBuilderUtil.bindQueryParam(external.getType());
                CodeBuilderUtil.convertValue(b, external.getType(), bindType.toClass());
                b.invokeVirtual(filterValuesType, "with", filterValuesType,
                                new TypeDesc[] {bindType});
            }

            // Now fetch and return.
            b.invokeInterface(queryExecutorType, "fetch", cursorType,
                              new TypeDesc[] {filterValuesType});
            b.returnValue(cursorType);
        }

        if (canSetSourceReference) {
            // Override the "next" method to set source object on target.
            MethodInfo mi = cf.addMethod(Modifiers.PUBLIC, "next", TypeDesc.OBJECT, null);
            mi.addException(TypeDesc.forClass(FetchException.class));
            CodeBuilder b = new CodeBuilder(mi);

            b.loadThis();
            b.invokeSuper(TypeDesc.forClass(MultiTransformedCursor.class),
                          "next", TypeDesc.OBJECT, null);
            b.checkCast(TypeDesc.forClass(targetType));
            b.dup();

            b.loadThis();
            b.loadField(ACTIVE_SOURCE_FIELD_NAME, TypeDesc.forClass(sourceType));
            b.invoke(targetToSourceProperty.getWriteMethod());

            b.returnValue(TypeDesc.OBJECT);
        }

        return (Class<Cursor<T>>) ci.defineClass(cf);
    }

    private static <S extends Storable, T extends Storable> OrderingList<T>
        transformOrdering(Class<T> targetType,
                          String targetToSourceProperty,
                          QueryExecutor<S> sourceExecutor)
    {
        OrderingList<T> targetOrdering = OrderingList.emptyList();
        StorableInfo<T> targetInfo = StorableIntrospector.examine(targetType);

        for (OrderedProperty<S> sourceProp : sourceExecutor.getOrdering()) {
            String targetName = targetToSourceProperty + '.' + sourceProp.getChainedProperty();
            OrderedProperty<T> targetProp = OrderedProperty
                .get(ChainedProperty.parse(targetInfo, targetName), sourceProp.getDirection());
            targetOrdering = targetOrdering.concat(targetProp);
        }

        return targetOrdering;
    }

    /**
     * Given a list of chained ordering properties, returns the properties
     * stripped of the matching chain prefix for the targetToSourceProperty. As
     * the target ordering is scanned left-to-right, if any property is found
     * which doesn't match the targetToSourceProperty, the building of the new
     * list stops. In other words, it returns a consecutive run of matching
     * properties.
     */
    private static <T extends Storable> OrderingList
        mostOrdering(StorableProperty<T> primeTarget, OrderingList<T> targetOrdering)
    {
        OrderingList handledOrdering = OrderingList.emptyList();
        for (OrderedProperty<T> targetProp : targetOrdering) {
            ChainedProperty<T> chainedProp = targetProp.getChainedProperty();
            if (chainedProp.getPrimeProperty().equals(primeTarget)) {
                handledOrdering = handledOrdering
                    // I hate Java generics. Note the stupid cast. I have finally
                    // realized the core problem: the wildcard model is broken.
                    .concat(OrderedProperty
                            .get((ChainedProperty) chainedProp.tail(),
                                 targetProp.getDirection()));
            } else {
                break;
            }
        }

        return handledOrdering;
    }

    /**
     * Examines the given ordering against available indexes, returning the
     * ordering that the best index can provide for free.
     */
    private static <T extends Storable> OrderingList<T>
        expectedOrdering(StorageAccess<T> access, Filter<T> filter, OrderingList<T> ordering)
    {
        List<Filter<T>> split;
        if (filter == null) {
            split = Filter.getOpenFilter(access.getStorableType()).disjunctiveNormalFormSplit();
        } else {
            split = filter.disjunctiveNormalFormSplit();
        }

        Comparator comparator = CompositeScore.fullComparator();

        CompositeScore bestScore = null;
        for (StorableIndex<T> index : access.getAllIndexes()) {
            for (Filter<T> sub : split) {
                CompositeScore candidateScore = CompositeScore.evaluate(index, sub, ordering);
                if (bestScore == null || comparator.compare(candidateScore, bestScore) < 0) {
                    bestScore = candidateScore;
                }
            }
        }

        // Reduce source ordering to that which can be handled for
        // free. Otherwise, a sort would be performed which is a waste of time
        // if some source results will later be filtered out.
        int handledCount = bestScore == null ? 0 : bestScore.getOrderingScore().getHandledCount();
        return ordering.subList(0, handledCount);
    }

    /**
     * Returns the count of exactly matching properties from the two
     * orderings. The match must be consecutive and start at the first
     * property.
     */
    private static <T extends Storable> int
        commonOrderingCount(OrderingList<T> orderingA, OrderingList<T> orderingB)
    {
        int commonCount = Math.min(orderingA.size(), orderingB.size());

        for (int i=0; i<commonCount; i++) {
            if (!orderingA.get(i).equals(orderingB.get(i))) {
                return i;
            }
        }

        return commonCount;
    }

    private final Filter<T> mTargetFilter;
    private final StorableProperty<T> mTargetToSourceProperty;

    private final QueryExecutor<S> mOuterLoopExecutor;
    private final FilterValues<S> mOuterLoopFilterValues;

    private final QueryExecutor<T> mInnerLoopExecutor;
    private final FilterValues<T> mInnerLoopFilterValues;

    private final Filter<T> mSourceFilterAsFromTarget;
    private final Filter<T> mCombinedFilter;
    private final OrderingList<T> mCombinedOrdering;

    private final Joiner.Factory<S, T> mJoinerFactory;

    /**
     * @param outerLoopExecutor executor for <i>source</i> instances
     * @param innerLoopExecutorFactory used to construct inner loop executor
     * @param targetToSourceProperty join property of <i>target</i> type which maps
     * to instances of <i>source</i> type
     * @param targetFilter optional initial filter for fetching <i>target</i> instances
     * @param targetOrdering optional desired ordering to apply to
     * <i>target</i> executor
     * @param targetAccess used with target ordering to determine actual
     * ordering which an index provides for free
     * @throws IllegalArgumentException if any parameter is null or if join
     * property is not of <i>source</i> type
     * @throws RepositoryException from innerLoopExecutorFactory
     */
    private JoinedQueryExecutor(QueryExecutor<S> outerLoopExecutor,
                                QueryExecutorFactory<T> innerLoopExecutorFactory,
                                StorableProperty<T> targetToSourceProperty,
                                Filter<T> targetFilter,
                                OrderingList<T> targetOrdering,
                                StorageAccess<T> targetAccess)
        throws RepositoryException
    {
        if (targetToSourceProperty == null || outerLoopExecutor == null) {
            throw new IllegalArgumentException("Null parameter");
        }

        Class<S> sourceType = outerLoopExecutor.getStorableType();
        if (targetToSourceProperty.getType() != sourceType) {
            throw new IllegalArgumentException
                ("Property is not of type \"" + sourceType.getName() + "\": " +
                 targetToSourceProperty);
        }

        if (!targetToSourceProperty.isJoin()) {
            throw new IllegalArgumentException
                ("Property is not a join: " + targetToSourceProperty);
        }

        if (targetFilter != null && !targetFilter.isBound()) {
            throw new IllegalArgumentException("Target filter must be bound");
        }

        if (!outerLoopExecutor.getFilter().isBound()) {
            throw new IllegalArgumentException("Outer loop executor filter must be bound");
        }

        if (targetFilter.isOpen()) {
            targetFilter = null;
        }

        mTargetFilter = targetFilter;
        mTargetToSourceProperty = targetToSourceProperty;
        mOuterLoopExecutor = outerLoopExecutor;
        mOuterLoopFilterValues = outerLoopExecutor.getFilter().initialFilterValues();

        Class<T> targetType = targetToSourceProperty.getEnclosingType();

        // Prepare inner loop filter which is and'd by the join property elements.
        Filter<T> innerLoopExecutorFilter = Filter.getOpenFilter(targetType);
        if (targetFilter != null) {
            innerLoopExecutorFilter = innerLoopExecutorFilter.and(targetFilter);
        }
        int count = targetToSourceProperty.getJoinElementCount();
        for (int i=0; i<count; i++) {
            innerLoopExecutorFilter = innerLoopExecutorFilter
                .and(targetToSourceProperty.getInternalJoinElement(i).getName(), RelOp.EQ);
        }
        innerLoopExecutorFilter = innerLoopExecutorFilter.bind();

        mInnerLoopFilterValues = innerLoopExecutorFilter.initialFilterValues();

        // Only perform requested ordering if inner loop index provides it for
        // free. This optimization is only valid if outer loop matches at most
        // one record.
        if (targetOrdering != null) {
            if (outerLoopExecutor instanceof KeyQueryExecutor) {
                targetOrdering =
                    expectedOrdering(targetAccess, innerLoopExecutorFilter, targetOrdering);
            } else {
                targetOrdering = null;
            }
        }

        mInnerLoopExecutor = innerLoopExecutorFactory
            .executor(innerLoopExecutorFilter, targetOrdering, null);

        Filter<T> filter = outerLoopExecutor.getFilter()
            .asJoinedFrom(ChainedProperty.get(targetToSourceProperty));

        mSourceFilterAsFromTarget = filter;

        if (targetFilter != null) {
            filter = filter.and(targetFilter);
        }

        mCombinedFilter = filter;

        // Prepare combined ordering.
        OrderingList<T> ordering = transformOrdering
            (targetType, targetToSourceProperty.getName(), outerLoopExecutor);

        if (targetOrdering != null) {
            ordering = ordering.concat(targetOrdering);
        }

        mCombinedOrdering = ordering;

        mJoinerFactory = getJoinerFactory(targetToSourceProperty);
    }

    public Cursor<T> fetch(FilterValues<T> values) throws FetchException {
        FilterValues<T> innerLoopFilterValues = mInnerLoopFilterValues;

        if (mTargetFilter != null) {
            // Prepare this before opening source cursor, in case an exception is thrown.
            innerLoopFilterValues = innerLoopFilterValues
                .withValues(values.getValuesFor(mTargetFilter));
        }

        Cursor<S> outerLoopCursor = mOuterLoopExecutor.fetch(transferValues(values));

        return mJoinerFactory.newJoinedCursor
            (outerLoopCursor, mInnerLoopExecutor, innerLoopFilterValues);
    }

    public Filter<T> getFilter() {
        return mCombinedFilter;
    }

    public OrderingList<T> getOrdering() {
        return mCombinedOrdering;
    }

    public boolean printPlan(Appendable app, int indentLevel, FilterValues<T> values)
        throws IOException
    {
        indent(app, indentLevel);
        app.append("join: ");
        app.append(mTargetToSourceProperty.getEnclosingType().getName());
        newline(app);
        indent(app, indentLevel);
        app.append("...inner loop: ");
        app.append(mTargetToSourceProperty.getName());
        newline(app);
        mInnerLoopExecutor.printPlan(app, increaseIndent(indentLevel), values);
        indent(app, indentLevel);
        app.append("...outer loop");
        newline(app);
        mOuterLoopExecutor.printPlan(app, increaseIndent(indentLevel), transferValues(values));
        return true;
    }

    private FilterValues<S> transferValues(FilterValues<T> values) {
        if (values == null) {
            return null;
        }
        return mOuterLoopFilterValues
            .withValues(values.getSuppliedValuesFor(mSourceFilterAsFromTarget));
    }

    @SuppressWarnings("unused")
    private static interface Joiner {
        /**
         * Needs to be public for {@link QuickConstructorGenerator}, but hide
         * it inside a private interface.
         */
        public static interface Factory<S, T extends Storable> {
            Cursor<T> newJoinedCursor(Cursor<S> outerLoopCursor,
                                      QueryExecutor<T> innerLoopExecutor,
                                      FilterValues<T> innerLoopFilterValues);
        }
    }
}
