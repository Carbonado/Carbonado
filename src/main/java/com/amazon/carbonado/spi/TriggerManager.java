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

package com.amazon.carbonado.spi;

import java.lang.reflect.Method;

import java.util.ArrayList;
import java.util.Arrays;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.Clob;

/**
 * Used by Storage implementations to manage triggers and consolidate them into
 * single logical triggers. This class is thread-safe and ensures that changes
 * to the trigger set do not affect transactions in progress.
 *
 * @author Brian S O'Neill
 */
public class TriggerManager<S extends Storable> extends Trigger<S> {
    // Bit masks returned by selectTypes.
    private static final int FOR_INSERT = 1;
    private static final int FOR_UPDATE = 2;
    private static final int FOR_DELETE = 4;
    private static final int FOR_ADAPT_LOB = 8;

    private static final Method
        BEFORE_INSERT_METHOD,
        BEFORE_TRY_INSERT_METHOD,
        AFTER_INSERT_METHOD,
        AFTER_TRY_INSERT_METHOD,
        FAILED_INSERT_METHOD,

        BEFORE_UPDATE_METHOD,
        BEFORE_TRY_UPDATE_METHOD,
        AFTER_UPDATE_METHOD,
        AFTER_TRY_UPDATE_METHOD,
        FAILED_UPDATE_METHOD,

        BEFORE_DELETE_METHOD,
        BEFORE_TRY_DELETE_METHOD,
        AFTER_DELETE_METHOD,
        AFTER_TRY_DELETE_METHOD,
        FAILED_DELETE_METHOD,

        ADAPT_BLOB_METHOD,
        ADAPT_CLOB_METHOD;

    static {
        Class<?> triggerClass = Trigger.class;
        Class[] ONE_PARAM = {Object.class};
        Class[] TWO_PARAMS = {Object.class, Object.class};

        try {
            BEFORE_INSERT_METHOD     = triggerClass.getMethod("beforeInsert", ONE_PARAM);
            BEFORE_TRY_INSERT_METHOD = triggerClass.getMethod("beforeTryInsert", ONE_PARAM);
            AFTER_INSERT_METHOD      = triggerClass.getMethod("afterInsert", TWO_PARAMS);
            AFTER_TRY_INSERT_METHOD  = triggerClass.getMethod("afterTryInsert", TWO_PARAMS);
            FAILED_INSERT_METHOD     = triggerClass.getMethod("failedInsert", TWO_PARAMS);

            BEFORE_UPDATE_METHOD     = triggerClass.getMethod("beforeUpdate", ONE_PARAM);
            BEFORE_TRY_UPDATE_METHOD = triggerClass.getMethod("beforeTryUpdate", ONE_PARAM);
            AFTER_UPDATE_METHOD      = triggerClass.getMethod("afterUpdate", TWO_PARAMS);
            AFTER_TRY_UPDATE_METHOD  = triggerClass.getMethod("afterTryUpdate", TWO_PARAMS);
            FAILED_UPDATE_METHOD     = triggerClass.getMethod("failedUpdate", TWO_PARAMS);

            BEFORE_DELETE_METHOD     = triggerClass.getMethod("beforeDelete", ONE_PARAM);
            BEFORE_TRY_DELETE_METHOD = triggerClass.getMethod("beforeTryDelete", ONE_PARAM);
            AFTER_DELETE_METHOD      = triggerClass.getMethod("afterDelete", TWO_PARAMS);
            AFTER_TRY_DELETE_METHOD  = triggerClass.getMethod("afterTryDelete", TWO_PARAMS);
            FAILED_DELETE_METHOD     = triggerClass.getMethod("failedDelete", TWO_PARAMS);

            ADAPT_BLOB_METHOD = triggerClass
                .getMethod("adaptBlob", Object.class, String.class, Blob.class);
            ADAPT_CLOB_METHOD = triggerClass
                .getMethod("adaptClob", Object.class, String.class, Clob.class);
        } catch (NoSuchMethodException e) {
            Error error = new NoSuchMethodError();
            error.initCause(e);
            throw error;
        }
    }

    private final ForInsert<S> mForInsert = new ForInsert<S>();
    private final ForUpdate<S> mForUpdate = new ForUpdate<S>();
    private final ForDelete<S> mForDelete = new ForDelete<S>();
    private final ForAdaptLob<S> mForAdaptLob = new ForAdaptLob<S>();

    public TriggerManager() {
    }

    /**
     * @param triggerFactories TriggerFactories which will be called upon to
     * optionally return a trigger to initially register
     */
    public TriggerManager(Class<S> type, Iterable<TriggerFactory> triggerFactories)
        throws RepositoryException
    {
        if (triggerFactories != null) {
            addTriggers(type, triggerFactories);
        }
    }

    /**
     * Returns a consolidated trigger to call for insert operations, or null if
     * none. If not null, the consolidated trigger is not a snapshot -- it will
     * change as the set of triggers in this manager changes.
     */
    public Trigger<? super S> getInsertTrigger() {
        ForInsert<S> forInsert = mForInsert;
        return forInsert.isEmpty() ? null : forInsert;
    }

    /**
     * Returns a consolidated trigger to call for update operations, or null if
     * none. If not null, the consolidated trigger is not a snapshot -- it will
     * change as the set of triggers in this manager changes.
     */
    public Trigger<? super S> getUpdateTrigger() {
        ForUpdate<S> forUpdate = mForUpdate;
        return forUpdate.isEmpty() ? null : forUpdate;
    }

    /**
     * Returns a consolidated trigger to call for delete operations, or null if
     * none. If not null, the consolidated trigger is not a snapshot -- it will
     * change as the set of triggers in this manager changes.
     */
    public Trigger<? super S> getDeleteTrigger() {
        ForDelete<S> forDelete = mForDelete;
        return forDelete.isEmpty() ? null : forDelete;
    }

    /**
     * Returns a consolidated trigger to call for adapt LOB operations, or null
     * if none. If not null, the consolidated trigger is not a snapshot -- it
     * will change as the set of triggers in this manager changes.
     *
     * @since 1.2
     */
    public Trigger<? super S> getAdaptLobTrigger() {
        ForAdaptLob<S> forAdaptLob = mForAdaptLob;
        return forAdaptLob.isEmpty() ? null : forAdaptLob;
    }

    public boolean addTrigger(Trigger<? super S> trigger) {
        if (trigger == null) {
            throw new IllegalArgumentException();
        }

        int types = selectTypes(trigger);

        boolean retValue = false;

        if ((types & FOR_INSERT) != 0) {
            retValue |= mForInsert.add(trigger);
        }
        if ((types & FOR_UPDATE) != 0) {
            retValue |= mForUpdate.add(trigger);
        }
        if ((types & FOR_DELETE) != 0) {
            retValue |= mForDelete.add(trigger);
        }
        if ((types & FOR_ADAPT_LOB) != 0) {
            retValue |= mForAdaptLob.add(trigger);
        }

        return retValue;
    }

    public boolean removeTrigger(Trigger<? super S> trigger) {
        if (trigger == null) {
            throw new IllegalArgumentException();
        }

        int types = selectTypes(trigger);

        boolean retValue = false;

        if ((types & FOR_INSERT) != 0) {
            retValue |= mForInsert.remove(trigger);
        }
        if ((types & FOR_UPDATE) != 0) {
            retValue |= mForUpdate.remove(trigger);
        }
        if ((types & FOR_DELETE) != 0) {
            retValue |= mForDelete.remove(trigger);
        }
        if ((types & FOR_ADAPT_LOB) != 0) {
            retValue |= mForAdaptLob.remove(trigger);
        }

        return retValue;
    }

    public void addTriggers(Class<S> type, Iterable<TriggerFactory> triggerFactories)
        throws RepositoryException
    {
        for (TriggerFactory factory : triggerFactories) {
            Trigger<? super S> trigger = factory.triggerFor(type);
            if (trigger != null) {
                addTrigger(trigger);
            }
        }
    }

    /**
     * Disables execution of all managed triggers for the current thread. Call
     * localEnable to enable again. This call can be made multiple times, but
     * be sure to call localEnable the same number of times to fully enable.
     */
    public void localDisable() {
        mForInsert.localDisable();
        mForUpdate.localDisable();
        mForDelete.localDisable();
        mForAdaptLob.localDisable();
    }

    /**
     * Enables execution of all managed triggers for the current thread, if
     * they had been disabled before.
     */
    public void localEnable() {
        mForInsert.localEnable();
        mForUpdate.localEnable();
        mForDelete.localEnable();
        mForAdaptLob.localEnable();
    }

    @Override
    public Object beforeInsert(S storable) throws PersistException {
        return mForInsert.beforeInsert(storable);
    }

    @Override
    public Object beforeTryInsert(S storable) throws PersistException {
        return mForInsert.beforeTryInsert(storable);
    }

    @Override
    public void afterInsert(S storable, Object state) throws PersistException {
        mForInsert.afterInsert(storable, state);
    }

    @Override
    public void afterTryInsert(S storable, Object state) throws PersistException {
        mForInsert.afterTryInsert(storable, state);
    }

    @Override
    public void failedInsert(S storable, Object state) {
        mForInsert.failedInsert(storable, state);
    }

    @Override
    public Object beforeUpdate(S storable) throws PersistException {
        return mForUpdate.beforeUpdate(storable);
    }

    @Override
    public Object beforeTryUpdate(S storable) throws PersistException {
        return mForUpdate.beforeTryUpdate(storable);
    }

    @Override
    public void afterUpdate(S storable, Object state) throws PersistException {
        mForUpdate.afterUpdate(storable, state);
    }

    @Override
    public void afterTryUpdate(S storable, Object state) throws PersistException {
        mForUpdate.afterTryUpdate(storable, state);
    }

    @Override
    public void failedUpdate(S storable, Object state) {
        mForUpdate.failedUpdate(storable, state);
    }

    @Override
    public Object beforeDelete(S storable) throws PersistException {
        return mForDelete.beforeDelete(storable);
    }

    @Override
    public Object beforeTryDelete(S storable) throws PersistException {
        return mForDelete.beforeTryDelete(storable);
    }

    @Override
    public void afterDelete(S storable, Object state) throws PersistException {
        mForDelete.afterDelete(storable, state);
    }

    @Override
    public void afterTryDelete(S storable, Object state) throws PersistException {
        mForDelete.afterTryDelete(storable, state);
    }

    @Override
    public void failedDelete(S storable, Object state) {
        mForDelete.failedDelete(storable, state);
    }

    @Override
    public Blob adaptBlob(S storable, String name, Blob blob) {
        return mForAdaptLob.adaptBlob(storable, name, blob);
    }

    @Override
    public Clob adaptClob(S storable, String name, Clob clob) {
        return mForAdaptLob.adaptClob(storable, name, clob);
    }

    /**
     * Determines which operations the given trigger overrides.
     */
    private int selectTypes(Trigger<? super S> trigger) {
        Class<? extends Trigger> triggerClass = trigger.getClass();

        int types = 0;

        if (overridesMethod(triggerClass, BEFORE_INSERT_METHOD) ||
            overridesMethod(triggerClass, BEFORE_TRY_INSERT_METHOD) ||
            overridesMethod(triggerClass, AFTER_INSERT_METHOD) ||
            overridesMethod(triggerClass, AFTER_TRY_INSERT_METHOD) ||
            overridesMethod(triggerClass, FAILED_INSERT_METHOD))
        {
            types |= FOR_INSERT;
        }

        if (overridesMethod(triggerClass, BEFORE_UPDATE_METHOD) ||
            overridesMethod(triggerClass, BEFORE_TRY_UPDATE_METHOD) ||
            overridesMethod(triggerClass, AFTER_UPDATE_METHOD) ||
            overridesMethod(triggerClass, AFTER_TRY_UPDATE_METHOD) ||
            overridesMethod(triggerClass, FAILED_UPDATE_METHOD))
        {
            types |= FOR_UPDATE;
        }

        if (overridesMethod(triggerClass, BEFORE_DELETE_METHOD) ||
            overridesMethod(triggerClass, BEFORE_TRY_DELETE_METHOD) ||
            overridesMethod(triggerClass, AFTER_DELETE_METHOD) ||
            overridesMethod(triggerClass, AFTER_TRY_DELETE_METHOD) ||
            overridesMethod(triggerClass, FAILED_DELETE_METHOD))
        {
            types |= FOR_DELETE;
        }

        if (overridesMethod(triggerClass, ADAPT_BLOB_METHOD) ||
            overridesMethod(triggerClass, ADAPT_CLOB_METHOD))
        {
            types |= FOR_ADAPT_LOB;
        }

        return types;
    }

    private boolean overridesMethod(Class<? extends Trigger> triggerClass, Method method) {
        try {
            return !method.equals(triggerClass.getMethod(method.getName(),
                                                         method.getParameterTypes()));
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static class TriggerStates<S> {
        final Trigger<? super S>[] mTriggers;
        final Object[] mStates;

        TriggerStates(Trigger<? super S>[] triggers) {
            mTriggers = triggers;
            mStates = new Object[triggers.length];
        }
    }

    private static abstract class ForSomething<S> extends Trigger<S> {
        private static final AtomicReferenceFieldUpdater<ForSomething, ThreadLocal>
            cDisabledFlagRef = AtomicReferenceFieldUpdater
             .newUpdater(ForSomething.class, ThreadLocal.class, "mDisabledFlag");

        private static Trigger[] NO_TRIGGERS = new Trigger[0];

        protected volatile Trigger<? super S>[] mTriggers;

        private volatile ThreadLocal<AtomicInteger> mDisabledFlag;

        ForSomething() {
            mTriggers = NO_TRIGGERS;
        }

        boolean add(Trigger<? super S> trigger) {
            ArrayList<Trigger<? super S>> list =
                new ArrayList<Trigger<? super S>>(Arrays.asList(mTriggers));
            if (list.contains(trigger)) {
                return false;
            }
            list.add(trigger);
            mTriggers = list.toArray(new Trigger[list.size()]);
            return true;
        }

        boolean remove(Trigger<? super S> trigger) {
            ArrayList<Trigger<? super S>> list =
                new ArrayList<Trigger<? super S>>(Arrays.asList(mTriggers));
            if (!list.remove(trigger)) {
                return false;
            }
            mTriggers = list.toArray(new Trigger[list.size()]);
            return true;
        }

        boolean isEmpty() {
            return mTriggers.length == 0;
        }

        boolean isLocallyDisabled() {
            ThreadLocal<AtomicInteger> disabledFlag = mDisabledFlag;
            if (disabledFlag == null) {
                return false;
            }
            // Count indicates how many times disabled (nested)
            AtomicInteger i = disabledFlag.get();
            return i != null && i.get() > 0;
        }

        void localDisable() {
            // Using a count allows this method call to be nested.
            ThreadLocal<AtomicInteger> disabledFlag = disabledFlag();
            AtomicInteger i = disabledFlag.get();
            if (i == null) {
                disabledFlag.set(new AtomicInteger(1));
            } else {
                i.incrementAndGet();
            }
        }

        void localEnable() {
            // Using a count allows this method call to be nested.
            AtomicInteger i = disabledFlag().get();
            if (i != null) {
                i.decrementAndGet();
            }
        }

        private ThreadLocal<AtomicInteger> disabledFlag() {
            ThreadLocal<AtomicInteger> disabledFlag = mDisabledFlag;
            while (disabledFlag == null) {
                disabledFlag = new ThreadLocal<AtomicInteger>();
                if (cDisabledFlagRef.compareAndSet(this, null, disabledFlag)) {
                    break;
                }
                disabledFlag = mDisabledFlag;
            }
            return disabledFlag;
        }
    }

    private static class ForInsert<S> extends ForSomething<S> {
        @Override
        public Object beforeInsert(S storable) throws PersistException {
            if (isLocallyDisabled()) {
                return null;
            }

            TriggerStates<S> triggerStates = null;
            Trigger<? super S>[] triggers = mTriggers;

            for (int i=triggers.length; --i>=0; ) {
                Object state = triggers[i].beforeInsert(storable);
                if (state != null) {
                    if (triggerStates == null) {
                        triggerStates = new TriggerStates<S>(triggers);
                    }
                    triggerStates.mStates[i] = state;
                }
            }

            return triggerStates == null ? triggers : triggerStates;
        }

        @Override
        public Object beforeTryInsert(S storable) throws PersistException {
            if (isLocallyDisabled()) {
                return null;
            }

            TriggerStates<S> triggerStates = null;
            Trigger<? super S>[] triggers = mTriggers;

            for (int i=triggers.length; --i>=0; ) {
                Object state = triggers[i].beforeTryInsert(storable);
                if (state != null) {
                    if (triggerStates == null) {
                        triggerStates = new TriggerStates<S>(triggers);
                    }
                    triggerStates.mStates[i] = state;
                }
            }

            return triggerStates == null ? triggers : triggerStates;
        }

        @Override
        public void afterInsert(S storable, Object state) throws PersistException {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    triggers[i].afterInsert(storable, null);
                }
            } else {
                for (int i=0; i<length; i++) {
                    triggers[i].afterInsert(storable, triggerStates.mStates[i]);
                }
            }
        }

        @Override
        public void afterTryInsert(S storable, Object state) throws PersistException {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    triggers[i].afterTryInsert(storable, null);
                }
            } else {
                for (int i=0; i<length; i++) {
                    triggers[i].afterTryInsert(storable, triggerStates.mStates[i]);
                }
            }
        }

        @Override
        public void failedInsert(S storable, Object state) {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    try {
                        triggers[i].failedInsert(storable, null);
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }
            } else {
                for (int i=0; i<length; i++) {
                    try {
                        triggers[i].failedInsert(storable, triggerStates.mStates[i]);
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }
            }
        }
    }

    private static class ForUpdate<S> extends ForSomething<S> {
        @Override
        public Object beforeUpdate(S storable) throws PersistException {
            if (isLocallyDisabled()) {
                return null;
            }

            TriggerStates<S> triggerStates = null;
            Trigger<? super S>[] triggers = mTriggers;

            for (int i=triggers.length; --i>=0; ) {
                Object state = triggers[i].beforeUpdate(storable);
                if (state != null) {
                    if (triggerStates == null) {
                        triggerStates = new TriggerStates<S>(triggers);
                    }
                    triggerStates.mStates[i] = state;
                }
            }

            return triggerStates == null ? triggers : triggerStates;
        }

        @Override
        public Object beforeTryUpdate(S storable) throws PersistException {
            if (isLocallyDisabled()) {
                return null;
            }

            TriggerStates<S> triggerStates = null;
            Trigger<? super S>[] triggers = mTriggers;

            for (int i=triggers.length; --i>=0; ) {
                Object state = triggers[i].beforeTryUpdate(storable);
                if (state != null) {
                    if (triggerStates == null) {
                        triggerStates = new TriggerStates<S>(triggers);
                    }
                    triggerStates.mStates[i] = state;
                }
            }

            return triggerStates == null ? triggers : triggerStates;
        }

        @Override
        public void afterUpdate(S storable, Object state) throws PersistException {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    triggers[i].afterUpdate(storable, null);
                }
            } else {
                for (int i=0; i<length; i++) {
                    triggers[i].afterUpdate(storable, triggerStates.mStates[i]);
                }
            }
        }

        @Override
        public void afterTryUpdate(S storable, Object state) throws PersistException {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    triggers[i].afterTryUpdate(storable, null);
                }
            } else {
                for (int i=0; i<length; i++) {
                    triggers[i].afterTryUpdate(storable, triggerStates.mStates[i]);
                }
            }
        }

        @Override
        public void failedUpdate(S storable, Object state) {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    try {
                        triggers[i].failedUpdate(storable, null);
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }
            } else {
                for (int i=0; i<length; i++) {
                    try {
                        triggers[i].failedUpdate(storable, triggerStates.mStates[i]);
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }
            }
        }
    }

    private static class ForDelete<S> extends ForSomething<S> {
        @Override
        public Object beforeDelete(S storable) throws PersistException {
            if (isLocallyDisabled()) {
                return null;
            }

            TriggerStates<S> triggerStates = null;
            Trigger<? super S>[] triggers = mTriggers;

            for (int i=triggers.length; --i>=0; ) {
                Object state = triggers[i].beforeDelete(storable);
                if (state != null) {
                    if (triggerStates == null) {
                        triggerStates = new TriggerStates<S>(triggers);
                    }
                    triggerStates.mStates[i] = state;
                }
            }

            return triggerStates == null ? triggers : triggerStates;
        }

        @Override
        public Object beforeTryDelete(S storable) throws PersistException {
            if (isLocallyDisabled()) {
                return null;
            }

            TriggerStates<S> triggerStates = null;
            Trigger<? super S>[] triggers = mTriggers;

            for (int i=triggers.length; --i>=0; ) {
                Object state = triggers[i].beforeTryDelete(storable);
                if (state != null) {
                    if (triggerStates == null) {
                        triggerStates = new TriggerStates<S>(triggers);
                    }
                    triggerStates.mStates[i] = state;
                }
            }

            return triggerStates == null ? triggers : triggerStates;
        }

        @Override
        public void afterDelete(S storable, Object state) throws PersistException {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    triggers[i].afterDelete(storable, null);
                }
            } else {
                for (int i=0; i<length; i++) {
                    triggers[i].afterDelete(storable, triggerStates.mStates[i]);
                }
            }
        }

        @Override
        public void afterTryDelete(S storable, Object state) throws PersistException {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    triggers[i].afterTryDelete(storable, null);
                }
            } else {
                for (int i=0; i<length; i++) {
                    triggers[i].afterTryDelete(storable, triggerStates.mStates[i]);
                }
            }
        }

        @Override
        public void failedDelete(S storable, Object state) {
            if (isLocallyDisabled()) {
                return;
            }

            TriggerStates<S> triggerStates;
            Trigger<? super S>[] triggers;

            if (state == null) {
                triggerStates = null;
                triggers = mTriggers;
            } else if (state instanceof TriggerStates) {
                triggerStates = (TriggerStates<S>) state;
                triggers = triggerStates.mTriggers;
            } else {
                triggerStates = null;
                triggers = (Trigger<? super S>[]) state;
            }

            int length = triggers.length;

            if (triggerStates == null) {
                for (int i=0; i<length; i++) {
                    try {
                        triggers[i].failedDelete(storable, null);
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }
            } else {
                for (int i=0; i<length; i++) {
                    try {
                        triggers[i].failedDelete(storable, triggerStates.mStates[i]);
                    } catch (Throwable e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }
            }
        }
    }

    private static class ForAdaptLob<S> extends ForSomething<S> {
        @Override
        public Blob adaptBlob(S storable, String name, Blob blob) {
            if (isLocallyDisabled()) {
                return blob;
            }

            Trigger<? super S>[] triggers = mTriggers;

            int length = triggers.length;

            for (int i=0; i<length; i++) {
                blob = triggers[i].adaptBlob(storable, name, blob);
            }

            return blob;
        }

        @Override
        public Clob adaptClob(S storable, String name, Clob clob) {
            if (isLocallyDisabled()) {
                return clob;
            }

            Trigger<? super S>[] triggers = mTriggers;

            int length = triggers.length;

            for (int i=0; i<length; i++) {
                clob = triggers[i].adaptClob(storable, name, clob);
            }

            return clob;
        }
    }
}
