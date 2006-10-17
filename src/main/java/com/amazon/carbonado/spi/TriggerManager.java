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

import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.TriggerFactory;

/**
 * Used by Storage implementations to manage triggers and consolidate them into
 * single logical triggers. This class is thread-safe and ensures that changes
 * to the trigger set do not affect transactions in progress.
 *
 * @author Brian S O'Neill
 */
public class TriggerManager<S extends Storable> {
    // Bit masks returned by selectTypes.
    private static final int FOR_INSERT = 1;
    private static final int FOR_UPDATE = 2;
    private static final int FOR_DELETE = 4;

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
        FAILED_DELETE_METHOD;

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
        } catch (NoSuchMethodException e) {
            Error error = new NoSuchMethodError();
            error.initCause(e);
            throw error;
        }
    }

    private volatile ForInsert<S> mForInsert;
    private volatile ForUpdate<S> mForUpdate;
    private volatile ForDelete<S> mForDelete;

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
     * Returns consolidated trigger to call for insert operations, or null if
     * none.
     */
    public Trigger<? super S> getInsertTrigger() {
        return mForInsert;
    }

    /**
     * Returns consolidated trigger to call for update operations, or null if
     * none.
     */
    public Trigger<? super S> getUpdateTrigger() {
        return mForUpdate;
    }

    /**
     * Returns consolidated trigger to call for delete operations, or null if
     * none.
     */
    public Trigger<? super S> getDeleteTrigger() {
        return mForDelete;
    }

    public synchronized boolean addTrigger(Trigger<? super S> trigger) {
        if (trigger == null) {
            throw new IllegalArgumentException();
        }

        int types = selectTypes(trigger);
        if (types == 0) {
            return false;
        }

        boolean retValue = false;

        if ((types & FOR_INSERT) != 0) {
            if (mForInsert == null) {
                mForInsert = new ForInsert<S>();
            }
            retValue |= mForInsert.add(trigger);
        }

        if ((types & FOR_UPDATE) != 0) {
            if (mForUpdate == null) {
                mForUpdate = new ForUpdate<S>();
            }
            retValue |= mForUpdate.add(trigger);
        }

        if ((types & FOR_DELETE) != 0) {
            if (mForDelete == null) {
                mForDelete = new ForDelete<S>();
            }
            retValue |= mForDelete.add(trigger);
        }

        return retValue;
    }

    public synchronized boolean removeTrigger(Trigger<? super S> trigger) {
        if (trigger == null) {
            throw new IllegalArgumentException();
        }

        int types = selectTypes(trigger);
        if (types == 0) {
            return false;
        }

        boolean retValue = false;

        if ((types & FOR_INSERT) != 0) {
            if (mForInsert != null && mForInsert.remove(trigger)) {
                retValue = true;
                if (mForInsert.isEmpty()) {
                    mForInsert = null;
                }
            }
        }

        if ((types & FOR_UPDATE) != 0) {
            if (mForUpdate != null && mForUpdate.remove(trigger)) {
                retValue = true;
                if (mForUpdate.isEmpty()) {
                    mForUpdate = null;
                }
            }
        }

        if ((types & FOR_DELETE) != 0) {
            if (mForDelete != null && mForDelete.remove(trigger)) {
                retValue = true;
                if (mForDelete.isEmpty()) {
                    mForDelete = null;
                }
            }
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
        private static Trigger[] NO_TRIGGERS = new Trigger[0];

        protected volatile Trigger<? super S>[] mTriggers;

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
    }

    private static class ForInsert<S> extends ForSomething<S> {
        @Override
        public Object beforeInsert(S storable) throws PersistException {
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
}
