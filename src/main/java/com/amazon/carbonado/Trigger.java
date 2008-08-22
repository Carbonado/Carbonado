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

/**
 * Callback mechanism to allow custom code to run when a storable is
 * persisted. By default, the methods defined in this class do
 * nothing. Subclass and override trigger conditions of interest, and then
 * {@link Storage#addTrigger register} it. Each overridden trigger method is
 * called in the same transaction scope as the persist operation. Trigger
 * implementations are encouraged to override the equals method, to prevent
 * accidental double registration.
 *
 * <p>To ensure proper nesting, all "before" events are run in the
 * <em>opposite</em> order that the trigger was registered. All "after" and
 * "failed" events are run in the same order that the trigger was registered.
 * In other words, the last added trigger is at the outermost nesting level.
 *
 * <p>Triggers always run within the same transaction as the triggering
 * operation. The exact isolation level and update mode is outside the
 * trigger's control. If an explicit isolation level or update mode is
 * required, create a nested transaction within a trigger method. A trigger's
 * nested transaction can also be defined to span the entire triggering operation.
 * To do this, enter the transaction in the "before" method, but return the
 * transaction object without exiting it. The "after" method is responsible for
 * exiting the transaction. It extracts (or simply casts) the transaction from
 * the state object passed into it. When creating spanning transactions like
 * this, it is critical that the "failed" method be defined to properly exit
 * the transaction upon failure.
 *
 * @author Brian S O'Neill
 */
public abstract class Trigger<S> {
    /**
     * Called before a storable is to be inserted. The default implementation
     * does nothing.
     *
     * <p>Any exception thrown by this method will cause the insert operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the insert method.
     *
     * @param storable storable before being inserted
     * @return arbitrary state object, passed to afterInsert or failedInsert method
     */
    public Object beforeInsert(S storable) throws PersistException {
        return null;
    }

    /**
     * Called before a storable is to be inserted via tryInsert. The default
     * implementation simply calls {@link #beforeInsert}. Only override if
     * trigger needs to distinguish between different insert variants.
     *
     * <p>Any exception thrown by this method will cause the tryInsert operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the tryInsert method.
     *
     * @param storable storable before being inserted
     * @return arbitrary state object, passed to afterTryInsert or failedInsert method
     * @see #abortTry
     */
    public Object beforeTryInsert(S storable) throws PersistException {
        return beforeInsert(storable);
    }

    /**
     * Called right after a storable has been successfully inserted. The
     * default implementation does nothing.
     *
     * <p>Any exception thrown by this method will cause the insert operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the insert method.
     *
     * @param storable storable after being inserted
     * @param state object returned by beforeInsert method
     */
    public void afterInsert(S storable, Object state) throws PersistException {
    }

    /**
     * Called right after a storable has been successfully inserted via
     * tryInsert. The default implementation simply calls {@link #afterInsert}.
     * Only override if trigger needs to distinguish between different insert
     * variants.
     *
     * <p>Any exception thrown by this method will cause the tryInsert
     * operation to rollback and all remaining triggers to not run. The
     * exception is ultimately passed to the caller of the tryInsert method.
     *
     * @param storable storable after being inserted
     * @param state object returned by beforeTryInsert method
     * @see #abortTry
     */
    public void afterTryInsert(S storable, Object state) throws PersistException {
        afterInsert(storable, state);
    }

    /**
     * Called when an insert operation failed due to a unique constraint
     * violation or an exception was thrown. The main purpose of this method is
     * to allow any necessary clean-up to occur on the optional state object.
     *
     * <p>Any exception thrown by this method will be passed to the current
     * thread's uncaught exception handler.
     *
     * @param storable storable which failed to be inserted
     * @param state object returned by beforeInsert method, but it may be null
     */
    public void failedInsert(S storable, Object state) {
    }

    /**
     * Called before a storable is to be updated. The default implementation
     * does nothing.
     *
     * <p>Any exception thrown by this method will cause the update operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the update method.
     *
     * @param storable storable before being updated
     * @return arbitrary state object, passed to afterUpdate or failedUpdate method
     */
    public Object beforeUpdate(S storable) throws PersistException {
        return null;
    }

    /**
     * Called before a storable is to be updated via tryUpdate. The default
     * implementation simply calls {@link #beforeUpdate}. Only override if
     * trigger needs to distinguish between different update variants.
     *
     * <p>Any exception thrown by this method will cause the tryUpdate operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the tryUpdate method.
     *
     * @param storable storable before being updated
     * @return arbitrary state object, passed to afterTryUpdate or failedUpdate method
     * @see #abortTry
     */
    public Object beforeTryUpdate(S storable) throws PersistException {
        return beforeUpdate(storable);
    }

    /**
     * Called right after a storable has been successfully updated. The default
     * implementation does nothing.
     *
     * <p>Any exception thrown by this method will cause the update operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the update method.
     *
     * @param storable storable after being updated
     * @param state optional object returned by beforeUpdate method
     */
    public void afterUpdate(S storable, Object state) throws PersistException {
    }

    /**
     * Called right after a storable has been successfully updated via
     * tryUpdate. The default implementation simply calls {@link #afterUpdate}.
     * Only override if trigger needs to distinguish between different update
     * variants.
     *
     * <p>Any exception thrown by this method will cause the tryUpdate
     * operation to rollback and all remaining triggers to not run. The
     * exception is ultimately passed to the caller of the tryUpdate method.
     *
     * @param storable storable after being updated
     * @param state object returned by beforeTryUpdate method
     * @see #abortTry
     */
    public void afterTryUpdate(S storable, Object state) throws PersistException {
        afterUpdate(storable, state);
    }

    /**
     * Called when an update operation failed because the record was missing or
     * an exception was thrown. The main purpose of this method is to allow any
     * necessary clean-up to occur on the optional state object.
     *
     * <p>Any exception thrown by this method will be passed to the current
     * thread's uncaught exception handler.
     *
     * @param storable storable which failed to be updated
     * @param state optional object returned by beforeUpdate
     * method, but it may be null
     */
    public void failedUpdate(S storable, Object state) {
    }

    /**
     * Called before a storable is to be deleted. The default implementation
     * does nothing.
     *
     * <p>Any exception thrown by this method will cause the delete operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the delete method.
     *
     * @param storable storable before being deleted
     * @return arbitrary state object, passed to afterDelete or failedDelete method
     */
    public Object beforeDelete(S storable) throws PersistException {
        return null;
    }

    /**
     * Called before a storable is to be deleted via tryDelete. The default
     * implementation simply calls {@link #beforeDelete}. Only override if
     * trigger needs to distinguish between different delete variants.
     *
     * <p>Any exception thrown by this method will cause the tryDelete operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the tryDelete method.
     *
     * @param storable storable before being deleted
     * @return arbitrary state object, passed to afterTryDelete or failedDelete method
     * @see #abortTry
     */
    public Object beforeTryDelete(S storable) throws PersistException {
        return beforeDelete(storable);
    }

    /**
     * Called right after a storable has been successfully deleted. The default
     * implementation does nothing.
     *
     * <p>Any exception thrown by this method will cause the delete operation
     * to rollback and all remaining triggers to not run. The exception is
     * ultimately passed to the caller of the delete method.
     *
     * @param storable storable after being deleted
     * @param state optional object returned by beforeDelete method
     */
    public void afterDelete(S storable, Object state) throws PersistException {
    }

    /**
     * Called right after a storable has been successfully deleted via
     * tryDelete. The default implementation simply calls {@link #afterDelete}.
     * Only override if trigger needs to distinguish between different delete
     * variants.
     *
     * <p>Any exception thrown by this method will cause the tryDelete
     * operation to rollback and all remaining triggers to not run. The
     * exception is ultimately passed to the caller of the tryDelete method.
     *
     * @param storable storable after being deleted
     * @param state object returned by beforeTryDelete method
     * @see #abortTry
     */
    public void afterTryDelete(S storable, Object state) throws PersistException {
        afterDelete(storable, state);
    }

    /**
     * Called when an delete operation failed because the record was missing or
     * an exception was thrown. The main purpose of this method is to allow any
     * necessary clean-up to occur on the optional state object.
     *
     * <p>Any exception thrown by this method will be passed to the current
     * thread's uncaught exception handler.
     *
     * @param storable storable which failed to be deleted
     * @param state optional object returned by beforeDelete
     * method, but it may be null
     */
    public void failedDelete(S storable, Object state) {
    }

    /**
     * Called right after a storable has been successfully loaded or
     * fetched. The default implementation does nothing.
     *
     * @param storable storable after being loaded or fetched
     * @since 1.2
     */
    public void afterLoad(S storable) throws FetchException {
    }

    /**
     * Call to quickly abort a "try" operation, returning false to the
     * caller. This method should not be called by a non-try trigger method,
     * since the caller gets thrown an exception with an incomplete stack trace.
     *
     * <p>This method never returns normally, but as a convenience, a return
     * type is defined. The abort exception can be thrown by {@code throw abortTry()},
     * but the {@code throw} keyword is not needed.
     */
    protected Abort abortTry() throws Abort {
        // Throwing and catching an exception is not terribly expensive, but
        // creating a new exception is more than an order of magnitude slower.
        // Therefore, re-use the same instance. It has no stack trace since it
        // would be meaningless.
        throw Abort.INSTANCE;
    }

    public static final class Abort extends PersistException {
        private static final long serialVersionUID = -8498639796139966911L;

        static final Abort INSTANCE = new Abort();

        private Abort() {
            super("Trigger aborted operation", null);
        }

        private Abort(String message) {
            super(message);
            super.fillInStackTrace();
        }

        /**
         * Override to remove the stack trace.
         */
        @Override
        public Throwable fillInStackTrace() {
            return null;
        }

        /**
         * Returns this exception but with a fresh stack trace. The trace does
         * not include the original thrower of this exception.
         */
        public Abort withStackTrace() {
            Abort a = new Abort(getMessage());

            StackTraceElement[] trace = a.getStackTrace();
            if (trace != null && trace.length > 1) {
                // Trim off this method from the trace, which is element 0.
                StackTraceElement[] trimmed = new StackTraceElement[trace.length - 1];
                System.arraycopy(trace, 1, trimmed, 0, trimmed.length);
                a.setStackTrace(trimmed);
            }

            return a;
        }
    }
}
