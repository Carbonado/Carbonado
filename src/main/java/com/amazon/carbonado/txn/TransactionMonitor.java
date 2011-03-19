/*
 * Copyright 2011 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.txn;

import com.amazon.carbonado.Transaction;

/**
 * Is notified as transactions enter and exit. Implementation must be thread-safe.
 *
 * @author Brian S O'Neill
 * @see TransactionManager
 */
public interface TransactionMonitor {
    /**
     * Called by a thread which has just entered a transaction.
     *
     * @param entered transaction just entered
     * @param parent optional parent of transaction; is null for top level transactions
     */
    public abstract void entered(Transaction entered, Transaction parent);

    /**
     * Called by a thread which has just exited a transaction. Only the first
     * invocation of the exit method is passed to this monitor.
     *
     * @param exited transaction just exited
     * @param active optional transaction which is now active; is null if the
     * outermost transaction scope exited
     */
    public abstract void exited(Transaction exited, Transaction active);
}
