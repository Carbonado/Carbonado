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

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Transaction;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.PersistException;

/**
 * Neatly scope a transactional operation.  To use, a subclass of RunnableTransaction should be
 * provided which implements any one of the three flavors of the body method.
 * The default implementations pass control from most specific to least specific -- that is,
 * from {@link #body(Storable)} to {@link #body()} -- so the
 * implementor is free to override whichever makes the most sense.
 *
 * <P>A typical use pattern would be:
 *
 *  <pre>
 *  RunnableTransaction rt = new RunnableTransaction(repository) {
 *      public void body() throws PersistException {
 *        for (Storable s : someFieldContainingStorables) {
 *            s.insert();
 *       }
 *  };
 *  rt.run();
 * </pre>
 *
 * @author Don Schneider
 * @author Todd V. Jonker (jonker)
 */
public class RunnableTransaction {
    final Repository mRepo;

    public RunnableTransaction(Repository repo) {
        mRepo = repo;
    }

    /**
     * Enter a transaction, execute {@link #body(Storable)} for each storable, commit
     * if no exception, and exit the transaction.
     * @param storables array of storables on which to operate
     * @throws PersistException
     */
    public final <S extends Storable> void run(S storable, S... storables)
        throws PersistException
    {
        Transaction txn = mRepo.enterTransaction();
        try {
            for (S s : storables) {
                body(s);
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    /**
     * Enter a transaction, execute {@link #body(Storable)} on the provided storable, commit if no
     * exception, and exit the transaction.
     * @param storable on which to operate
     * @throws PersistException
     */
    public final <S extends Storable> void run(S storable) throws PersistException {
        Transaction txn = mRepo.enterTransaction();
        try {
            body(storable);
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    /**
     * Enter a transaction on the provided repository, execute {@link #body()}
     * @throws PersistException
     */
    public final void run() throws PersistException {
        Transaction txn = mRepo.enterTransaction();
        try {
            body();
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    public <S extends Storable> void body(S s) throws PersistException {
        body();
    }

    public void body() throws PersistException {
    }

    public String toString() {
        return "RunnableTransaction(" + mRepo + ')';
    }
}
