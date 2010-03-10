/*
 * Copyright 2010 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.gen;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.sequence.SequenceValueProducer;

import org.cojen.util.QuickConstructorGenerator;
import org.cojen.util.ThrowUnchecked;

/**
 * Creates {@link Storable} instances which are basic data containers. All load
 * and store operations throw an exception, as do accesses to join properties.
 *
 * @author Brian S O'Neill
 * @since 1.2.2
 */
public class DetachedStorableFactory {
    private DetachedStorableFactory() {
    }

    public static <S extends Storable> S create(Class<S> type)
        throws SupportException
    {
        return (S) QuickConstructorGenerator
            .getInstance(DelegateStorableGenerator.getDelegateClass(type, null),
                         NoSupport.Factory.class)
            .newInstance(NoSupport.THE);
    }

    private static class NoSupport implements DelegateSupport<Storable> {
        public static interface Factory {
            Storable newInstance(DelegateSupport support);
        }

        private static final NoSupport THE = new NoSupport();

        private static final String MESSAGE = "Storable is detached";

        private NoSupport() {
        }

        public boolean doTryLoad(Storable storable) throws FetchException {
            throw new FetchException(MESSAGE);
        }

        public boolean doTryInsert(Storable storable) throws PersistException {
            throw new PersistException(MESSAGE);
        }

        public boolean doTryUpdate(Storable storable) throws PersistException {
            throw new PersistException(MESSAGE);
        }

        public boolean doTryDelete(Storable storable) throws PersistException {
            throw new PersistException(MESSAGE);
        }

        public SequenceValueProducer getSequenceValueProducer(String name)
            throws PersistException
        {
            throw new PersistException(MESSAGE);
        }

        public Trigger<? super Storable> getInsertTrigger() {
            return null;
        }

        public Trigger<? super Storable> getUpdateTrigger() {
            return null;
        }

        public Trigger<? super Storable> getDeleteTrigger() {
            return null;
        }

        public Trigger<? super Storable> getLoadTrigger() {
            return null;
        }

        public void locallyDisableLoadTrigger() {
        }

        public void locallyEnableLoadTrigger() {
        }

        public Repository getRootRepository() {
            // This method is only called when fetching join properties.
            ThrowUnchecked.fire(new FetchException(MESSAGE));
            return null;
        }

        public boolean isPropertySupported(String propertyName) {
            return true;
        }
   }
}
