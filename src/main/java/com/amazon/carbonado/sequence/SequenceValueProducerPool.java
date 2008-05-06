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

package com.amazon.carbonado.sequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.util.AbstractPool;

/**
 * A concurrent pool of strongly referenced {@link SequenceValueProducer}
 * instances mapped by name. SequenceValueProducer instances are lazily created
 * and pooled.
 * 
 * @author bcastill
 * @author Brian S O'Neill
 * @since 1.2
 */
public abstract class SequenceValueProducerPool
    extends AbstractPool<String, SequenceValueProducer, RepositoryException>
{
    public SequenceValueProducerPool() {
    }

    /**
     * Returns a SequenceValueProducer instance for the given name, which is
     * lazily created and pooled. If multiple threads are requesting upon the
     * same name concurrently, at most one thread attempts to lazily create the
     * SequenceValueProducer. The others wait for it to become available.
     *
     * @param name name of sequence
     */
    @Override
    public SequenceValueProducer get(String name) throws RepositoryException {
        return (SequenceValueProducer) super.get(name);
    }
    
    /**
     * Returns reserved values for all {@link SequenceValueProducer}s.
     *
     * @param log optional log to report errors; uses default log if null
     */
    public void returnReservedValues(Log log) {
        for (SequenceValueProducer producer : values()) {
            try {
                producer.returnReservedValues();
            } catch (RepositoryException e) {
                if (log == null) {
                    log = LogFactory.getLog(SequenceValueProducerPool.class);
                }
                log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    protected final SequenceValueProducer create(String name) throws RepositoryException {
        return createSequenceValueProducer(name);
    }

    protected abstract SequenceValueProducer createSequenceValueProducer(String name)
        throws RepositoryException;
}
