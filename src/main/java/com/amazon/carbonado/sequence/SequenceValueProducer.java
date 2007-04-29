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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

/**
 * Produces values for sequences.
 *
 * @author Brian S O'Neill
 * @author bcastill
 * @see com.amazon.carbonado.Sequence
 * @since 1.2
 */
public interface SequenceValueProducer {
    /**
     * Returns the next value from the sequence, which may wrap negative if all
     * positive values are exhausted. When sequence wraps back to initial
     * value, the sequence is fully exhausted, and an exception is thrown to
     * indicate this.
     *
     * <p>Note: this method throws PersistException even for fetch failures
     * since this method is called by insert operations. Insert operations can
     * only throw a PersistException.
     *
     * @throws PersistException for fetch/persist failure or if sequence is exhausted.
     */
    public long nextLongValue() throws PersistException;

    /**
     * Returns the next value from the sequence, which may wrap negative if all
     * positive values are exhausted. When sequence wraps back to initial
     * value, the sequence is fully exhausted, and an exception is thrown to
     * indicate this.
     *
     * <p>Note: this method throws PersistException even for fetch failures
     * since this method is called by insert operations. Insert operations can
     * only throw a PersistException.
     *
     * @throws PersistException for fetch/persist failure or if sequence is
     * exhausted for int values.
     */
    public int nextIntValue() throws PersistException;

    /**
     * Returns the next decimal string value from the sequence, which remains
     * positive. When sequence wraps back to initial value, the sequence is
     * fully exhausted, and an exception is thrown to indicate this.
     *
     * <p>Note: this method throws PersistException even for fetch failures
     * since this method is called by insert operations. Insert operations can
     * only throw a PersistException.
     *
     * @throws PersistException for fetch/persist failure or if sequence is exhausted.
     */
    public String nextDecimalValue() throws PersistException;

    /**
     * Returns the next numerical string value from the sequence, which remains
     * positive. When sequence wraps back to initial value, the sequence is
     * fully exhausted, and an exception is thrown to indicate this.
     *
     * <p>Note: this method throws PersistException even for fetch failures
     * since this method is called by insert operations. Insert operations can
     * only throw a PersistException.
     *
     * @param radix use 2 for binary, 10 for decimal, 16 for hex. Max is 36.
     * @param minLength ensure string is at least this long (padded with zeros if
     * necessary) to ensure proper string sort
     * @throws PersistException for fetch/persist failure or if sequence is exhausted.
     */
    public String nextNumericalValue(int radix, int minLength) throws PersistException;
    
    /**
     * Allow any unused reserved values to be returned for re-use. If the
     * repository is shared by other processes, then reserved values might not
     * be returnable.
     *
     * <p>This method should be called during the shutdown process of a
     * repository, although calling it does not invalidate this
     * SequenceValueGenerator. If getNextValue is called again, it will reserve
     * values again.
     *
     * @return true if reserved values were returned
     */
    public boolean returnReservedValues() throws FetchException, PersistException;
}
