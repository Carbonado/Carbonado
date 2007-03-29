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

import java.lang.annotation.*;

/**
 * Identifies a {@link Storable} property capable of selecting its own value on
 * insert, by a named sequence. Support for sequences is repository dependent,
 * and if not supported, a {@link PersistException} is thrown when trying to
 * insert. Explicitly specifying a value bypasses the sequence altogether.
 *
 * <p>Example:<pre>
 * &#64;PrimaryKey("userInfoID")
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     <b>&#64;Sequence("USER_ID_SEQ")</b>
 *     long getUserInfoID();
 *     void setUserInfoID(long id);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see Automatic
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Sequence {
    /**
     * Name of the sequence used by the storage layer.
     */
    String value();
}
