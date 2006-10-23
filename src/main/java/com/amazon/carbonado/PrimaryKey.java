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
 * Identifies a {@link Storable} property as being a member of the primary
 * key. All Storable types must have at least one property belonging to the
 * primary key.
 *
 * <p>Example:<pre>
 * <b>&#64;PrimaryKey</b>("userInfoID")
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     long getUserInfoID();
 *     void setUserInfoID(long id);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see AlternateKeys
 * @see Sequence
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface PrimaryKey {
    /**
     * A list of property names, which may be prefixed with '+' or '-' to
     * indicate a preference for ascending or descending order.
     */
    String[] value();
}
