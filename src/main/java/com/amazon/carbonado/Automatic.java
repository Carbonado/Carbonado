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
 * insert. The actual process by which a value is automatically assigned is
 * repository dependent. In the JDBC repository, the value might come from an
 * auto-increment column or a database-specific trigger.
 *
 * <p>If the underlying repository doesn't automatically supply a value to an
 * automatic property, no immediate warning is given and instead the property
 * will be assigned a default value of null or zero. This may cause problems if
 * the automatic property is a member of a key. Explicitly specifying a value
 * can sometimes be used to bypass the automatic value altogether.
 *
 * <p>Example:<pre>
 * &#64;PrimaryKey("userInfoID")
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     <b>&#64;Automatic</b>
 *     long getUserInfoID();
 *     void setUserInfoID(long id);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see Sequence
 * @since 1.2
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Automatic {
}
