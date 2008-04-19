/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Overrides the primary name of a Storable property. By default, the primary
 * name of a property is determined by JavaBeans conventions. When overridden,
 * all references to the named property must use the new name.
 *
 * <p>Example:<pre>
 * &#64;PrimaryKey(<b>"userId"</b>)
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     <b>&#64;Name("userId")</b>
 *     long getUserInfoID();
 *     void setUserInfoID(long id);
 *
 *     ...
 * }
 * </pre>
 *
 * The first character of a name must be a {@link
 * Character#isUnicodeIdentifierStart unicode identifier start}, and all
 * subsequent characters must be a {@link Character#isUnicodeIdentifierPart
 * unicode identifier part}.
 *
 * @since 1.2
 * @author Fang Chen
 * @author Brian S O'Neill
 * @see Alias
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Name {

    /**
     * Name assigned to the property.
     */
    String value();

}
