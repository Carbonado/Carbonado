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

import java.lang.annotation.*;

/**
 * Identifies a {@link Storable} property which is not directly persisted, but
 * is instead derived from other property values. A derived property cannot be
 * abstract, and a "set" method is optional.
 *
 * <p>Derived properties can be used just like a normal property in most
 * cases. They can be used in query filters, indexes, alternate keys, and they
 * can also be used to define a {@link Version} property.
 *
 * <p>If the derived property depends on {@link Join} properties and is also
 * used in an index or alternate key, dependencies must be listed in order for
 * the index to be properly updated.
 *
 * <p>Example:<pre>
 * &#64;Indexes(&#64;Index("uppercaseName"))
 * public abstract class UserInfo implements Storable&lt;UserInfo&gt; {
 *     /**
 *      * Derive an uppercase name for case-insensitive searches.
 *      *&#47;
 *     <b>&#64;Derived</b>
 *     public String getUppercaseName() {
 *         String name = getName();
 *         return name == null ? null : name.toUpperCase();
 *     }
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @author Tobias Holgers
 * @since 1.2
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Derived {
    /**
     * List of properties that this property is derived from.
     */
    String[] from() default {};

    /**
     * Returns whether this property should be included when copying a
     * storable. Copying of a derived property uses the "get" and "set" methods
     * and requires the "set" method to be defined. Default false.
     */
    boolean shouldCopy() default false;
}
