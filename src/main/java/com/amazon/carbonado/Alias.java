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
 * Identifies alternate names for a {@link Storable} or a Storable property. An alias is used
 * only by a repository to link to entities. Without an alias, the repository will perform
 * a best guess at finding an entity to use. Aliases may be ignored by repositories that
 * don't require explicitly named entities.
 * <P>The most common use for an alias is for a JDBC repository, to link a storable to a table and
 * its properties to the corresponding columns.  Naming conventions for databases rarely work
 * well for class and variable names.
 *
 * <p>Example:<pre>
 * <b>&#64;Alias("USER_INFO")</b>
 * &#64;PrimaryKey("userInfoID")
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     <b>&#64;Alias("USER_ID")</b>
 *     long getUserInfoID();
 *     void setUserInfoID(long id);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see Name
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Alias {

    /**
     * Alias values for the storage layer to select from. It will choose the
     * first one in the list that matches one of its own entities.
     */
    String[] value();

}
