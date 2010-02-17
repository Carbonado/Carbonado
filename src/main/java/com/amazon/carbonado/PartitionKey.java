/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
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
 * Identifies a {@link Storable} property as being a member of the partition key.
 * This key is ignored if the underlying repository lacks support for partitioning.
 *
 * <p>Example:<pre>
 * &#64;PrimaryKey("userInfoID")
 * <b>&#64;PartitionKey("userInfoGroup")</b>
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     long getUserInfoID();
 *     void setUserInfoID(long id);
 *
 *     String getUserInfoGroup();
 *     void setUserInfoGroup(String group);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Archit Shivaprakash
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface PartitionKey {
    /**
     * A list of property names.
     */
    String[] value() default {};
}
