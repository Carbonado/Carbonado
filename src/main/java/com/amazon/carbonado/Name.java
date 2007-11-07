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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies prefered name for a {@link Storable} or a Storable property. By default the 
 * property name is following the convention of a java Bean. Use this annotation when 
 * you want to use a prefered name for a storable property. .
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable&lt;UserInfo&gt; {
 *     <b>&#64;Name("MyName")</b>
 *     String getName();
 *     void setName(String name);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Fang Chen
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Name {

    /**
     * The prefered name of the property
     */
    String value();

}