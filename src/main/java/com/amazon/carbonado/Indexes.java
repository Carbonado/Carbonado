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
 * The Indexes annotation is applied to a {@link Storable} for specifying all
 * the {@link Index indexes} it should have.
 *
 * <p>Example:<pre>
 * <b>&#64;Indexes</b>({
 *     <b>&#64;Index</b>("name"),
 *     <b>&#64;Index</b>("-lastModified"),
 *     <b>&#64;Index</b>({"length", "lastModified"})
 * })
 * &#64;PrimaryKey("ID")
 * public interface FileInfo extends Storable&lt;FileInfo&gt; {
 *     long getID();
 *     void setID(long id);
 *
 *     String getName();
 *     void setName(String name);
 *
 *     long getLength();
 *     void setLength(long length);
 *
 *     long getLastModified();
 *     void getLastModified(long datetime);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see Index
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Indexes {
    /**
     * A list of Index annotations.
     */
    Index[] value() default {};
}
