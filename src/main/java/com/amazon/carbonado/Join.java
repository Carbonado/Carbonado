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
 * Identifies a {@link Storable} property as defining a join relationship
 * with another Storable type. Joins can also refer to their own enclosing
 * class or interface.
 * <p>
 * To complete the join, lists of internal and external properties may be
 * supplied. If these lists are not supplied, then join is "natural", and the
 * properties are determined automatically. When the lists are specified, the
 * join is "explicit". Natural joins are merely a convenience; they can always
 * be replaced by an explicit join.
 * <p>
 * The lists used for explicit joins must have the same length, and each must
 * have at least one element. Each element in the internal list must refer to
 * a property defined in this property's class or interface, and each element
 * in the external list must refer to a matching property defined in the joined
 * type. The matched up property pairs must not themselves be join properties,
 * and they must be compatible with each other.
 * <p>
 * If the join is made to external properties which do not completely specify a
 * primary key, then the type of the join property must be a {@link Query} of
 * the joined type. When the type is a Query, a property mutator method cannot
 * be defined. The returned query has all of the "with" parameters filled in.
 * <p>
 * With a natural join, the internal and external properties are deduced by
 * examining the type of the referenced join property. If the type is a Query,
 * then the internal and external properties are set to match this property's
 * primary key. The referenced join property (specified as a parameterized type
 * to Query) must have properties matching name and type of this property's
 * primary key.
 * <p>
 * If a natural join's property type is not defined by a Query, then the
 * internal and external properties are set to match the referenced property's
 * primary key. This join property must have properties matching name and type
 * of the referenced property's primary key.
 *
 * <p>Example:<pre>
 * &#64;PrimaryKey("addressID")
 * public interface Address extends Storable {
 *     int getAddressID();
 *
 *     ...
 * }
 *
 * &#64;PrimaryKey("userID")
 * public interface UserInfo extends Storable {
 *     int getUserID();
 *     void setUserID(int id);
 *
 *     int getAddressID();
 *     void setAddressID(int value);
 *
 *     // Natural join, which works because Address has a primary key
 *     // property of addressID which matches a property in this type.
 *     <b>&#64;Join</b>
 *     Address getAddress() throws FetchException;
 *     void setAddress(Address address);
 *
 *     // Explicit join, equivalent to getAddress.
 *     <b>&#64;Join(internal="addressID", external="addressID")</b>
 *     Address getCurrentAddress() throws FetchException;
 *     void setCurrentAddress(Address address);
 *
 *     &#64;Nullable
 *     Integer getParentID();
 *     void setParentID(Integer value);
 *
 *     // Many-to-one relationship
 *     &#64;Nullable
 *     <b>&#64;Join(internal="parentID", external="userID")</b>
 *     UserInfo getParent() throws FetchException;
 *     void setParent(UserInfo parent);
 *
 *     // One-to-many relationship
 *     <b>&#64;Join(internal="userID", external="parentID")</b>
 *     Query&lt;UserInfo&gt; getChildren() throws FetchException;
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Join {

    /**
     * List of property names defined in this property's enclosing class or
     * interface.
     */
    String[] internal() default {};

    /**
     * List of property names defined in the foreign property's enclosing class
     * or interface.
     */
    String[] external() default {};

}
