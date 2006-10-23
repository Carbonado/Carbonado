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

package com.amazon.carbonado.constraint;

import java.lang.annotation.*;
import java.util.Arrays;

import com.amazon.carbonado.MalformedTypeException;

/**
 * Limits the value of a property to be a member of a specific set. The
 * property value may be a String, CharSequence, char, Character, or character
 * array. If the property value is outside the set, an IllegalArgumentException
 * is thrown.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     char isActive();
 *     <b>&#64;TextConstraint(allowed={"Y", "N"})</b>
 *     void setActive(char value);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see IntegerConstraint
 * @see FloatConstraint
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ConstraintDefinition
public @interface TextConstraint {
    /**
     * Specific allowed values for property. Default is unlimited.
     */
    String[] allowed() default {};

    /**
     * Specific disallowed values for property. Default is none.
     */
    String[] disallowed() default {};

    /**
     * Constraint implementation for {@link TextConstraint}.
     */
    public static class Constraint {
        private final String mPropertyName;

        /** Allowed values, sorted for binary search. */
        private final String[] mAllowed;

        /** Disallowed values, sorted for binary search. */
        private final String[] mDisallowed;

        /**
         * @param type type of object that contains the constrained property
         * @param propertyName name of property with constraint
         * @param ann specific annotation that binds to this constraint class
         */
        public Constraint(Class<?> type, String propertyName, TextConstraint ann) {
            this(type, propertyName, ann.allowed(), ann.disallowed());
        }

        /**
         * @param type type of object that contains the constrained property
         * @param propertyName name of property with constraint
         * @param allowed optional set of allowed values
         * @param disallowed optional set of disallowed values
         */
        public Constraint(Class<?> type, String propertyName,
                          String[] allowed, String[] disallowed) {
            mPropertyName = propertyName;

            if (disallowed == null || disallowed.length == 0) {
                disallowed = null;
            } else {
                disallowed = disallowed.clone();
                Arrays.sort(disallowed);
            }

            if (allowed == null || allowed.length == 0) {
                allowed = null;
            } else {
                allowed = allowed.clone();
                Arrays.sort(allowed);
                if (disallowed != null) {
                    for (String value : allowed) {
                        if (Arrays.binarySearch(disallowed, value) >= 0) {
                            throw new MalformedTypeException
                                (type, "Allowed value contradiction for text constraint " +
                                 "on property \"" + propertyName + "\": " + value);
                        }
                    }

                    // No need to have a set of disallowed values.
                    disallowed = null;
                }
            }

            mDisallowed = disallowed;
            mAllowed = allowed;
        }

        public void constrain(CharSequence propertyValue) {
            constrain(propertyValue.toString());
        }

        public void constrain(String propertyValue) {
            if (propertyValue == null) {
                return;
            }
            if (mDisallowed != null && Arrays.binarySearch(mDisallowed, propertyValue) >= 0) {
                throw new IllegalArgumentException
                    ("Value for \"" + mPropertyName + "\" is disallowed: " + propertyValue);
            }
            if (mAllowed != null && Arrays.binarySearch(mAllowed, propertyValue) < 0) {
                throw new IllegalArgumentException
                    ("Value for \"" + mPropertyName + "\" is not allowed: " + propertyValue);
            }
        }

        public void constrain(char propertyValue) {
            constrain(Character.toString(propertyValue));
        }

        public void constrain(char[] propertyValue) {
            if (propertyValue != null) {
                constrain(new String(propertyValue));
            }
        }
    }
}

