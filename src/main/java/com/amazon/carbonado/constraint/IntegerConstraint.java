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
 * property value may be a boxed or unboxed byte, short, int, long, float,
 * double, String, CharSequence, char, Character, or character array. If the
 * property value is outside the set, an IllegalArgumentException is thrown.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     int getAge();
 *     <b>&#64;IntegerConstraint(min=0, max=120)</b>
 *     void setAge(int value);
 *
 *     int getRoleID();
 *     <b>&#64;IntegerConstraint(allowed={ROLE_REGULAR, ROLE_ADMIN})</b>
 *     void setRoleID(int role);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see FloatConstraint
 * @see TextConstraint
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ConstraintDefinition
public @interface IntegerConstraint {
    /**
     * Specific allowed values for property. Default is unlimited.
     */
    long[] allowed() default {};

    /**
     * Specific disallowed values for property. Default is none.
     */
    long[] disallowed() default {};

    /**
     * Specify minimum allowed value for integer property. Default is unlimited.
     */
    long min() default Long.MIN_VALUE;

    /**
     * Specify maximum allowed value for integer property. Default is unlimited.
     */
    long max() default Long.MAX_VALUE;

    /**
     * Constraint implementation for {@link IntegerConstraint}.
     */
    public static class Constraint {
        private final String mPropertyName;

        private final long mMinValue;
        private final long mMaxValue;

        /** Disallowed values, sorted for binary search. */
        private final long[] mDisallowed;

        /** Allowed values, sorted for binary search. */
        private final long[] mAllowed;

        /**
         * @param type type of object that contains the constrained property
         * @param propertyName name of property with constraint
         * @param ann specific annotation that binds to this constraint class
         */
        public Constraint(Class<?> type, String propertyName, IntegerConstraint ann) {
            this(type, propertyName,
                 ann.min(), ann.max(), ann.allowed(), ann.disallowed());
        }

        /**
         * @param type type of object that contains the constrained property
         * @param propertyName name of property with constraint
         * @param min minimum allowed value
         * @param max maximum allowed value
         * @param allowed optional set of allowed values
         * @param disallowed optional set of disallowed values
         */
        public Constraint(Class<?> type, String propertyName,
                          long min, long max, long[] allowed, long[] disallowed) {
            mPropertyName = propertyName;
            mMinValue = min;
            mMaxValue = max;
            if (mMaxValue < mMinValue) {
                throw new MalformedTypeException
                    (type, "Illegal range for integer constraint on property \"" +
                     propertyName + "\": " + rangeString());
            }

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
                for (long value : allowed) {
                    if (value < mMinValue || value > mMaxValue ||
                        (disallowed != null && Arrays.binarySearch(disallowed, value) >= 0)) {
                        throw new MalformedTypeException
                            (type, "Allowed value contradiction for integer constraint " +
                             "on property \"" + propertyName + "\": " + value);
                    }
                }

                // No need to have a set of disallowed values.
                disallowed = null;
            }

            mDisallowed = disallowed;
            mAllowed = allowed;
        }

        public void constrain(long propertyValue) {
            if (propertyValue < mMinValue || propertyValue > mMaxValue) {
                throw new IllegalArgumentException
                    ("Value for \"" + mPropertyName + "\" must be in range " +
                     rangeString() + ": " + propertyValue);
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

        public void constrain(double propertyValue) {
            if (propertyValue < mMinValue || propertyValue > mMaxValue) {
                throw new IllegalArgumentException
                    ("Value for \"" + mPropertyName + "\" must be in range " +
                     rangeString() + ": " + propertyValue);
            }
            if (mDisallowed != null) {
                long longValue = (long) propertyValue;
                if (longValue == propertyValue &&
                    Arrays.binarySearch(mDisallowed, longValue) >= 0) {

                    throw new IllegalArgumentException
                        ("Value for \"" + mPropertyName + "\" is disallowed: " + propertyValue);
                }
            }
            if (mAllowed != null) {
                long longValue = (long) propertyValue;
                if (longValue != propertyValue ||
                    Arrays.binarySearch(mAllowed, longValue) < 0) {

                    throw new IllegalArgumentException
                        ("Value for \"" + mPropertyName + "\" is not allowed: " + propertyValue);
                }
            }
        }

        public void constrain(CharSequence propertyValue) {
            if (propertyValue != null) {
                try {
                    constrain(Long.parseLong(propertyValue.toString()));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException
                        ("Value for \"" + mPropertyName + "\" is not an integer: " +
                         propertyValue);
                }
            }
        }

        public void constrain(char propertyValue) {
            if ('0' <= propertyValue && propertyValue <= '9') {
                constrain((long) (propertyValue - '0'));
            } else {
                throw new IllegalArgumentException
                    ("Value for \"" + mPropertyName + "\" is not an integer: " + propertyValue);
            }
        }

        public void constrain(char[] propertyValue) {
            if (propertyValue != null) {
                constrain(new String(propertyValue));
        }
        }

        private String rangeString() {
            StringBuilder b = new StringBuilder();
            b.append('(');
            if (mMinValue > Long.MIN_VALUE) {
                b.append(mMinValue);
            }
            b.append("..");
            if (mMaxValue < Long.MAX_VALUE) {
                b.append(mMaxValue);
            }
            b.append(')');
            return b.toString();
        }
    }
}
