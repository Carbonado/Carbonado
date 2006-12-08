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
 * property value may be a boxed or unboxed float, double, String,
 * CharSequence, char, Character, or character array. If the property value is
 * outside the set, an IllegalArgumentException is thrown.
 *
 * <p>Example:<pre>
 * public interface PolarCoordinate extends Storable {
 *     double getTheta();
 *     <b>&#64;FloatConstraint(min=0, max=Math.PI * 2, disallowed=Double.NaN)</b>
 *     void setTheta(double radians);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see IntegerConstraint
 * @see TextConstraint
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ConstraintDefinition
public @interface FloatConstraint {
    /**
     * Specific allowed values for property. Default is unlimited.
     */
    double[] allowed() default {};

    /**
     * Specific disallowed values for property. Default is none.
     */
    double[] disallowed() default {};

    /**
     * Specify minimum allowed value for float/double property. Default is unlimited.
     */
    double min() default Double.NEGATIVE_INFINITY;

    /**
     * Specify maximum allowed value for float/double property. Default is unlimited.
     */
    double max() default Double.POSITIVE_INFINITY;

    /**
     * Constraint implementation for {@link FloatConstraint}.
     */
    public static class Constraint {
        private final String mPropertyName;
        private final double mMinValue;
        private final double mMaxValue;

        /** Disallowed values, sorted for binary search. */
        private final double[] mDisallowed;

        /** Allowed values, sorted for binary search. */
        private final double[] mAllowed;

        /**
         * @param type type of object that contains the constrained property
         * @param propertyName name of property with constraint
         * @param ann specific annotation that binds to this constraint class
         */
        public Constraint(Class<?> type, String propertyName, FloatConstraint ann) {
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
                          double min, double max, double[] allowed, double[] disallowed) {
            mPropertyName = propertyName;
            mMinValue = min;
            mMaxValue = max;
            if (mMaxValue < mMinValue) {
                throw new MalformedTypeException
                    (type, "Illegal range for float constraint on property \"" +
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
                for (double value : allowed) {
                    if (value < mMinValue || value > mMaxValue ||
                        (disallowed != null && Arrays.binarySearch(disallowed, value) >= 0)) {
                        throw new MalformedTypeException
                            (type, "Allowed value contradiction for float constraint " +
                             "on property \"" + propertyName + "\": " + value);
                    }
                }

                // No need to have a set of disallowed values.
                disallowed = null;
            }

            mDisallowed = disallowed;
            mAllowed = allowed;
        }

        public void constrain(double propertyValue) {
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

        public void constrain(CharSequence propertyValue) {
            if (propertyValue != null) {
                try {
                    constrain(Double.parseDouble(propertyValue.toString()));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException
                        ("Value for \"" + mPropertyName + "\" is not a number: " +
                         propertyValue);
                }
            }
        }

        public void constrain(char propertyValue) {
            if ('0' <= propertyValue && propertyValue <= '9') {
                constrain((double) (propertyValue - '0'));
            } else {
                throw new IllegalArgumentException
                    ("Value for \"" + mPropertyName + "\" is not a number: " + propertyValue);
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
            if (mMinValue > Double.MIN_VALUE) {
                b.append(mMinValue);
            }
            b.append("..");
            if (mMaxValue < Double.MAX_VALUE) {
                b.append(mMaxValue);
            }
            b.append(')');
            return b.toString();
        }
    }
}
