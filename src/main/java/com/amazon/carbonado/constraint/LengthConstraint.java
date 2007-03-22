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

import com.amazon.carbonado.MalformedTypeException;

/**
 * Limits the value of a property to lie within a specific length range. The
 * property value may be a String, CharSequence, or any kind of array. If the
 * set property length is outside the range, an IllegalArgumentException is
 * thrown.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     String getFirstName();
 *     <b>&#64;LengthConstraint(min=1, max=50)</b>
 *     void setFirstName(String name);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ConstraintDefinition
public @interface LengthConstraint {
    /**
     * Specify minimum allowed length for property. Default is zero.
     */
    int min() default 0;

    /**
     * Specify maximum allowed length for property. Default is unlimited.
     */
    int max() default Integer.MAX_VALUE;

    /**
     * Constraint implementation for {@link LengthConstraint}.
     */
    public static class Constraint {
        private final String mPropertyName;
        private final int mMinLength;
        private final int mMaxLength;

        /**
         * @param type type of object that contains the constrained property
         * @param propertyName name of property with constraint
         * @param ann specific annotation that binds to this constraint class
         */
        public Constraint(Class<?> type, String propertyName, LengthConstraint ann) {
            this(type, propertyName, ann.min(), ann.max());
        }

        /**
         * @param type type of object that contains the constrained property
         * @param propertyName name of property with constraint
         * @param min minimum allowed length
         * @param max maximum allowed length
         */
        public Constraint(Class<?> type, String propertyName, int min, int max) {
            mPropertyName = propertyName;
            mMinLength = min;
            mMaxLength = max;
            if (mMinLength < 0 || mMaxLength < mMinLength) {
                throw new MalformedTypeException
                    (type, "Illegal length constraint for property \"" + propertyName +
                     "\": " + rangeString());
            }
        }

        public void constrain(CharSequence str) {
            if (str != null) {
                constrainLength(str.length());
            }
        }

        public void constrain(boolean[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(byte[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(short[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(char[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(int[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(long[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(float[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(double[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        public void constrain(Object[] array) {
            if (array != null) {
                constrainLength(array.length);
            }
        }

        private void constrainLength(int length) {
            if (length < mMinLength || length > mMaxLength) {
                throw new IllegalArgumentException
                    ("Value length for \"" + mPropertyName + "\" must be in range " +
                     rangeString() + ": " + length);
            }
        }

        private String rangeString() {
            StringBuilder b = new StringBuilder();
            b.append('(');
            b.append(mMinLength);
            b.append("..");
            if (mMaxLength < Integer.MAX_VALUE) {
                b.append(mMaxLength);
            }
            b.append(')');
            return b.toString();
        }
    }
}
