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

package com.amazon.carbonado.adapter;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Adapter that converts 'Y' or 'N' to and from a boolean value.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     <b>&#64;YesNoAdapter</b>
 *     boolean isAdministrator();
 *     void setAdministrator(boolean admin);
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @see TrueFalseAdapter
 * @see AdapterDefinition
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@AdapterDefinition(storageTypePreferences={
    char.class,
    Character.class,
    String.class
})
public @interface YesNoAdapter {
    /**
     * Specify that this adapter should be lenient when converting characters
     * into booleans. By default it is true, and it accepts the following as
     * true: 'Y', 'y', 'T', 't', '1'. For false: 'N', 'n', 'F', 'f', '0'. When
     * lenient is false, only 'Y' and 'N' are accepted.
     */
    boolean lenient() default true;

    /**
     * Adapter implementation for {@link YesNoAdapter}.
     */
    public static class Adapter {
        private final String mPropertyName;
        private final boolean mLenient;

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with adapter
         * @param ann specific annotation that binds to this adapter class
         */
        public Adapter(Class<?> type, String propertyName, YesNoAdapter ann) {
            this(type, propertyName, ann == null ? true : ann.lenient());
        }

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with
         * @param lenient lenient when true
         */
        public Adapter(Class<?> type, String propertyName, boolean lenient) {
            mPropertyName = propertyName;
            mLenient = lenient;
        }

        /**
         * Adapts a boolean true or false into 'Y' or 'N'.
         */
        public char adaptToChar(boolean value) {
            return value ? 'Y' : 'N';
        }

        /**
         * Adapts a boolean true into 'Y', and anything else to 'N'.
         */
        public char adaptToChar(Boolean value) {
            return (value != null && value) ? 'Y' : 'N';
        }

        /**
         * Adapts a boolean true or false into 'Y' or 'N'.
         */
        public Character adaptToCharacter(boolean value) {
            return value ? 'Y' : 'N';
        }

        /**
         * Adapts a boolean true into 'Y', and anything else to 'N'.
         */
        public Character adaptToCharacter(Boolean value) {
            return (value != null && value) ? 'Y' : 'N';
        }

        /**
         * Adapts a boolean true or false into "Y" or "N".
         */
        public String adaptToString(boolean value) {
            return value ? "Y" : "N";
        }

        /**
         * Adapts a boolean true into "Y", and anything else to "N".
         */
        public String adaptToString(Boolean value) {
            return (value != null && value) ? "Y" : "N";
        }

        /**
         * Adapts a character 'Y' or 'N' to true or false. If {@link
         * YesNoAdapter#lenient lenient}, other characters are accepted as
         * well.
         */
        public boolean adaptToBoolean(char value) {
            switch (value) {
            case 'Y':
                return true;
            case 'N':
                return false;

            case 'y': case 'T': case 't': case '1':
                if (mLenient) {
                    return true;
                }
                break;

            case 'n': case 'F': case 'f': case '0':
                if (mLenient) {
                    return false;
                }
                break;
            }

            throw new IllegalArgumentException
                ("Cannot adapt '" + value + "' into boolean for property \"" +
                 mPropertyName + '"');
        }

        /**
         * Adapts a character 'Y' or 'N' to true or false. If {@link
         * YesNoAdapter#lenient lenient}, other characters are accepted as
         * well.
         */
        public boolean adaptToBoolean(Character value) {
            if (value == null) {
                if (mLenient) {
                    return false;
                }
            } else {
                return adaptToBoolean(value.charValue());
            }

            throw new IllegalArgumentException
                ("Cannot adapt '" + value + "' into boolean for property \"" +
                 mPropertyName + '"');
        }

        /**
         * Adapts a character "Y" or "N" to true or false. If {@link
         * YesNoAdapter#lenient lenient}, other characters are accepted as
         * well.
         */
        public boolean adaptToBoolean(String value) {
            int length;
            if (value == null || (length = value.length()) == 0) {
                if (mLenient) {
                    return false;
                }
            } else if (length == 1 || mLenient) {
                return adaptToBoolean(value.charAt(0));
            }

            throw new IllegalArgumentException
                ("Cannot adapt '" + value + "' into boolean for property \"" +
                 mPropertyName + '"');
        }

        /**
         * Adapts a character 'Y' or 'N' to true or false. If {@link
         * YesNoAdapter#lenient lenient}, other characters are accepted as
         * well.
         */
        public Boolean adaptToBooleanObj(char value) {
            return adaptToBoolean(value);
        }

        /**
         * Adapts a character 'Y' or 'N' to true or false. If {@link
         * YesNoAdapter#lenient lenient}, other characters are accepted as
         * well.
         */
        public Boolean adaptToBooleanObj(Character value) {
            return adaptToBoolean(value);
        }

        /**
         * Adapts a character "Y" or "N" to true or false. If {@link
         * YesNoAdapter#lenient lenient}, other characters are accepted as
         * well.
         */
        public Boolean adaptToBooleanObj(String value) {
            return adaptToBoolean(value);
        }
    }
}
