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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;

import com.amazon.carbonado.adapter.AdapterDefinition;

/**
 * Converts Joda-Time datetime objects to and from other forms. This adapter is
 * applied automatically for all storable properties of type {@link DateTime}
 * or {@link DateMidnight}. Explicit use allows a different time zone to be
 * used. DateTimeAdapter can also be used to support {@link Date} properties,
 * but it must be explicitly specified.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     <b>&#64;DateTimeAdapter(timeZone="UTC")</b>
 *     DateTime getModifyDateTime();
 *
 *     ...
 * }
 * </pre>
 *
 * @author Brian S O'Neill
 * @author Justin Rudd
 * @see AdapterDefinition
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@AdapterDefinition(storageTypePreferences={
    long.class,
    Long.class,
    String.class
})
public @interface DateTimeAdapter {
    /**
     * Optionally specify a time zone to apply to new DateTimes, overriding the
     * default time zone.
     */
    String timeZone() default "";

    /**
     * Adapter implementation for {@link DateTimeAdapter}.
     */
    public static class Adapter {
        private static DateTimeZone toDateTimeZone(DateTimeAdapter ann) {
            String id;
            if (ann == null || (id = ann.timeZone()) == null || id.length() == 0) {
                return null;
            }
            return DateTimeZone.forID(id);
        }

        private final String mPropertyName;
        private final DateTimeZone mZone;

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with adapter
         * @param ann specific annotation that binds to this adapter class
         */
        public Adapter(Class<?> type, String propertyName, DateTimeAdapter ann) {
            this(type, propertyName, toDateTimeZone(ann));
        }

        /**
         * @param type type of object that contains the adapted property
         * @param propertyName name of property with
         * @param zone time zone to apply, or null to use default
         */
        public Adapter(Class<?> type, String propertyName, DateTimeZone zone) {
            mPropertyName = propertyName;
            mZone = zone;
        }

        // Adapt to DateTime...

        public DateTime adaptToDateTime(long instant) {
            return new DateTime(instant, mZone);
        }

        public DateTime adaptToDateTime(Long instant) {
            return instant == null ? null : new DateTime(instant, mZone);
        }

        public DateTime adaptToDateTime(String isoDateString) {
            return isoDateString == null ? null : new DateTime(isoDateString, mZone);
        }

        public DateTime adaptToDateTime(Date date) {
            return date == null ? null : new DateTime(date, mZone);
        }

        public DateMidnight adaptToDateMidnight(long instant) {
            return new DateMidnight(instant, mZone);
        }

        public DateMidnight adaptToDateMidnight(Long instant) {
            return instant == null ? null : new DateMidnight(instant, mZone);
        }

        public DateMidnight adaptToDateMidnight(String isoDateString) {
            return isoDateString == null ? null : new DateMidnight(isoDateString, mZone);
        }

        public DateMidnight adaptToDateMidnight(Date date) {
            return date == null ? null : new DateMidnight(date, mZone);
        }

        // Adapt from DateTime... (accept the more generic ReadableInstant)

        public long adaptToLong(ReadableInstant instant) {
            if (instant != null) {
                return instant.getMillis();
            }
            throw new IllegalArgumentException
                ("Cannot adapt null DateTime into long for property \"" +
                 mPropertyName + '"');
        }

        public Long adaptToLongObj(ReadableInstant instant) {
            return instant == null ? null : instant.getMillis();
        }

        public String adaptToString(ReadableInstant instant) {
            return instant == null ? null : instant.toString();
        }

        public Date adaptToDate(ReadableInstant instant) {
            return instant == null ? null : new Date(instant.getMillis());
        }

        public java.sql.Date adaptToSqlDate(ReadableInstant instant) {
            return instant == null ? null : new java.sql.Date(instant.getMillis());
        }

        public Time adaptToSqlTime(ReadableInstant instant) {
            return instant == null ? null : new Time(instant.getMillis());
        }

        public Timestamp adaptToSqlTimestamp(ReadableInstant instant) {
            return instant == null ? null : new Timestamp(instant.getMillis());
        }

        // Adapt to Date...

        public Date adaptToDate(long instant) {
            return new Date(instant);
        }

        public Date adaptToDate(Long instant) {
            return instant == null ? null : new Date(instant);
        }

        public Date adaptToDate(String isoDateString) {
            return isoDateString == null ? null : new DateTime(isoDateString, mZone).toDate();
        }

        // Adapt from Date...

        public long adaptToLong(Date date) {
            if (date != null) {
                return date.getTime();
            }
            throw new IllegalArgumentException
                ("Cannot adapt null Date into long for property \"" +
                 mPropertyName + '"');
        }

        public Long adaptToLongObj(Date date) {
            return date == null ? null : date.getTime();
        }

        public String adaptToString(Date date) {
            return date == null ? null : new DateTime(date, mZone).toString();
        }

        public java.sql.Date adaptToSqlDate(Date date) {
            return date == null ? null : new java.sql.Date(date.getTime());
        }

        public Time adaptToSqlTime(Date date) {
            return date == null ? null : new Time(date.getTime());
        }

        public Timestamp adaptToSqlTimestamp(Date date) {
            return date == null ? null : new Timestamp(date.getTime());
        }
    }
}
