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
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.ReadableInstant;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;

import com.amazon.carbonado.adapter.AdapterDefinition;

/**
 * Converts Joda-Time datetime objects to and from other forms. This adapter is
 * applied automatically for all storable properties of type {@link DateTime},
 * {@link DateMidnight}, {@link LocalDateTime}, {@link LocalDate} and also
 * {@link java.util.Date}. Explicit use allows a different time zone to be
 * used, but this only works for Joda-Time objects.
 *
 * <p>Example:<pre>
 * public interface UserInfo extends Storable {
 *     <b>&#64;DateTimeAdapter(timeZone="UTC")</b>
 *     DateTime getModifyDateTime();
 *     void setModifyDateTime(DateTime dt);
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
        private static final DateTimeFormatter cDateTimeParser;

        static {
            // Joda-time's date-time parser requires a 'T' separator, so need
            // to create a custom parser.

            DateTimeParser offset = new DateTimeFormatterBuilder()
                .appendTimeZoneOffset("Z", true, 2, 4)
                .toParser();

            DateTimeParser ttime = new DateTimeFormatterBuilder()
                .appendLiteral('T')
                .append(ISODateTimeFormat.timeElementParser().getParser())
                .appendOptional(offset)
                .toParser();
            
            DateTimeParser separator = new DateTimeFormatterBuilder()
                .append(null, new DateTimeParser[] {
                    new DateTimeFormatterBuilder()
                    .appendLiteral(' ')
                    .toParser(),
                    new DateTimeFormatterBuilder()
                    .appendLiteral('T')
                    .toParser()
                })
                .toParser();

            DateTimeParser separatedTimeOrOffset = new DateTimeFormatterBuilder()
                .append(separator)
                .appendOptional(ISODateTimeFormat.timeElementParser().getParser())
                .appendOptional(offset)
                .toParser();
            
            DateTimeParser dateOptionalTime = new DateTimeFormatterBuilder()
                .append(ISODateTimeFormat.dateElementParser().getParser())
                .appendOptional(separatedTimeOrOffset)
                .toParser();

            cDateTimeParser = new DateTimeFormatterBuilder()
                .append(null, new DateTimeParser[] {ttime, dateOptionalTime})
                .toFormatter();
        }

        private static DateTimeZone toDateTimeZone(DateTimeAdapter ann) {
            String id;
            if (ann == null || (id = ann.timeZone()) == null || id.length() == 0) {
                return null;
            }
            return DateTimeZone.forID(id);
        }

        private final String mPropertyName;
        private final DateTimeZone mZone;
        private final DateTimeFormatter mDateTimeParser;

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
            mDateTimeParser = cDateTimeParser.withZone(zone);
        }

        // Adapt to DateTime...

        public DateTime adaptToDateTime(long instant) {
            return new DateTime(instant, mZone);
        }

        public DateTime adaptToDateTime(Long instant) {
            return instant == null ? null : new DateTime(instant, mZone);
        }

        public DateTime adaptToDateTime(String isoDateString) {
            return isoDateString == null ? null : mDateTimeParser.parseDateTime(isoDateString);
        }

        public DateTime adaptToDateTime(Date date) {
            return date == null ? null : new DateTime(date, mZone);
        }

        public DateTime adaptToDateTime(java.sql.Date date) {
            return date == null ? null : new DateTime(date.getTime(), mZone);
        }

        public DateTime adaptToDateTime(Time time) {
            return time == null ? null : new DateTime(time.getTime(), mZone);
        }

        public DateTime adaptToDateTime(Timestamp timestamp) {
            return timestamp == null ? null : new DateTime(timestamp.getTime(), mZone);
        }

        // Adapt to DateMidnight...

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

        public DateMidnight adaptToDateMidnight(java.sql.Date date) {
            return date == null ? null : new DateMidnight(date.getTime(), mZone);
        }

        public DateMidnight adaptToDateMidnight(Time time) {
            return time == null ? null : new DateMidnight(time.getTime(), mZone);
        }

        public DateMidnight adaptToDateMidnight(Timestamp timestamp) {
            return timestamp == null ? null : new DateMidnight(timestamp.getTime(), mZone);
        }

        // Adapt to LocalDateTime...

        public LocalDateTime adaptToLocalDateTime(long instant) {
            return new LocalDateTime(instant, mZone);
        }

        public LocalDateTime adaptToLocalDateTime(Long instant) {
            return instant == null ? null : new LocalDateTime(instant, mZone);
        }

        public LocalDateTime adaptToLocalDateTime(String isoDateString) {
            return isoDateString == null ? null : new LocalDateTime(isoDateString, mZone);
        }

        public LocalDateTime adaptToLocalDateTime(Date date) {
            return date == null ? null : new LocalDateTime(date, mZone);
        }

        public LocalDateTime adaptToLocalDateTime(java.sql.Date date) {
            return date == null ? null : new LocalDateTime(date.getTime(), mZone);
        }

        public LocalDateTime adaptToLocalDateTime(Time time) {
            return time == null ? null : new LocalDateTime(time.getTime(), mZone);
        }

        public LocalDateTime adaptToLocalDateTime(Timestamp timestamp) {
            return timestamp == null ? null : new LocalDateTime(timestamp.getTime(), mZone);
        }

        // Adapt to LocalDate...

        public LocalDate adaptToLocalDate(long instant) {
            return new LocalDate(instant, mZone);
        }

        public LocalDate adaptToLocalDate(Long instant) {
            return instant == null ? null : new LocalDate(instant, mZone);
        }

        public LocalDate adaptToLocalDate(String isoDateString) {
            return isoDateString == null ? null : new LocalDate(isoDateString, mZone);
        }

        public LocalDate adaptToLocalDate(Date date) {
            return date == null ? null : new LocalDate(date, mZone);
        }

        public LocalDate adaptToLocalDate(java.sql.Date date) {
            return date == null ? null : new LocalDate(date.getTime(), mZone);
        }

        public LocalDate adaptToLocalDate(Time time) {
            return time == null ? null : new LocalDate(time.getTime(), mZone);
        }

        public LocalDate adaptToLocalDate(Timestamp timestamp) {
            return timestamp == null ? null : new LocalDate(timestamp.getTime(), mZone);
        }

        // Adapt from DateTime and DateMidnight... (accept the more generic ReadableInstant)

        public long adaptToLong(ReadableInstant instant) {
            if (instant != null) {
                return instant.getMillis();
            }
            throw new IllegalArgumentException
                ("Cannot adapt null instant into long for property \"" +
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

        // Adapt from LocalDateTime...

        public long adaptToLong(LocalDateTime dateTime) {
            if (dateTime != null) {
                return dateTime.toDateTime(mZone).getMillis();
            }
            throw new IllegalArgumentException
                ("Cannot adapt null datetime into long for property \"" +
                 mPropertyName + '"');
        }

        public Long adaptToLongObj(LocalDateTime dateTime) {
            return dateTime == null ? null : dateTime.toDateTime(mZone).getMillis();
        }

        public String adaptToString(LocalDateTime dateTime) {
            return dateTime == null ? null : dateTime.toString();
        }

        public Date adaptToDate(LocalDateTime dateTime) {
            return dateTime == null ? null : dateTime.toDateTime(mZone).toDate();
        }

        public java.sql.Date adaptToSqlDate(LocalDateTime dateTime) {
            return dateTime == null ? null
                : new java.sql.Date(dateTime.toDateTime(mZone).getMillis());
        }

        public Time adaptToSqlTime(LocalDateTime dateTime) {
            return dateTime == null ? null : new Time(dateTime.toDateTime(mZone).getMillis());
        }

        public Timestamp adaptToSqlTimestamp(LocalDateTime dateTime) {
            return dateTime == null ? null : new Timestamp(dateTime.toDateTime(mZone).getMillis());
        }

        // Adapt from LocalDate...

        public long adaptToLong(LocalDate date) {
            if (date != null) {
                return date.toDateMidnight(mZone).getMillis();
            }
            throw new IllegalArgumentException
                ("Cannot adapt null date into long for property \"" +
                 mPropertyName + '"');
        }

        public Long adaptToLongObj(LocalDate date) {
            return date == null ? null : date.toDateMidnight(mZone).getMillis();
        }

        public String adaptToString(LocalDate date) {
            return date == null ? null : date.toString();
        }

        public Date adaptToDate(LocalDate date) {
            return date == null ? null : date.toDateMidnight(mZone).toDate();
        }

        public java.sql.Date adaptToSqlDate(LocalDate date) {
            return date == null ? null : new java.sql.Date(date.toDateMidnight(mZone).getMillis());
        }

        public Timestamp adaptToSqlTimestamp(LocalDate date) {
            return date == null ? null : new Timestamp(date.toDateMidnight(mZone).getMillis());
        }

        // Adapt to Date...

        public Date adaptToDate(long instant) {
            return new Date(instant);
        }

        public Date adaptToDate(Long instant) {
            return instant == null ? null : new Date(instant);
        }

        public Date adaptToDate(String isoDateString) {
            return isoDateString == null ? null
                : mDateTimeParser.parseDateTime(isoDateString).toDate();
        }

        public Date adaptToDate(java.sql.Date date) {
            return date == null ? null : new Date(date.getTime());
        }

        public Date adaptToDate(Time time) {
            return time == null ? null : new Date(time.getTime());
        }

        public Date adaptToDate(Timestamp timestamp) {
            return timestamp == null ? null : new Date(timestamp.getTime());
        }

        // Adapt from Date...

        public long adaptToLong(Date date) {
            if (date != null) {
                return date.getTime();
            }
            throw new IllegalArgumentException
                ("Cannot adapt null date into long for property \"" +
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
