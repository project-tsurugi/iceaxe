/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.result;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Entity for name access.
 *
 * @since 1.5.0
 */
public interface TsurugiResultNameEntity {

    /**
     * get value.
     *
     * @param name column name
     * @return value
     */
    public @Nullable Object getValueOrNull(String name);

    /**
     * get convert utility.
     *
     * @return convert utility
     */
    public IceaxeConvertUtil getConvertUtil();

    // boolean

    /**
     * get value as boolean.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default boolean getBoolean(String name) {
        var value = getBooleanOrNull(name);
        return Objects.requireNonNull(value, () -> "getBoolean(" + name + ") is null");
    }

    /**
     * get value as boolean.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default boolean getBoolean(String name, boolean defaultValue) {
        var value = getBooleanOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as boolean.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<Boolean> findBoolean(String name) {
        var value = getBooleanOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as boolean.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable Boolean getBooleanOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toBoolean(value);
    }

    // int

    /**
     * get value as int.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default int getInt(String name) {
        var value = getIntOrNull(name);
        return Objects.requireNonNull(value, () -> "getInt(" + name + ") is null");
    }

    /**
     * get value as int.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default int getInt(String name, int defaultValue) {
        var value = getIntOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as int.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<Integer> findInt(String name) {
        var value = getIntOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as int.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable Integer getIntOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toInt(value);
    }

    // long

    /**
     * get value as long.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default long getLong(String name) {
        var value = getLongOrNull(name);
        return Objects.requireNonNull(value, () -> "getLong(" + name + ") is null");
    }

    /**
     * get value as long.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default long getLong(String name, long defaultValue) {
        var value = getLongOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as long.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<Long> findLong(String name) {
        var value = getLongOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as long.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable Long getLongOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toLong(value);
    }

    // float

    /**
     * get value as float.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default float getFloat(String name) {
        var value = getFloatOrNull(name);
        return Objects.requireNonNull(value, () -> "getFloat(" + name + ") is null");
    }

    /**
     * get value as float.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default float getFloat(String name, float defaultValue) {
        var value = getFloatOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as float.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<Float> findFloat(String name) {
        var value = getFloatOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as float.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable Float getFloatOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toFloat(value);
    }

    // double

    /**
     * get value as double.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default double getDouble(String name) {
        var value = getDoubleOrNull(name);
        return Objects.requireNonNull(value, () -> "getDouble(" + name + ") is null");
    }

    /**
     * get value as double.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default double getDouble(String name, double defaultValue) {
        var value = getDoubleOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as double.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<Double> findDouble(String name) {
        var value = getDoubleOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as double.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable Double getDoubleOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toDouble(value);
    }

    // decimal

    /**
     * get value as decimal.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull BigDecimal getDecimal(String name) {
        var value = getDecimalOrNull(name);
        return Objects.requireNonNull(value, () -> "getDecimal(" + name + ") is null");
    }

    /**
     * get value as decimal.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default BigDecimal getDecimal(String name, BigDecimal defaultValue) {
        var value = getDecimalOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as decimal.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<BigDecimal> findDecimal(String name) {
        var value = getDecimalOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as decimal.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable BigDecimal getDecimalOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toDecimal(value);
    }

    // string

    /**
     * get value as string.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull String getString(String name) {
        var value = getStringOrNull(name);
        return Objects.requireNonNull(value, () -> "getString(" + name + ") is null");
    }

    /**
     * get value as string.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default String getString(String name, String defaultValue) {
        var value = getStringOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as string.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<String> findString(String name) {
        var value = getStringOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as string.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable String getStringOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toString(value);
    }

    // byte[]

    /**
     * get value as byte[].
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull byte[] getBytes(String name) {
        var value = getBytesOrNull(name);
        return Objects.requireNonNull(value, () -> "getBytes(" + name + ") is null");
    }

    /**
     * get value as byte[].
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default byte[] getBytes(String name, byte[] defaultValue) {
        var value = getBytesOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as byte[].
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<byte[]> findBytes(String name) {
        var value = getBytesOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as byte[].
     *
     * @param name column name
     * @return value
     */
    public default @Nullable byte[] getBytesOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toBytes(value);
    }

    // boolean[]

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull boolean[] getBits(String name) {
        var value = getBitsOrNull(name);
        return Objects.requireNonNull(value, () -> "getBits(" + name + ") is null");
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default boolean[] getBits(String name, boolean[] defaultValue) {
        var value = getBitsOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<boolean[]> findBits(String name) {
        var value = getBitsOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param name column name
     * @return value
     */
    public default @Nullable boolean[] getBitsOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toBits(value);
    }

    // date

    /**
     * get value as date.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull LocalDate getDate(String name) {
        var value = getDateOrNull(name);
        return Objects.requireNonNull(value, () -> "getDate(" + name + ") is null");
    }

    /**
     * get value as date.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default LocalDate getDate(String name, LocalDate defaultValue) {
        var value = getDateOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as date.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<LocalDate> findDate(String name) {
        var value = getDateOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as date.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable LocalDate getDateOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toDate(value);
    }

    // time

    /**
     * get value as time.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull LocalTime getTime(String name) {
        var value = getTimeOrNull(name);
        return Objects.requireNonNull(value, () -> "getTime(" + name + ") is null");
    }

    /**
     * get value as time.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default LocalTime getTime(String name, LocalTime defaultValue) {
        var value = getTimeOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as time.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<LocalTime> findTime(String name) {
        var value = getTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as time.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable LocalTime getTimeOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toTime(value);
    }

    // dateTime

    /**
     * get value as dateTime.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull LocalDateTime getDateTime(String name) {
        var value = getDateTimeOrNull(name);
        return Objects.requireNonNull(value, () -> "getDateTime(" + name + ") is null");
    }

    /**
     * get value as dateTime.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default LocalDateTime getDateTime(String name, LocalDateTime defaultValue) {
        var value = getDateTimeOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as dateTime.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<LocalDateTime> findDateTime(String name) {
        var value = getDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as dateTime.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable LocalDateTime getDateTimeOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toDateTime(value);
    }

    // offset time

    /**
     * get value as offset time.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull OffsetTime getOffsetTime(String name) {
        var value = getOffsetTimeOrNull(name);
        return Objects.requireNonNull(value, () -> "getOffsetTime(" + name + ") is null");
    }

    /**
     * get value as offset time.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default OffsetTime getOffsetTime(String name, OffsetTime defaultValue) {
        var value = getOffsetTimeOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as offset time.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<OffsetTime> findOffsetTime(String name) {
        var value = getOffsetTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as offset time.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable OffsetTime getOffsetTimeOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toOffsetTime(value);
    }

    // offset dateTime

    /**
     * get value as offset dateTime.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull OffsetDateTime getOffsetDateTime(String name) {
        var value = getOffsetDateTimeOrNull(name);
        return Objects.requireNonNull(value, () -> "getOffsetDateTime(" + name + ") is null");
    }

    /**
     * get value as offset dateTime.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default OffsetDateTime getOffsetDateTime(String name, OffsetDateTime defaultValue) {
        var value = getOffsetDateTimeOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as offset dateTime.
     *
     * @param name column name
     * @return value
     */
    public default @Nonnull Optional<OffsetDateTime> findOffsetDateTime(String name) {
        var value = getOffsetDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as offset dateTime.
     *
     * @param name column name
     * @return value
     */
    public default @Nullable OffsetDateTime getOffsetDateTimeOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toOffsetDateTime(value);
    }

    // zoned dateTime

    /**
     * get value as zoned dateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull ZonedDateTime getZonedDateTime(String name, @Nonnull ZoneId zone) {
        var value = getZonedDateTimeOrNull(name, zone);
        return Objects.requireNonNull(value, () -> "getZonedDateTime(" + name + ") is null");
    }

    /**
     * get value as zoned dateTime.
     *
     * @param name         column name
     * @param zone         time-zone
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default ZonedDateTime getZonedDateTime(String name, @Nonnull ZoneId zone, ZonedDateTime defaultValue) {
        var value = getZonedDateTimeOrNull(name, zone);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as zoned dateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return value
     */
    public default @Nonnull Optional<ZonedDateTime> findZonedDateTime(String name, @Nonnull ZoneId zone) {
        var value = getZonedDateTimeOrNull(name, zone);
        return Optional.ofNullable(value);
    }

    /**
     * get value as zoned dateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return value
     */
    public default @Nullable ZonedDateTime getZonedDateTimeOrNull(String name, @Nonnull ZoneId zone) {
        var value = getValueOrNull(name);
        return getConvertUtil().toZonedDateTime(value, zone);
    }

    // BLOB

    /**
     * get value as BLOB.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     * @since X.X.X
     */
    public default @Nonnull TgBlob getBlob(String name) {
        var value = getBlobOrNull(name);
        return Objects.requireNonNull(value, () -> "getBlob(" + name + ") is null");
    }

    /**
     * get value as BLOB.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     * @since X.X.X
     */
    public default TgBlob getBlob(String name, TgBlob defaultValue) {
        var value = getBlobOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as BLOB.
     *
     * @param name column name
     * @return value
     * @since X.X.X
     */
    public default @Nonnull Optional<TgBlob> findBlob(String name) {
        var value = getBlobOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as BLOB.
     *
     * @param name column name
     * @return value
     * @since X.X.X
     */
    public default @Nullable TgBlob getBlobOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toBlob(value);
    }

    // CLOB

    /**
     * get value as CLOB.
     *
     * @param name column name
     * @return value
     * @throws NullPointerException if value is null
     * @since X.X.X
     */
    public default @Nonnull TgClob getClob(String name) {
        var value = getClobOrNull(name);
        return Objects.requireNonNull(value, () -> "getClob(" + name + ") is null");
    }

    /**
     * get value as CLOB.
     *
     * @param name         column name
     * @param defaultValue value to return if original value is null
     * @return value
     * @since X.X.X
     */
    public default TgClob getClob(String name, TgClob defaultValue) {
        var value = getClobOrNull(name);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as CLOB.
     *
     * @param name column name
     * @return value
     * @since X.X.X
     */
    public default @Nonnull Optional<TgClob> findClob(String name) {
        var value = getClobOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get value as CLOB.
     *
     * @param name column name
     * @return value
     * @since X.X.X
     */
    public default @Nullable TgClob getClobOrNull(String name) {
        var value = getValueOrNull(name);
        return getConvertUtil().toClob(value);
    }
}
