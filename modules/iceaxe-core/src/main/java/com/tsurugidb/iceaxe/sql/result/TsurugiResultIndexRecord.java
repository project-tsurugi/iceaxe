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
package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;
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

import com.tsurugidb.iceaxe.sql.type.TgBlobReference;
import com.tsurugidb.iceaxe.sql.type.TgClobReference;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Record for index access.
 *
 * @since 1.5.0
 */
public interface TsurugiResultIndexRecord {

    /**
     * get value.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public @Nullable Object getValueOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException;

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
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default boolean getBoolean(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBooleanOrNull(index);
        return Objects.requireNonNull(value, () -> "getBoolean(" + index + ") is null");
    }

    /**
     * get value as boolean.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default boolean getBoolean(int index, boolean defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBooleanOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as boolean.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Boolean> findBoolean(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBooleanOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as boolean.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Boolean getBooleanOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toBoolean(value);
    }

    // int

    /**
     * get value as int.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default int getInt(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getIntOrNull(index);
        return Objects.requireNonNull(value, () -> "getInt(" + index + ") is null");
    }

    /**
     * get value as int.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default int getInt(int index, int defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getIntOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as int.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Integer> findInt(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getIntOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as int.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Integer getIntOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toInt(value);
    }

    // long

    /**
     * get value as long.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default long getLong(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getLongOrNull(index);
        return Objects.requireNonNull(value, () -> "getLong(" + index + ") is null");
    }

    /**
     * get value as long.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default long getLong(int index, long defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getLongOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as long.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Long> findLong(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getLongOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as long.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Long getLongOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toLong(value);
    }

    // float

    /**
     * get value as float.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default float getFloat(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getFloatOrNull(index);
        return Objects.requireNonNull(value, () -> "getFloat(" + index + ") is null");
    }

    /**
     * get value as float.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default float getFloat(int index, float defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getFloatOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as float.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Float> findFloat(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getFloatOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as float.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Float getFloatOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toFloat(value);
    }

    // double

    /**
     * get value as double.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default double getDouble(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDoubleOrNull(index);
        return Objects.requireNonNull(value, () -> "getDouble(" + index + ") is null");
    }

    /**
     * get value as double.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default double getDouble(int index, double defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDoubleOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as double.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Double> findDouble(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDoubleOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as double.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Double getDoubleOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toDouble(value);
    }

    // decimal

    /**
     * get value as decimal.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull BigDecimal getDecimal(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDecimalOrNull(index);
        return Objects.requireNonNull(value, () -> "getDecimal(" + index + ") is null");
    }

    /**
     * get value as decimal.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default BigDecimal getDecimal(int index, BigDecimal defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDecimalOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as decimal.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<BigDecimal> findDecimal(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDecimalOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as decimal.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable BigDecimal getDecimalOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toDecimal(value);
    }

    // string

    /**
     * get value as string.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull String getString(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getStringOrNull(index);
        return Objects.requireNonNull(value, () -> "getString(" + index + ") is null");
    }

    /**
     * get value as string.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default String getString(int index, String defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getStringOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as string.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<String> findString(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getStringOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as string.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable String getStringOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toString(value);
    }

    // byte[]

    /**
     * get value as byte[].
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull byte[] getBytes(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBytesOrNull(index);
        return Objects.requireNonNull(value, () -> "getBytes(" + index + ") is null");
    }

    /**
     * get value as byte[].
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default byte[] getBytes(int index, byte[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBytesOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as byte[].
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<byte[]> findBytes(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBytesOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as byte[].
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable byte[] getBytesOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toBytes(value);
    }

    // boolean[]

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull boolean[] getBits(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBitsOrNull(index);
        return Objects.requireNonNull(value, () -> "getBits(" + index + ") is null");
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default boolean[] getBits(int index, boolean[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBitsOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<boolean[]> findBits(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBitsOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get value as boolean[].
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable boolean[] getBitsOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toBits(value);
    }

    // date

    /**
     * get value as date.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull LocalDate getDate(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateOrNull(index);
        return Objects.requireNonNull(value, () -> "getDate(" + index + ") is null");
    }

    /**
     * get value as date.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default LocalDate getDate(int index, LocalDate defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as date.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<LocalDate> findDate(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as date.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable LocalDate getDateOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toDate(value);
    }

    // time

    /**
     * get value as time.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull LocalTime getTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getTime(" + index + ") is null");
    }

    /**
     * get value as time.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default LocalTime getTime(int index, LocalTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as time.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<LocalTime> findTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as time.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable LocalTime getTimeOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toTime(value);
    }

    // dateTime

    /**
     * get value as dateTime.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull LocalDateTime getDateTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getDateTime(" + index + ") is null");
    }

    /**
     * get value as dateTime.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default LocalDateTime getDateTime(int index, LocalDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as dateTime.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<LocalDateTime> findDateTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as dateTime.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable LocalDateTime getDateTimeOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toDateTime(value);
    }

    // offset time

    /**
     * get value as offset time.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull OffsetTime getOffsetTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getOffsetTime(" + index + ") is null");
    }

    /**
     * get value as offset time.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default OffsetTime getOffsetTime(int index, OffsetTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as offset time.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<OffsetTime> findOffsetTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as offset time.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable OffsetTime getOffsetTimeOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toOffsetTime(value);
    }

    // offset dateTime

    /**
     * get value as offset dateTime.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull OffsetDateTime getOffsetDateTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getOffsetDateTime(" + index + ") is null");
    }

    /**
     * get value as offset dateTime.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default OffsetDateTime getOffsetDateTime(int index, OffsetDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as offset dateTime.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<OffsetDateTime> findOffsetDateTime(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as offset dateTime.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable OffsetDateTime getOffsetDateTimeOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toOffsetDateTime(value);
    }

    // zoned dateTime

    /**
     * get value as zoned dateTime.
     *
     * @param index column index
     * @param zone  time-zone
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull ZonedDateTime getZonedDateTime(int index, @Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getZonedDateTimeOrNull(index, zone);
        return Objects.requireNonNull(value, () -> "getZonedDateTime(" + index + ") is null");
    }

    /**
     * get value as zoned dateTime.
     *
     * @param index        column index
     * @param zone         time-zone
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default ZonedDateTime getZonedDateTime(int index, @Nonnull ZoneId zone, ZonedDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getZonedDateTimeOrNull(index, zone);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as zoned dateTime.
     *
     * @param index column index
     * @param zone  time-zone
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<ZonedDateTime> findZonedDateTime(int index, @Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getZonedDateTimeOrNull(index, zone);
        return Optional.ofNullable(value);
    }

    /**
     * get value as zoned dateTime.
     *
     * @param index column index
     * @param zone  time-zone
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable ZonedDateTime getZonedDateTimeOrNull(int index, @Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toZonedDateTime(value, zone);
    }

    // BLOB

    /**
     * get value as BLOB.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     * @since 1.8.0
     */
    public default @Nonnull TgBlobReference getBlob(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBlobOrNull(index);
        return Objects.requireNonNull(value, () -> "getBlob(" + index + ") is null");
    }

    /**
     * get value as BLOB.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.8.0
     */
    public default TgBlobReference getBlob(int index, TgBlobReference defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBlobOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as BLOB.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.8.0
     */
    public default @Nonnull Optional<TgBlobReference> findBlob(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBlobOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as BLOB.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.8.0
     */
    public default @Nullable TgBlobReference getBlobOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toBlobReference(value);
    }

    // CLOB

    /**
     * get value as CLOB.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     * @since 1.8.0
     */
    public default @Nonnull TgClobReference getClob(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getClobOrNull(index);
        return Objects.requireNonNull(value, () -> "getClob(" + index + ") is null");
    }

    /**
     * get value as CLOB.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.8.0
     */
    public default TgClobReference getClob(int index, TgClobReference defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getClobOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as CLOB.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.8.0
     */
    public default @Nonnull Optional<TgClobReference> findClob(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getClobOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as CLOB.
     *
     * @param index column index
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.8.0
     */
    public default @Nullable TgClobReference getClobOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getValueOrNull(index);
        return getConvertUtil().toClobReference(value);
    }
}
