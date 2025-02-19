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
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi Result Record for sequential access.
 *
 * @since 1.5.0
 */
public interface TsurugiResultNextRecord {

    /**
     * get current value and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public @Nullable Object nextValueOrNull() throws IOException, InterruptedException, TsurugiTransactionException;

    /**
     * get convert utility.
     *
     * @return convert utility
     */
    public IceaxeConvertUtil getConvertUtil();

    /**
     * get current column index.
     *
     * @return currenct olumn index
     */
    @IceaxeInternal
    /* protected */ int getCurrentColumnIndex();

    // boolean

    /**
     * get current value as boolean and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default boolean nextBoolean() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBooleanOrNull();
        return Objects.requireNonNull(value, () -> "nextBoolean(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as boolean and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default boolean nextBoolean(boolean defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBooleanOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as boolean and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Boolean> nextBooleanOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBooleanOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as boolean and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Boolean nextBooleanOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toBoolean(value);
    }

    // int

    /**
     * get current value as int and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default int nextInt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextIntOrNull();
        return Objects.requireNonNull(value, () -> "nextInt(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as int and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default int nextInt(int defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextIntOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as int and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Integer> nextIntOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextIntOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as int and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Integer nextIntOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toInt(value);
    }

    // long

    /**
     * get current value as long and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default long nextLong() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextLongOrNull();
        return Objects.requireNonNull(value, () -> "nextLong(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as long and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default long nextLong(long defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextLongOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as long and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Long> nextLongOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextLongOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as long and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Long nextLongOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toLong(value);
    }

    // float

    /**
     * get current value as float and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default float nextFloat() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        return Objects.requireNonNull(value, () -> "nextFloat(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as float and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default float nextFloat(float defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as float and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Float> nextFloatOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as float and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Float nextFloatOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toFloat(value);
    }

    // double

    /**
     * get current value as double and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default double nextDouble() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        return Objects.requireNonNull(value, () -> "nextDouble(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as double and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default double nextDouble(double defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as double and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<Double> nextDoubleOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as double and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable Double nextDoubleOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toDouble(value);
    }

    // decimal

    /**
     * get current value as decimal and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull BigDecimal nextDecimal() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDecimalOrNull();
        return Objects.requireNonNull(value, () -> "nextDecimal(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as decimal and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default BigDecimal nextDecimal(BigDecimal defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDecimalOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as decimal and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<BigDecimal> nextDecimalOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDecimalOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as decimal and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable BigDecimal nextDecimalOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toDecimal(value);
    }

    // string

    /**
     * get current value as string and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull String nextString() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextStringOrNull();
        return Objects.requireNonNull(value, () -> "nextString(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as string and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default String nextString(String defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextStringOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as string and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<String> nextStringOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextStringOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as string and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable String nextStringOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toString(value);
    }

    // byte[]

    /**
     * get current value as byte[] and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull byte[] nextBytes() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBytesOrNull();
        return Objects.requireNonNull(value, () -> "nextBytes(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as byte[] and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default byte[] nextBytes(byte[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBytesOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as byte[] and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<byte[]> nextBytesOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBytesOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as byte[] and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable byte[] nextBytesOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toBytes(value);
    }

    // boolean[]

    /**
     * <em>This method is not yet implemented:</em>
     * get current value as boolean[] and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull boolean[] nextBits() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBitsOrNull();
        return Objects.requireNonNull(value, () -> "nextBits(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get current value as boolean[] and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default boolean[] nextBits(boolean[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBitsOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get current value as boolean[] and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<boolean[]> nextBitsOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBitsOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * <em>This method is not yet implemented:</em>
     * get current value as boolean[] and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable boolean[] nextBitsOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toBits(value);
    }

    // date

    /**
     * get current value as date and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull LocalDate nextDate() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateOrNull();
        return Objects.requireNonNull(value, () -> "nextDate(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as date and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default LocalDate nextDate(LocalDate defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as date and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<LocalDate> nextDateOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as date and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable LocalDate nextDateOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toDate(value);
    }

    // time

    /**
     * get current value as time and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull LocalTime nextTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextTimeOrNull();
        return Objects.requireNonNull(value, () -> "nextTime(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as time and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default LocalTime nextTime(LocalTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextTimeOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as time and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<LocalTime> nextTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as time and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable LocalTime nextTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toTime(value);
    }

    // dateTime

    /**
     * get current value as dateTime and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull LocalDateTime nextDateTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        return Objects.requireNonNull(value, () -> "nextDateTime(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as dateTime and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default LocalDateTime nextDateTime(LocalDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as dateTime and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<LocalDateTime> nextDateTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as dateTime and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable LocalDateTime nextDateTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toDateTime(value);
    }

    // offset time

    /**
     * get current value as offset time and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull OffsetTime nextOffsetTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        return Objects.requireNonNull(value, () -> "nextOffsetTime(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as offset time and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default OffsetTime nextOffsetTime(OffsetTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as offset time and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<OffsetTime> nextOffsetTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as offset time and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable OffsetTime nextOffsetTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toOffsetTime(value);
    }

    // offset dateTime

    /**
     * get current value as offset dateTime and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull OffsetDateTime nextOffsetDateTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        return Objects.requireNonNull(value, () -> "nextOffsetDateTime(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as offset dateTime and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default OffsetDateTime nextOffsetDateTime(OffsetDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as offset dateTime and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<OffsetDateTime> nextOffsetDateTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as offset dateTime and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable OffsetDateTime nextOffsetDateTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toOffsetDateTime(value);
    }

    // zoned dateTime

    /**
     * get current value as zoned dateTime and move next column.
     *
     * @param zone time-zone
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     */
    public default @Nonnull ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextZonedDateTimeOrNull(zone);
        return Objects.requireNonNull(value, () -> "nextZonedDateTime(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as zoned dateTime and move next column.
     *
     * @param zone         time-zone
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone, ZonedDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextZonedDateTimeOrNull(zone);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as zoned dateTime and move next column.
     *
     * @param zone time-zone
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nonnull Optional<ZonedDateTime> nextZonedDateTimeOpt(@Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextZonedDateTimeOrNull(zone);
        return Optional.ofNullable(value);
    }

    /**
     * get current value as zoned dateTime and move next column.
     *
     * @param zone time-zone
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public default @Nullable ZonedDateTime nextZonedDateTimeOrNull(@Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toZonedDateTime(value, zone);
    }

    // BLOB

    /**
     * get current value as BLOB and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @throws NullPointerException        if value is null
     * @since X.X.X
     */
    public default @Nonnull TgBlobReference nextBlob() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBlobOrNull();
        return Objects.requireNonNull(value, () -> "nextBlob(" + getCurrentColumnIndex() + ") is null");
    }

    /**
     * get current value as BLOB and move next column.
     *
     * @param defaultValue value to return if original value is null
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since X.X.X
     */
    public default TgBlobReference nextBlob(TgBlobReference defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBlobOrNull();
        return (value != null) ? value : defaultValue;
    }

    /**
     * get current value as BLOB and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since X.X.X
     */
    public default @Nonnull Optional<TgBlobReference> nextBlobOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBlobOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current value as BLOB and move next column.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since X.X.X
     */
    public default @Nullable TgBlobReference nextBlobOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextValueOrNull();
        return getConvertUtil().toBlobReference(value);
    }
}
