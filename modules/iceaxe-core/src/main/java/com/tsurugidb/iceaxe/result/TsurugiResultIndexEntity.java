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

import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Entity for index access.
 *
 * @since 1.5.0
 */
public interface TsurugiResultIndexEntity {

    /**
     * get value.
     *
     * @param index column index
     * @return value
     */
    public @Nullable Object getValueOrNull(int index);

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
     * @throws NullPointerException if value is null
     */
    public default boolean getBoolean(int index) {
        var value = getBooleanOrNull(index);
        return Objects.requireNonNull(value, () -> "getBoolean(" + index + ") is null");
    }

    /**
     * get value as boolean.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default boolean getBoolean(int index, boolean defaultValue) {
        var value = getBooleanOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as boolean.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<Boolean> findBoolean(int index) {
        var value = getBooleanOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as boolean.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable Boolean getBooleanOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toBoolean(value);
    }

    // int

    /**
     * get value as int.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default int getInt(int index) {
        var value = getIntOrNull(index);
        return Objects.requireNonNull(value, () -> "getInt(" + index + ") is null");
    }

    /**
     * get value as int.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default int getInt(int index, int defaultValue) {
        var value = getIntOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as int.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<Integer> findInt(int index) {
        var value = getIntOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as int.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable Integer getIntOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toInt(value);
    }

    // long

    /**
     * get value as long.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default long getLong(int index) {
        var value = getLongOrNull(index);
        return Objects.requireNonNull(value, () -> "getLong(" + index + ") is null");
    }

    /**
     * get value as long.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default long getLong(int index, long defaultValue) {
        var value = getLongOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as long.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<Long> findLong(int index) {
        var value = getLongOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as long.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable Long getLongOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toLong(value);
    }

    // float

    /**
     * get value as float.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default float getFloat(int index) {
        var value = getFloatOrNull(index);
        return Objects.requireNonNull(value, () -> "getFloat(" + index + ") is null");
    }

    /**
     * get value as float.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default float getFloat(int index, float defaultValue) {
        var value = getFloatOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as float.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<Float> findFloat(int index) {
        var value = getFloatOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as float.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable Float getFloatOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toFloat(value);
    }

    // double

    /**
     * get value as double.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default double getDouble(int index) {
        var value = getDoubleOrNull(index);
        return Objects.requireNonNull(value, () -> "getDouble(" + index + ") is null");
    }

    /**
     * get value as double.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default double getDouble(int index, double defaultValue) {
        var value = getDoubleOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as double.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<Double> findDouble(int index) {
        var value = getDoubleOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as double.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable Double getDoubleOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toDouble(value);
    }

    // decimal

    /**
     * get value as decimal.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull BigDecimal getDecimal(int index) {
        var value = getDecimalOrNull(index);
        return Objects.requireNonNull(value, () -> "getDecimal(" + index + ") is null");
    }

    /**
     * get value as decimal.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default BigDecimal getDecimal(int index, BigDecimal defaultValue) {
        var value = getDecimalOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as decimal.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<BigDecimal> findDecimal(int index) {
        var value = getDecimalOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as decimal.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable BigDecimal getDecimalOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toDecimal(value);
    }

    // string

    /**
     * get value as string.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull String getString(int index) {
        var value = getStringOrNull(index);
        return Objects.requireNonNull(value, () -> "getString(" + index + ") is null");
    }

    /**
     * get value as string.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default String getString(int index, String defaultValue) {
        var value = getStringOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as string.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<String> findString(int index) {
        var value = getStringOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as string.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable String getStringOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toString(value);
    }

    // byte[]

    /**
     * get value as byte[].
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull byte[] getBytes(int index) {
        var value = getBytesOrNull(index);
        return Objects.requireNonNull(value, () -> "getBytes(" + index + ") is null");
    }

    /**
     * get value as byte[].
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default byte[] getBytes(int index, byte[] defaultValue) {
        var value = getBytesOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as byte[].
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<byte[]> findBytes(int index) {
        var value = getBytesOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as byte[].
     *
     * @param index column index
     * @return value
     */
    public default @Nullable byte[] getBytesOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toBytes(value);
    }

    // boolean[]

    /**
     * get value as boolean[].
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull boolean[] getBits(int index) {
        var value = getBitsOrNull(index);
        return Objects.requireNonNull(value, () -> "getBits(" + index + ") is null");
    }

    /**
     * get value as boolean[].
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default boolean[] getBits(int index, boolean[] defaultValue) {
        var value = getBitsOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as boolean[].
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<boolean[]> findBits(int index) {
        var value = getBitsOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as boolean[].
     *
     * @param index column index
     * @return value
     */
    public default @Nullable boolean[] getBitsOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toBits(value);
    }

    // date

    /**
     * get value as date.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull LocalDate getDate(int index) {
        var value = getDateOrNull(index);
        return Objects.requireNonNull(value, () -> "getDate(" + index + ") is null");
    }

    /**
     * get value as date.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default LocalDate getDate(int index, LocalDate defaultValue) {
        var value = getDateOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as date.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<LocalDate> findDate(int index) {
        var value = getDateOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as date.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable LocalDate getDateOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toDate(value);
    }

    // time

    /**
     * get value as time.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull LocalTime getTime(int index) {
        var value = getTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getTime(" + index + ") is null");
    }

    /**
     * get value as time.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default LocalTime getTime(int index, LocalTime defaultValue) {
        var value = getTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as time.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<LocalTime> findTime(int index) {
        var value = getTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as time.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable LocalTime getTimeOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toTime(value);
    }

    // dateTime

    /**
     * get value as dateTime.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull LocalDateTime getDateTime(int index) {
        var value = getDateTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getDateTime(" + index + ") is null");
    }

    /**
     * get value as dateTime.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default LocalDateTime getDateTime(int index, LocalDateTime defaultValue) {
        var value = getDateTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as dateTime.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<LocalDateTime> findDateTime(int index) {
        var value = getDateTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as dateTime.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable LocalDateTime getDateTimeOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toDateTime(value);
    }

    // offset time

    /**
     * get value as offset time.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull OffsetTime getOffsetTime(int index) {
        var value = getOffsetTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getOffsetTime(" + index + ") is null");
    }

    /**
     * get value as offset time.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default OffsetTime getOffsetTime(int index, OffsetTime defaultValue) {
        var value = getOffsetTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as offset time.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<OffsetTime> findOffsetTime(int index) {
        var value = getOffsetTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as offset time.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable OffsetTime getOffsetTimeOrNull(int index) {
        var value = getValueOrNull(index);
        return getConvertUtil().toOffsetTime(value);
    }

    // offset dateTime

    /**
     * get value as offset dateTime.
     *
     * @param index column index
     * @return value
     * @throws NullPointerException if value is null
     */
    public default @Nonnull OffsetDateTime getOffsetDateTime(int index) {
        var value = getOffsetDateTimeOrNull(index);
        return Objects.requireNonNull(value, () -> "getOffsetDateTime(" + index + ") is null");
    }

    /**
     * get value as offset dateTime.
     *
     * @param index        column index
     * @param defaultValue value to return if original value is null
     * @return value
     */
    public default OffsetDateTime getOffsetDateTime(int index, OffsetDateTime defaultValue) {
        var value = getOffsetDateTimeOrNull(index);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as offset dateTime.
     *
     * @param index column index
     * @return value
     */
    public default @Nonnull Optional<OffsetDateTime> findOffsetDateTime(int index) {
        var value = getOffsetDateTimeOrNull(index);
        return Optional.ofNullable(value);
    }

    /**
     * get value as offset dateTime.
     *
     * @param index column index
     * @return value
     */
    public default @Nullable OffsetDateTime getOffsetDateTimeOrNull(int index) {
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
     * @throws NullPointerException if value is null
     */
    public default @Nonnull ZonedDateTime getZonedDateTime(int index, @Nonnull ZoneId zone) {
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
     */
    public default ZonedDateTime getZonedDateTime(int index, @Nonnull ZoneId zone, ZonedDateTime defaultValue) {
        var value = getZonedDateTimeOrNull(index, zone);
        return (value != null) ? value : defaultValue;
    }

    /**
     * get value as zoned dateTime.
     *
     * @param index column index
     * @param zone  time-zone
     * @return value
     */
    public default @Nonnull Optional<ZonedDateTime> findZonedDateTime(int index, @Nonnull ZoneId zone) {
        var value = getZonedDateTimeOrNull(index, zone);
        return Optional.ofNullable(value);
    }

    /**
     * get value as zoned dateTime.
     *
     * @param index column index
     * @param zone  time-zone
     * @return value
     */
    public default @Nullable ZonedDateTime getZonedDateTimeOrNull(int index, @Nonnull ZoneId zone) {
        var value = getValueOrNull(index);
        return getConvertUtil().toZonedDateTime(value, zone);
    }
}
