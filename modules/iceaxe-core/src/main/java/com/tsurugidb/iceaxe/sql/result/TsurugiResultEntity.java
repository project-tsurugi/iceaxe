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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Record.
 */
@ThreadSafe
public class TsurugiResultEntity {

    /**
     * create entity.
     *
     * @param record Tsurugi Result Record
     * @return entity
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public static TsurugiResultEntity of(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        var entity = new TsurugiResultEntity();
        entity.setConvertUtil(record.getConvertUtil());
        while (record.moveCurrentColumnNext()) {
            var name = record.getCurrentColumnName();
            var value = record.fetchCurrentColumnValue();
            entity.add(name, value);
        }
        return entity;
    }

    private IceaxeConvertUtil convertUtil = IceaxeConvertUtil.INSTANCE;
    /** Map&lt;name, value&gt; */
    private final Map<String, Object> valueMap = new LinkedHashMap<>();
    private List<String> nameList;

    /**
     * set convert type utility.
     *
     * @param convertUtil convert type utility
     */
    public void setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
    }

    /**
     * add value.
     *
     * @param name  column name
     * @param value column value
     */
    protected void add(String name, Object value) {
        valueMap.put(name, value);
    }

    /**
     * get column name list.
     *
     * @return list of column name
     */
    public @Nonnull List<String> getNameList() {
        if (this.nameList == null) {
            var set = valueMap.keySet();
            this.nameList = List.of(set.toArray(new String[set.size()]));
        }
        return this.nameList;
    }

    /**
     * get column name.
     *
     * @param index column index
     * @return column name
     * @throws IndexOutOfBoundsException if the index is out of range ({@code index < 0 || index >= size()})
     */
    public String getName(int index) {
        return getNameList().get(index);
    }

    /**
     * get column value.
     *
     * @param name column name
     * @return column value
     */
    protected @Nullable Object getValue(String name) {
        var value = valueMap.get(name);
        if (value == null) {
            if (!valueMap.containsKey(name)) {
                throw new IllegalArgumentException("not found column. name=" + name);
            }
        }
        return value;
    }

    // boolean

    /**
     * get column value as boolean.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public boolean getBoolean(String name) {
        var value = getBooleanOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as boolean.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        var value = getBooleanOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as boolean.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<Boolean> findBoolean(String name) {
        var value = getBooleanOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable Boolean getBooleanOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toBoolean(value);
    }

    // int

    /**
     * get column value as int.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public int getInt(String name) {
        var value = getIntOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as int.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public int getInt(String name, int defaultValue) {
        var value = getIntOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as int.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<Integer> findInt(String name) {
        var value = getIntOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as int.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable Integer getIntOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toInt(value);
    }

    // long

    /**
     * get column value as long.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public long getLong(String name) {
        var value = getLongOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as long.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public long getLong(String name, long defaultValue) {
        var value = getLongOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as long.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<Long> findLong(String name) {
        var value = getLongOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as long.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable Long getLongOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toLong(value);
    }

    // float

    /**
     * get column value as float.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public float getFloat(String name) {
        var value = getFloatOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as float.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public float getFloat(String name, float defaultValue) {
        var value = getFloatOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as float.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<Float> findFloat(String name) {
        var value = getFloatOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as float.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable Float getFloatOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toFloat(value);
    }

    // double

    /**
     * get column value as double.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public double getDouble(String name) {
        var value = getDoubleOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as double.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public double getDouble(String name, double defaultValue) {
        var value = getDoubleOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as double.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<Double> findDouble(String name) {
        var value = getDoubleOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as double.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable Double getDoubleOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toDouble(value);
    }

    // decimal

    /**
     * get column value as decimal.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull BigDecimal getDecimal(String name) {
        var value = getDecimalOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as decimal.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public BigDecimal getDecimal(String name, BigDecimal defaultValue) {
        var value = getDecimalOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as decimal.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<BigDecimal> findDecimal(String name) {
        var value = getDecimalOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as decimal.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable BigDecimal getDecimalOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toDecimal(value);
    }

    // string

    /**
     * get column value as String.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull String getString(String name) {
        var value = getStringOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as String.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public String getString(String name, String defaultValue) {
        var value = getStringOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as String.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<String> findString(String name) {
        var value = getStringOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as String.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable String getStringOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toString(value);
    }

    // byte[]

    /**
     * get column value as byte[].
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull byte[] getBytes(String name) {
        var value = getBytesOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as byte[].
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public byte[] getBytes(String name, byte[] defaultValue) {
        var value = getBytesOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as byte[].
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<byte[]> findBytes(String name) {
        var value = getBytesOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as byte[].
     *
     * @param name column name
     * @return column value
     */
    public @Nullable byte[] getBytesOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toBytes(value);
    }

    // boolean[]

    /**
     * get column value as boolean[].
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull boolean[] getBits(String name) {
        var value = getBitsOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as boolean[].
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public boolean[] getBits(String name, boolean[] defaultValue) {
        var value = getBitsOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as boolean[].
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<boolean[]> findBits(String name) {
        var value = getBitsOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean[].
     *
     * @param name column name
     * @return column value
     */
    public @Nullable boolean[] getBitsOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toBits(value);
    }

    // date

    /**
     * get column value as date.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull LocalDate getDate(String name) {
        var value = getDateOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as date.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public LocalDate getDate(String name, LocalDate defaultValue) {
        var value = getDateOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as date.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<LocalDate> findDate(String name) {
        var value = getDateOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as date.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable LocalDate getDateOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toDate(value);
    }

    // time

    /**
     * get column value as time.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull LocalTime getTime(String name) {
        var value = getTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as time.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public LocalTime getTime(String name, LocalTime defaultValue) {
        var value = getTimeOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as time.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<LocalTime> findTime(String name) {
        var value = getTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as time.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable LocalTime getTimeOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toTime(value);
    }

    // dateTime

    /**
     * get column value as dateTime.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull LocalDateTime getDateTime(String name) {
        var value = getDateTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as dateTime.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public LocalDateTime getDateTime(String name, LocalDateTime defaultValue) {
        var value = getDateTimeOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as dateTime.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<LocalDateTime> findDateTime(String name) {
        var value = getDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as dateTime.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable LocalDateTime getDateTimeOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toDateTime(value);
    }

    // offset time

    /**
     * get column value as offset time.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull OffsetTime getOffsetTime(String name) {
        var value = getOffsetTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as offset time.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public OffsetTime getOffsetTime(String name, OffsetTime defaultValue) {
        var value = getOffsetTimeOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as offset time.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<OffsetTime> findOffsetTime(String name) {
        var value = getOffsetTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as offset time.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable OffsetTime getOffsetTimeOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toOffsetTime(value);
    }

    // offset dateTime

    /**
     * get column value as offset dateTime.
     *
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull OffsetDateTime getOffsetDateTime(String name) {
        var value = getOffsetDateTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as offset dateTime.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public OffsetDateTime getOffsetDateTime(String name, OffsetDateTime defaultValue) {
        var value = getOffsetDateTimeOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as offset dateTime.
     *
     * @param name column name
     * @return column value
     */
    public @Nonnull Optional<OffsetDateTime> findOffsetDateTime(String name) {
        var value = getOffsetDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as offset dateTime.
     *
     * @param name column name
     * @return column value
     */
    public @Nullable OffsetDateTime getOffsetDateTimeOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toOffsetDateTime(value);
    }

    // zoned dateTime

    /**
     * get column value as ZonedDateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return column value
     * @throws NullPointerException if value is null
     */
    public @Nonnull ZonedDateTime getZonedDateTime(String name, @Nonnull ZoneId zone) {
        var value = getZonedDateTimeOrNull(name, zone);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as ZonedDateTime.
     *
     * @param name         column name
     * @param zone         time-zone
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public ZonedDateTime getZonedDateTime(String name, @Nonnull ZoneId zone, ZonedDateTime defaultValue) {
        var value = getZonedDateTimeOrNull(name, zone);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as ZonedDateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return column value
     */
    public @Nonnull Optional<ZonedDateTime> findZonedDateTime(String name, @Nonnull ZoneId zone) {
        var value = getZonedDateTimeOrNull(name, zone);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as ZonedDateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return column value
     */
    public @Nullable ZonedDateTime getZonedDateTimeOrNull(String name, @Nonnull ZoneId zone) {
        var value = getValue(name);
        return convertUtil.toZonedDateTime(value, zone);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + valueMap;
    }
}
