package com.tsurugidb.iceaxe.result;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Record
 */
@ThreadSafe
public class TsurugiResultEntity {

    /**
     * create entity
     * 
     * @param record Tsurugi Result Record
     * @return entity
     * @throws IOException
     */
    public static TsurugiResultEntity of(TsurugiResultRecord record) throws IOException {
        var entity = new TsurugiResultEntity();
        entity.setConvertUtil(record.getConvertUtil());
        while (record.moveCurrentColumnNext()) {
            var name = record.getCurrentColumnName();
            var value = record.getCurrentColumnValue();
            entity.add(name, value);
        }
        return entity;
    }

    private IceaxeConvertUtil convertUtil = IceaxeConvertUtil.INSTANCE;
    /** Map&lt;name, value&gt; */
    private final Map<String, Object> valueMap = new LinkedHashMap<>();

    /**
     * set convert type utility
     * 
     * @param convertUtil convert type utility
     */
    public void setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
    }

    protected void add(String name, Object value) {
        valueMap.put(name, value);
    }

    @Nullable
    protected Object getValue(String name) {
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
     * get column value as boolean
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
     * get column value as boolean
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
     * get column value as boolean
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<Boolean> findBoolean(String name) {
        var value = getBooleanOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public Boolean getBooleanOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toBoolean(value);
    }

    // int4

    /**
     * get column value as int
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public int getInt4(String name) {
        var value = getInt4OrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as int
     * 
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public int getInt4(String name, int defaultValue) {
        var value = getInt4OrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as int
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<Integer> findInt4(String name) {
        var value = getInt4OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as int
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public Integer getInt4OrNull(String name) {
        var value = getValue(name);
        return convertUtil.toInt4(value);
    }

    // int8

    /**
     * get column value as long
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public long getInt8(String name) {
        var value = getInt8OrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as long
     * 
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public long getInt8(String name, long defaultValue) {
        var value = getInt8OrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as long
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<Long> findInt8(String name) {
        var value = getInt8OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as long
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public Long getInt8OrNull(String name) {
        var value = getValue(name);
        return convertUtil.toInt8(value);
    }

    // float4

    /**
     * get column value as float
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public float getFloat4(String name) {
        var value = getFloat4OrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as float
     * 
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public float getFloat4(String name, float defaultValue) {
        var value = getFloat4OrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as float
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<Float> findFloat4(String name) {
        var value = getFloat4OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as float
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public Float getFloat4OrNull(String name) {
        var value = getValue(name);
        return convertUtil.toFloat4(value);
    }

    // float8

    /**
     * get column value as double
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    public double getFloat8(String name) {
        var value = getFloat8OrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as double
     * 
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public double getFloat8(String name, double defaultValue) {
        var value = getFloat8OrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as double
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<Double> findFloat8(String name) {
        var value = getFloat8OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as double
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public Double getFloat8OrNull(String name) {
        var value = getValue(name);
        return convertUtil.toFloat8(value);
    }

    // decimal

    /**
     * get column value as decimal
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public BigDecimal getDecimal(String name) {
        var value = getDecimalOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as decimal
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
     * get column value as decimal
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<BigDecimal> findDecimal(String name) {
        var value = getDecimalOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as decimal
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public BigDecimal getDecimalOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toDecimal(value);
    }

    // character

    /**
     * get column value as String
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public String getCharacter(String name) {
        var value = getCharacterOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as String
     * 
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public String getCharacter(String name, String defaultValue) {
        var value = getCharacterOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as String
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<String> findCharacter(String name) {
        var value = getCharacterOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as String
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public String getCharacterOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toCharacter(value);
    }

    // byte[]

    /**
     * get column value as byte[]
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public byte[] getBytes(String name) {
        var value = getBytesOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as byte[]
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
     * get column value as byte[]
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<byte[]> findBytes(String name) {
        var value = getBytesOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as byte[]
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public byte[] getBytesOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toBytes(value);
    }

    // boolean[]

    /**
     * get column value as boolean[]
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public boolean[] getBits(String name) {
        var value = getBitsOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as boolean[]
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
     * get column value as boolean[]
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<boolean[]> findBits(String name) {
        var value = getBitsOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean[]
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public boolean[] getBitsOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toBits(value);
    }

    // date

    /**
     * get column value as date
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public LocalDate getDate(String name) {
        var value = getDateOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as date
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
     * get column value as date
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<LocalDate> findDate(String name) {
        var value = getDateOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as date
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public LocalDate getDateOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toDate(value);
    }

    // time

    /**
     * get column value as time
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public LocalTime getTime(String name) {
        var value = getTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as time
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
     * get column value as time
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<LocalTime> findTime(String name) {
        var value = getTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as time
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public LocalTime getTimeOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toTime(value);
    }

    // instant

    /**
     * get column value as instant
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public Instant getInstant(String name) {
        var value = getInstantOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as instant
     * 
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public Instant getInstant(String name, Instant defaultValue) {
        var value = getInstantOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as instant
     * 
     * @param name column name
     * @return column value
     */
    @Nonnull
    public Optional<Instant> findInstant(String name) {
        var value = getInstantOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as instant
     * 
     * @param name column name
     * @return column value
     */
    @Nullable
    public Instant getInstantOrNull(String name) {
        var value = getValue(name);
        return convertUtil.toInstant(value);
    }

    // ZonedDateTime

    /**
     * get column value as ZonedDateTime
     * 
     * @param name column name
     * @param zone time-zone
     * @return column value
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public ZonedDateTime getZonedDateTime(String name, @Nonnull ZoneId zone) {
        var value = getZonedDateTimeOrNull(name, zone);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as ZonedDateTime
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
     * get column value as ZonedDateTime
     * 
     * @param name column name
     * @param zone time-zone
     * @return column value
     */
    @Nonnull
    public Optional<ZonedDateTime> findZonedDateTime(String name, @Nonnull ZoneId zone) {
        var value = getZonedDateTimeOrNull(name, zone);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as ZonedDateTime
     * 
     * @param name column name
     * @param zone time-zone
     * @return column value
     */
    @Nullable
    public ZonedDateTime getZonedDateTimeOrNull(String name, @Nonnull ZoneId zone) {
        var value = getValue(name);
        return convertUtil.toZonedDateTime(value, zone);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + valueMap;
    }
}
