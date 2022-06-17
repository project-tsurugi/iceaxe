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

import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Record
 */
public class TsurugiResultEntity {

    public static TsurugiResultEntity of(TsurugiResultRecord record) throws IOException {
        var entity = new TsurugiResultEntity();
        while (record.moveCurrentColumnNext()) {
            var name = record.getCurrentColumnName();
            var value = record.getCurrentColumnValue();
            entity.add(name, value);
        }
        return entity;
    }

    /** Map&lt;name, value&gt; */
    private final Map<String, Object> valueMap = new LinkedHashMap<>();

    protected void add(String name, Object value) {
        valueMap.put(name, value);
    }

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
    public Boolean getBooleanOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toBoolean(value);
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
    public Integer getInt4OrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toInt4(value);
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
    public Long getInt8OrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toInt8(value);
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
    public Float getFloat4OrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toFloat4(value);
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
    public Double getFloat8OrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toFloat8(value);
    }

    // decimal

    /**
     * get column value as decimal
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
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
    public BigDecimal getDecimalOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toDecimal(value);
    }

    // character

    /**
     * get column value as String
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
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
    public String getCharacterOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toCharacter(value);
    }

    // byte[]

    /**
     * get column value as byte[]
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
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
    public byte[] getBytesOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toBytes(value);
    }

    // boolean[]

    /**
     * get column value as boolean[]
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
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
    public boolean[] getBitsOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toBits(value);
    }

    // date

    /**
     * get column value as date
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
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
    public LocalDate getDateOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toDate(value);
    }

    // time

    /**
     * get column value as time
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
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
    public LocalTime getTimeOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toTime(value);
    }

    // instant

    /**
     * get column value as instant
     * 
     * @param name column name
     * @return column value
     * @throws NullPointerException if value is null
     */
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
    public Instant getInstantOrNull(String name) {
        var value = getValue(name);
        return IceaxeConvertUtil.toInstant(value);
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
    public ZonedDateTime getZonedDateTime(String name, ZoneId zone) {
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
    public ZonedDateTime getZonedDateTime(String name, ZoneId zone, ZonedDateTime defaultValue) {
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
    public Optional<ZonedDateTime> findZonedDateTime(String name, ZoneId zone) {
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
    public ZonedDateTime getZonedDateTimeOrNull(String name, ZoneId zone) {
        var value = getValue(name);
        return IceaxeConvertUtil.toZonedDateTime(value, zone);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + valueMap;
    }
}
