package com.tsurugidb.iceaxe.result;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Record for {@link TsurugiResultSet}
 * 
 * <p>
 * TODO+++翻訳: 当クラスのメソッド群は以下の3種類に分類される。ある群のメソッドを使用したら、他の群のメソッドは使用不可。
 * </p>
 * <ul>
 * <li>current column系
 * <ul>
 * <li>{@link #moveCurrentColumnNext()}によって現在カラムを移動しながら値を取得する。<br>
 * 各カラムの値は一度しか取得できない。</li>
 * <li>
 * 
 * <pre>
 * while (record.moveCurrentColumnNext()) {
 *     System.out.println(record.getCurrentColumnValue());
 * }
 * </pre>
 * 
 * </li>
 * </ul>
 * <li>name系
 * <ul>
 * <li>カラム名を指定して値を取得する。</li>
 * <li>
 * 
 * <pre>
 * entity.setFoo(record.getInt4("foo"));
 * entity.setBar(record.getInt8("bar"));
 * entity.setZzz(record.getCharacter("zzz"));
 * </pre>
 * 
 * </li>
 * </ul>
 * <li>next系</li>
 * <ul>
 * <li>現在カラムの値を取得し、次のカラムへ移動する。<br>
 * 各カラムの値は一度しか取得できない。<br>
 * next系メソッドを呼んだ直後に{@link #getCurrentColumnName()},{@link #getCurrentColumnType()}のみ使用可能。</li>
 * <li>
 * 
 * <pre>
 * entity.setFoo(record.nextInt4());
 * entity.setBar(record.nextInt8());
 * entity.setZzz(record.nextCharacter());
 * </pre>
 * 
 * </li>
 * </ul>
 * </ul>
 * <p>
 * 当クラスは{@link TsurugiResultSet}と連動しており、インスタンスは複数レコード間で共有される。<br>
 * そのため、レコードの値を保持する目的で、当インスタンスをユーザープログラムで保持してはならない。<br>
 * また、{@link TsurugiResultSet}のクローズ後に当クラスは使用できない。
 * </p>
 */
@NotThreadSafe
public class TsurugiResultRecord {

    protected static class TsurugiResultColumnValue { // record
        private final int index;
        private final Object value;

        public TsurugiResultColumnValue(int index, Object value) {
            this.index = index;
            this.value = value;
        }

        public int index() {
            return index;
        }

        public Object value() {
            return value;
        }
    }

    private final ResultSet lowResultSet;
    private Map<String, TsurugiResultColumnValue> columnMap;

    protected TsurugiResultRecord(ResultSet lowResultSet) {
        this.lowResultSet = lowResultSet;
    }

    void reset() {
        if (this.columnMap != null) {
            columnMap.clear();
        }
    }

    /*
     * current column
     */

    /**
     * move the current column to the next
     * 
     * @return true if the next column exists
     * @see #getCurrentColumnName()
     * @see #getCurrentColumnType()
     * @see #getCurrentColumnValue()
     */
    public boolean moveCurrentColumnNext() {
        return lowResultSet.nextColumn();
    }

    /**
     * get current column name
     * 
     * @return column name
     * @throws IOException
     * @see #moveCurrentColumnNext()
     */
    public String getCurrentColumnName() throws IOException {
        return lowResultSet.name();
    }

    /**
     * get current column data type
     * 
     * @return data type
     * @throws IOException
     * @see #moveCurrentColumnNext()
     */
    public TgDataType getCurrentColumnType() throws IOException {
        var lowType = lowResultSet.type();
        return TgDataType.of(lowType);
    }

    /**
     * get current column value
     * 
     * @return value
     * @throws IOException
     * @see #moveCurrentColumnNext()
     */
    @Nullable
    public Object getCurrentColumnValue() throws IOException {
        if (lowResultSet.isNull()) {
            return null;
        }
        var lowType = lowResultSet.type();
        switch (lowType) {
        // TODO boolean
        case INT4:
            return lowResultSet.getInt4();
        case INT8:
            return lowResultSet.getInt8();
        case FLOAT4:
            return lowResultSet.getFloat4();
        case FLOAT8:
            return lowResultSet.getFloat8();
        case CHARACTER:
            return lowResultSet.getCharacter();
        default:
            throw new UnsupportedOperationException("unsupported type error. lowType=" + lowType);
        }
    }

    /*
     * get by name
     */

    /**
     * get name list
     * 
     * @return list of column name
     * @throws IOException
     */
    @Nonnull
    public List<String> getNameList() throws IOException {
        return TsurugiResultSet.getNameList(lowResultSet);
    }

    @Nonnull
    protected synchronized Map<String, TsurugiResultColumnValue> getColumnMap() throws IOException {
        if (this.columnMap == null) {
            this.columnMap = new LinkedHashMap<String, TsurugiResultColumnValue>();
        }
        if (columnMap.isEmpty()) {
            for (int i = 0; lowResultSet.nextColumn(); i++) {
                var name = lowResultSet.name();
                var value = getCurrentColumnValue();
                var column = new TsurugiResultColumnValue(i, value);
                columnMap.put(name, column);
            }
        }
        return this.columnMap;
    }

    @Nonnull
    protected TsurugiResultColumnValue getColumn(String name) throws IOException {
        var map = getColumnMap();
        var column = map.get(name);
        if (column == null) {
            throw new IllegalArgumentException("not found column. name=" + name);
        }
        return column;
    }

    /**
     * get value
     * 
     * @param name column name
     * @return value
     * @throws IOException
     */
    @Nullable
    public Object getValue(String name) throws IOException {
        var column = getColumn(name);
        return column.value();
    }

    /**
     * get data type
     * 
     * @param name column name
     * @return data type
     * @throws IOException
     */
    @Nonnull
    public TgDataType getType(String name) throws IOException {
        var column = getColumn(name);
        var lowMeta = lowResultSet.getRecordMeta();
        var lowType = lowMeta.type(column.index());
        return TgDataType.of(lowType);
    }

    // boolean

    /**
     * get column value as boolean
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public boolean getBoolean(String name) throws IOException {
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
     * @throws IOException
     */
    public boolean getBoolean(String name, boolean defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<Boolean> findBoolean(String name) throws IOException {
        var value = getBooleanOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Boolean getBooleanOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toBoolean(lowValue);
    }

    // int4

    /**
     * get column value as int
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public int getInt4(String name) throws IOException {
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
     * @throws IOException
     */
    public int getInt4(String name, int defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<Integer> findInt4(String name) throws IOException {
        var value = getInt4OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as int
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Integer getInt4OrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toInt4(lowValue);
    }

    // int8

    /**
     * get column value as long
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public long getInt8(String name) throws IOException {
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
     * @throws IOException
     */
    public long getInt8(String name, long defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<Long> findInt8(String name) throws IOException {
        var value = getInt8OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as long
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Long getInt8OrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toInt8(lowValue);
    }

    // float4

    /**
     * get column value as float
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public float getFloat4(String name) throws IOException {
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
     * @throws IOException
     */
    public float getFloat4(String name, float defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<Float> findFloat4(String name) throws IOException {
        var value = getFloat4OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as float
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Float getFloat4OrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toFloat4(lowValue);
    }

    // float8

    /**
     * get column value as double
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public double getFloat8(String name) throws IOException {
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
     * @throws IOException
     */
    public double getFloat8(String name, float defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<Double> findFloat8(String name) throws IOException {
        var value = getFloat8OrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as double
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Double getFloat8OrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toFloat8(lowValue);
    }

    // decimal

    /**
     * get column value as decimal
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public BigDecimal getDecimal(String name) throws IOException {
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
     * @throws IOException
     */
    public BigDecimal getDecimal(String name, BigDecimal defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<BigDecimal> findDecimal(String name) throws IOException {
        var value = getDecimalOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as decimal
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public BigDecimal getDecimalOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toDecimal(lowValue);
    }

    // character

    /**
     * get column value as String
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public String getCharacter(String name) throws IOException {
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
     * @throws IOException
     */
    public String getCharacter(String name, String defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<String> findCharacter(String name) throws IOException {
        var value = getCharacterOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as String
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public String getCharacterOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toCharacter(lowValue);
    }

    // byte[]

    /**
     * get column value as byte[]
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public byte[] getBytes(String name) throws IOException {
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
     * @throws IOException
     */
    public byte[] getBytes(String name, byte[] defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<byte[]> findBytes(String name) throws IOException {
        var value = getBytesOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as byte[]
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public byte[] getBytesOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toBytes(lowValue);
    }

    // boolean[]

    /**
     * get column value as boolean[]
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public boolean[] getBits(String name) throws IOException {
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
     * @throws IOException
     */
    public boolean[] getBits(String name, boolean[] defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<boolean[]> findBits(String name) throws IOException {
        var value = getBitsOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean[]
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public boolean[] getBitsOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toBits(lowValue);
    }

    // date

    /**
     * get column value as date
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public LocalDate getDate(String name) throws IOException {
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
     * @throws IOException
     */
    public LocalDate getDate(String name, LocalDate defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<LocalDate> findDate(String name) throws IOException {
        var value = getDateOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as date
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public LocalDate getDateOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toDate(lowValue);
    }

    // time

    /**
     * get column value as time
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public LocalTime getTime(String name) throws IOException {
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
     * @throws IOException
     */
    public LocalTime getTime(String name, LocalTime defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<LocalTime> findTime(String name) throws IOException {
        var value = getTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as time
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public LocalTime getTimeOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toTime(lowValue);
    }

    // instant

    /**
     * get column value as instant
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public Instant getInstant(String name) throws IOException {
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
     * @throws IOException
     */
    public Instant getInstant(String name, Instant defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<Instant> findInstant(String name) throws IOException {
        var value = getInstantOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as instant
     * 
     * @param name column name
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Instant getInstantOrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toInstant(lowValue);
    }

    // ZonedDateTime

    /**
     * get column value as ZonedDateTime
     * 
     * @param name column name
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public ZonedDateTime getZonedDateTime(String name, ZoneId zone) throws IOException {
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
     * @throws IOException
     */
    public ZonedDateTime getZonedDateTime(String name, ZoneId zone, ZonedDateTime defaultValue) throws IOException {
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
     * @throws IOException
     */
    @Nonnull
    public Optional<ZonedDateTime> findZonedDateTime(String name, ZoneId zone) throws IOException {
        var value = getZonedDateTimeOrNull(name, zone);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as ZonedDateTime
     * 
     * @param name column name
     * @param zone time-zone
     * @return column value
     * @throws IOException
     */
    @Nullable
    public ZonedDateTime getZonedDateTimeOrNull(String name, ZoneId zone) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toZonedDateTime(lowValue, zone);
    }

    /*
     * next
     */

    /**
     * move next column
     * 
     * @throws IllegalStateException if not found next column
     */
    public void nextColumn() {
        boolean hasColumn = lowResultSet.nextColumn();
        if (!hasColumn) {
            throw new IllegalStateException("not found next column");
        }
    }

    @Nullable
    protected Object nextLowValue() throws IOException {
        nextColumn();
        return getCurrentColumnValue();
    }

    // boolean

    /**
     * get current column value as boolean and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public boolean nextBoolean() throws IOException {
        var value = nextBooleanOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as boolean and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public boolean nextBoolean(boolean defaultValue) throws IOException {
        var value = nextBooleanOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as boolean and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<Boolean> nextBooleanOpt() throws IOException {
        var value = nextBooleanOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as boolean and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Boolean nextBooleanOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toBoolean(lowValue);
    }

    // int4

    /**
     * get current column value as int and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public int nextInt4() throws IOException {
        var value = nextInt4OrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as int and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public int nextInt4(int defaultValue) throws IOException {
        var value = nextInt4OrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as int and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<Integer> nextInt4Opt() throws IOException {
        var value = nextInt4OrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as int and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Integer nextInt4OrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toInt4(lowValue);
    }

    // int8

    /**
     * get current column value as long and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public long nextInt8() throws IOException {
        var value = nextInt8OrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as long and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public long nextInt8(long defaultValue) throws IOException {
        var value = nextInt8OrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as long and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<Long> nextInt8Opt() throws IOException {
        var value = nextInt8OrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as long and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Long nextInt8OrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toInt8(lowValue);
    }

    // float4

    /**
     * get current column value as float and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public float nextFloat4() throws IOException {
        var value = nextFloat4OrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as float and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public float nextFloat4(int defaultValue) throws IOException {
        var value = nextFloat4OrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as float and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<Float> nextFloat4Opt() throws IOException {
        var value = nextFloat4OrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as float and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Float nextFloat4OrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toFloat4(lowValue);
    }

    // float8

    /**
     * get current column value as double and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    public double nextFloat8() throws IOException {
        var value = nextFloat8OrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as double and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public double nextFloat8(int defaultValue) throws IOException {
        var value = nextFloat8OrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as double and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<Double> nextFloat8Opt() throws IOException {
        var value = nextFloat8OrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as double and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Double nextFloat8OrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toFloat8(lowValue);
    }

    // decimal

    /**
     * get current column value as decimal and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public BigDecimal nextDecimal() throws IOException {
        var value = nextDecimalOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as decimal and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public BigDecimal nextDecimal(BigDecimal defaultValue) throws IOException {
        var value = nextDecimalOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as decimal and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<BigDecimal> nextDecimalOpt() throws IOException {
        var value = nextDecimalOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as decimal and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public BigDecimal nextDecimalOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toDecimal(lowValue);
    }

    // character

    /**
     * get current column value as String and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public String nextCharacter() throws IOException {
        var value = nextCharacterOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as String and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public String nextCharacter(String defaultValue) throws IOException {
        var value = nextCharacterOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as String and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<String> nextCharacterOpt() throws IOException {
        var value = nextCharacterOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as String and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public String nextCharacterOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toCharacter(lowValue);
    }

    // byte[]

    /**
     * get current column value as byte[] and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public byte[] nextBytes() throws IOException {
        var value = nextBytesOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as byte[] and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public byte[] nextBytes(byte[] defaultValue) throws IOException {
        var value = nextBytesOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as byte[] and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<byte[]> nextBytesOpt() throws IOException {
        var value = nextBytesOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as byte[] and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public byte[] nextBytesOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toBytes(lowValue);
    }

    // boolean[]

    /**
     * get current column value as boolean[] and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public boolean[] nextBits() throws IOException {
        var value = nextBitsOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as boolean[] and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public boolean[] nextBits(boolean[] defaultValue) throws IOException {
        var value = nextBitsOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as boolean[] and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<boolean[]> nextBitsOpt() throws IOException {
        var value = nextBitsOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as boolean[] and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public boolean[] nextBitsOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toBits(lowValue);
    }

    // date

    /**
     * get current column value as date and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public LocalDate nextDate() throws IOException {
        var value = nextDateOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as date and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public LocalDate nextDate(LocalDate defaultValue) throws IOException {
        var value = nextDateOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as date and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<LocalDate> nextDateOpt() throws IOException {
        var value = nextDateOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as date and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public LocalDate nextDateOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toDate(lowValue);
    }

    // time

    /**
     * get current column value as time and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public LocalTime nextTime() throws IOException {
        var value = nextTimeOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as time and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public LocalTime nextTime(LocalTime defaultValue) throws IOException {
        var value = nextTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as time and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<LocalTime> nextTimeOpt() throws IOException {
        var value = nextTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as time and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public LocalTime nextTimeOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toTime(lowValue);
    }

    // instant

    /**
     * get current column value as instant and move next column
     * 
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public Instant nextInstant() throws IOException {
        var value = nextInstantOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as instant and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public Instant nextInstant(Instant defaultValue) throws IOException {
        var value = nextInstantOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as instant and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<Instant> nextInstantOpt() throws IOException {
        var value = nextInstantOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as instant and move next column
     * 
     * @return column value
     * @throws IOException
     */
    @Nullable
    public Instant nextInstantOrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toInstant(lowValue);
    }

    // ZonedDateTime

    /**
     * get current column value as ZonedDateTime and move next column
     * 
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws NullPointerException if value is null
     */
    @Nonnull
    public ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone) throws IOException {
        var value = nextZonedDateTimeOrNull(zone);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as ZonedDateTime and move next column
     * 
     * @param zone         time-zone
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     */
    public ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone, ZonedDateTime defaultValue) throws IOException {
        var value = nextZonedDateTimeOrNull(zone);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as ZonedDateTime and move next column
     * 
     * @param zone time-zone
     * @return column value
     * @throws IOException
     */
    @Nonnull
    public Optional<ZonedDateTime> nextZonedDateTimeOpt(@Nonnull ZoneId zone) throws IOException {
        var value = nextZonedDateTimeOrNull(zone);
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as ZonedDateTime and move next column
     * 
     * @param zone time-zone
     * @return column value
     * @throws IOException
     */
    @Nullable
    public ZonedDateTime nextZonedDateTimeOrNull(@Nonnull ZoneId zone) throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toZonedDateTime(lowValue, zone);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + lowResultSet + "}";
    }
}
