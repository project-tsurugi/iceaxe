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
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;

/**
 * Tsurugi Result Record for {@link TusurigQueryResult}
 *
 * <p>
 * TODO+++翻訳: 当クラスのメソッド群は以下の3種類に分類される。ある群のメソッドを使用したら、他の群のメソッドは基本的に使用不可。
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
 *     String name = record.getCurrentColumnName();
 *     Object value = record.fetchCurrentColumnValue();
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
 * entity.setFoo(record.getInt("foo"));
 * entity.setBar(record.getLong("bar"));
 * entity.setZzz(record.getString("zzz"));
 * </pre>
 *
 * </li>
 * </ul>
 * <li>next系</li>
 * <ul>
 * <li>現在カラムの値を取得し、次のカラムへ移動する。<br>
 * 各カラムの値は一度しか取得できない。<br>
 * next系メソッドを呼んだ直後に{@link #getCurrentColumnName()},{@link #getCurrentColumnType()}は使用可能。</li>
 * <li>
 *
 * <pre>
 * entity.setFoo(record.nextInt());
 * entity.setBar(record.nextLong());
 * entity.setZzz(record.nextString());
 * </pre>
 *
 * </li>
 * </ul>
 * </ul>
 * <p>
 * 当クラスは{@link TusurigQueryResult}と連動しており、インスタンスは複数レコード間で共有される。<br>
 * そのため、レコードの値を保持する目的で、当インスタンスをユーザープログラムで保持してはならない。<br>
 * また、{@link TusurigQueryResult}のクローズ後に当クラスは使用できない。
 * </p>
 */
@NotThreadSafe
public class TsurugiResultRecord {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResultRecord.class);

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
    private final IceaxeConvertUtil convertUtil;
    private int currentColumnIndex;
    private Map<String, TsurugiResultColumnValue> columnMap;

    protected TsurugiResultRecord(ResultSet lowResultSet, IceaxeConvertUtil convertUtil) {
        this.lowResultSet = lowResultSet;
        this.convertUtil = convertUtil;
        reset();
    }

    void reset() {
        this.currentColumnIndex = -1;
        if (this.columnMap != null) {
            columnMap.clear();
        }
    }

    // internal
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    /*
     * current column
     */

    /**
     * move the current column to the next
     *
     * @return true if the next column exists
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #getCurrentColumnName()
     * @see #getCurrentColumnType()
     * @see #fetchCurrentColumnValue()
     */
    public boolean moveCurrentColumnNext() throws IOException, TsurugiTransactionException {
        try {
            LOG.trace("nextLowColumn start");
            boolean exists = lowResultSet.nextColumn();
            if (LOG.isTraceEnabled()) {
                LOG.trace("nextLowColumn end. exists={}", exists);
            }
            if (exists) {
                this.currentColumnIndex++;
            }
            return exists;
        } catch (ServerException e) {
            throw new TsurugiTransactionException(e);
        } catch (InterruptedException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Nonnull
    protected Column getLowColumn(int index) throws IOException, TsurugiTransactionException {
        var lowColumnList = TusurigQueryResult.getLowColumnList(lowResultSet);
        return lowColumnList.get(index);
    }

    /**
     * get current column name
     *
     * @return column name
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #moveCurrentColumnNext()
     */
    @Nonnull
    public String getCurrentColumnName() throws IOException, TsurugiTransactionException {
        var lowColumn = getLowColumn(currentColumnIndex);
        return TusurigQueryResult.getColumnName(lowColumn, currentColumnIndex);
    }

    /**
     * get current column data type
     *
     * @return data type
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #moveCurrentColumnNext()
     */
    @Nonnull
    public TgDataType getCurrentColumnType() throws IOException, TsurugiTransactionException {
        var lowType = getCurrentColumnLowType();
        return TgDataType.of(lowType);
    }

    @Nonnull
    protected AtomType getCurrentColumnLowType() throws IOException, TsurugiTransactionException {
        var lowColumn = getLowColumn(currentColumnIndex);
        return lowColumn.getAtomType();
    }

    /**
     * get current column value (take once)
     *
     * @return value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @see #moveCurrentColumnNext()
     */
    @Nullable
    public Object fetchCurrentColumnValue() throws IOException, TsurugiTransactionException {
        if (lowResultSet.isNull()) {
            LOG.trace("fetch null");
            return null;
        }
        var lowType = getCurrentColumnLowType();
        try {
            switch (lowType) {
            case BOOLEAN:
                return lowResultSet.fetchBooleanValue();
            case INT4:
                return lowResultSet.fetchInt4Value();
            case INT8:
                return lowResultSet.fetchInt8Value();
            case FLOAT4:
                return lowResultSet.fetchFloat4Value();
            case FLOAT8:
                return lowResultSet.fetchFloat8Value();
            case DECIMAL:
                return lowResultSet.fetchDecimalValue();
            case CHARACTER:
                return lowResultSet.fetchCharacterValue();
            case OCTET:
                return lowResultSet.fetchOctetValue();
            case BIT:
                return lowResultSet.fetchBitValue();
            case DATE:
                return lowResultSet.fetchDateValue();
            case TIME_OF_DAY:
                return lowResultSet.fetchTimeOfDayValue();
            case TIME_POINT:
                return lowResultSet.fetchTimePointValue();
            case DATETIME_INTERVAL:
                return lowResultSet.fetchDateTimeIntervalValue();
            default:
                throw new UnsupportedOperationException("unsupported type error. lowType=" + lowType);
            }
        } catch (ServerException e) {
            throw new TsurugiTransactionException(e);
        } catch (InterruptedException e) {
            throw new IOException(e.getMessage(), e);
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public List<String> getNameList() throws IOException, TsurugiTransactionException {
        return TusurigQueryResult.getNameList(lowResultSet);
    }

    @Nonnull
    protected Map<String, TsurugiResultColumnValue> getColumnMap() throws IOException, TsurugiTransactionException {
        if (this.columnMap == null) {
            this.columnMap = new LinkedHashMap<>();
        }
        if (columnMap.isEmpty()) {
            while (moveCurrentColumnNext()) {
                var name = getCurrentColumnName();
                var value = fetchCurrentColumnValue();
                var column = new TsurugiResultColumnValue(currentColumnIndex, value);
                columnMap.put(name, column);
            }
        }
        return this.columnMap;
    }

    @Nonnull
    protected TsurugiResultColumnValue getColumn(String name) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Object getValue(String name) throws IOException, TsurugiTransactionException {
        var column = getColumn(name);
        return column.value();
    }

    /**
     * get data type
     *
     * @param name column name
     * @return data type
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public TgDataType getType(String name) throws IOException, TsurugiTransactionException {
        var column = getColumn(name);
        var lowColumn = getLowColumn(column.index());
        var lowType = lowColumn.getAtomType();
        return TgDataType.of(lowType);
    }

    // boolean

    /**
     * get column value as boolean
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public boolean getBoolean(String name) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public boolean getBoolean(String name, boolean defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Boolean> findBoolean(String name) throws IOException, TsurugiTransactionException {
        var value = getBooleanOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Boolean getBooleanOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toBoolean(lowValue);
    }

    // int

    /**
     * get column value as int
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public int getInt(String name) throws IOException, TsurugiTransactionException {
        var value = getIntOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    public int getInt(String name, int defaultValue) throws IOException, TsurugiTransactionException {
        var value = getIntOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Integer> findInt(String name) throws IOException, TsurugiTransactionException {
        var value = getIntOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as int
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Integer getIntOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toInt(lowValue);
    }

    // long

    /**
     * get column value as long
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public long getLong(String name) throws IOException, TsurugiTransactionException {
        var value = getLongOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    public long getLong(String name, long defaultValue) throws IOException, TsurugiTransactionException {
        var value = getLongOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Long> findLong(String name) throws IOException, TsurugiTransactionException {
        var value = getLongOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as long
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Long getLongOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toLong(lowValue);
    }

    // float

    /**
     * get column value as float
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public float getFloat(String name) throws IOException, TsurugiTransactionException {
        var value = getFloatOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    public float getFloat(String name, float defaultValue) throws IOException, TsurugiTransactionException {
        var value = getFloatOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Float> findFloat(String name) throws IOException, TsurugiTransactionException {
        var value = getFloatOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as float
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Float getFloatOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toFloat(lowValue);
    }

    // double

    /**
     * get column value as double
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public double getDouble(String name) throws IOException, TsurugiTransactionException {
        var value = getDoubleOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    public double getDouble(String name, float defaultValue) throws IOException, TsurugiTransactionException {
        var value = getDoubleOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Double> findDouble(String name) throws IOException, TsurugiTransactionException {
        var value = getDoubleOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as double
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Double getDoubleOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDouble(lowValue);
    }

    // decimal

    /**
     * get column value as decimal
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public BigDecimal getDecimal(String name) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public BigDecimal getDecimal(String name, BigDecimal defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<BigDecimal> findDecimal(String name) throws IOException, TsurugiTransactionException {
        var value = getDecimalOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as decimal
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public BigDecimal getDecimalOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDecimal(lowValue);
    }

    // string

    /**
     * get column value as String
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public String getString(String name) throws IOException, TsurugiTransactionException {
        var value = getStringOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    public String getString(String name, String defaultValue) throws IOException, TsurugiTransactionException {
        var value = getStringOrNull(name);
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<String> findString(String name) throws IOException, TsurugiTransactionException {
        var value = getStringOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as String
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public String getStringOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toString(lowValue);
    }

    // byte[]

    /**
     * get column value as byte[]
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public byte[] getBytes(String name) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public byte[] getBytes(String name, byte[] defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<byte[]> findBytes(String name) throws IOException, TsurugiTransactionException {
        var value = getBytesOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as byte[]
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public byte[] getBytesOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toBytes(lowValue);
    }

    // boolean[]

    /**
     * get column value as boolean[]
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public boolean[] getBits(String name) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public boolean[] getBits(String name, boolean[] defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<boolean[]> findBits(String name) throws IOException, TsurugiTransactionException {
        var value = getBitsOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean[]
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public boolean[] getBitsOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toBits(lowValue);
    }

    // date

    /**
     * get column value as date
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public LocalDate getDate(String name) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public LocalDate getDate(String name, LocalDate defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<LocalDate> findDate(String name) throws IOException, TsurugiTransactionException {
        var value = getDateOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as date
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public LocalDate getDateOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDate(lowValue);
    }

    // time

    /**
     * get column value as time
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public LocalTime getTime(String name) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public LocalTime getTime(String name, LocalTime defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<LocalTime> findTime(String name) throws IOException, TsurugiTransactionException {
        var value = getTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as time
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public LocalTime getTimeOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toTime(lowValue);
    }

    // date time

    /**
     * get column value as dateTime
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public LocalDateTime getDateTime(String name) throws IOException, TsurugiTransactionException {
        var value = getDateTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as dateTime
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public LocalDateTime getDateTime(String name, LocalDateTime defaultValue) throws IOException, TsurugiTransactionException {
        var value = getDateTimeOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as dateTime
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<LocalDateTime> findDateTime(String name) throws IOException, TsurugiTransactionException {
        var value = getDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as dateTime
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public LocalDateTime getDateTimeOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDateTime(lowValue);
    }

    // offset time

    /**
     * get column value as offset time
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public OffsetTime getOffsetTime(String name) throws IOException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as offset time
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public OffsetTime getOffsetTime(String name, OffsetTime defaultValue) throws IOException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as offset time
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<OffsetTime> findOffsetTime(String name) throws IOException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as offset time
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public OffsetTime getOffsetTimeOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toOffsetTime(lowValue);
    }

    // offset dateTime

    /**
     * get column value as offset dateTime
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public OffsetDateTime getOffsetDateTime(String name) throws IOException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(name);
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get column value as offset dateTime
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public OffsetDateTime getOffsetDateTime(String name, OffsetDateTime defaultValue) throws IOException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(name);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get column value as offset dateTime
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<OffsetDateTime> findOffsetDateTime(String name) throws IOException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as offset dateTime
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public OffsetDateTime getOffsetDateTimeOrNull(String name) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toOffsetDateTime(lowValue);
    }

    // zoned dateTime

    /**
     * get column value as ZonedDateTime
     *
     * @param name column name
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public ZonedDateTime getZonedDateTime(String name, ZoneId zone) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public ZonedDateTime getZonedDateTime(String name, ZoneId zone, ZonedDateTime defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<ZonedDateTime> findZonedDateTime(String name, ZoneId zone) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nullable
    public ZonedDateTime getZonedDateTimeOrNull(String name, ZoneId zone) throws IOException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toZonedDateTime(lowValue, zone);
    }

    /*
     * next
     */

    /**
     * move next column
     *
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws IllegalStateException       if not found next column
     */
    public void nextColumn() throws IOException, TsurugiTransactionException {
        boolean exists = moveCurrentColumnNext();
        if (!exists) {
            throw new IllegalStateException("not found next column");
        }
    }

    @Nullable
    protected Object nextLowValue() throws IOException, TsurugiTransactionException {
        nextColumn();
        return fetchCurrentColumnValue();
    }

    // boolean

    /**
     * get current column value as boolean and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public boolean nextBoolean() throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public boolean nextBoolean(boolean defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Boolean> nextBooleanOpt() throws IOException, TsurugiTransactionException {
        var value = nextBooleanOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as boolean and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Boolean nextBooleanOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toBoolean(lowValue);
    }

    // int

    /**
     * get current column value as int and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public int nextInt() throws IOException, TsurugiTransactionException {
        var value = nextIntOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as int and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public int nextInt(int defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextIntOrNull();
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Integer> nextIntOpt() throws IOException, TsurugiTransactionException {
        var value = nextIntOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as int and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Integer nextIntOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toInt(lowValue);
    }

    // long

    /**
     * get current column value as long and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public long nextLong() throws IOException, TsurugiTransactionException {
        var value = nextLongOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as long and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public long nextLong(long defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextLongOrNull();
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Long> nextLongOpt() throws IOException, TsurugiTransactionException {
        var value = nextLongOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as long and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Long nextLongOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toLong(lowValue);
    }

    // float

    /**
     * get current column value as float and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public float nextFloat() throws IOException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as float and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public float nextFloat(int defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextFloatOrNull();
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Float> nextFloatOpt() throws IOException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as float and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Float nextFloatOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toFloat(lowValue);
    }

    // double

    /**
     * get current column value as double and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public double nextDouble() throws IOException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as double and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public double nextDouble(int defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<Double> nextDoubleOpt() throws IOException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as double and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public Double nextDoubleOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDouble(lowValue);
    }

    // decimal

    /**
     * get current column value as decimal and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public BigDecimal nextDecimal() throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public BigDecimal nextDecimal(BigDecimal defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<BigDecimal> nextDecimalOpt() throws IOException, TsurugiTransactionException {
        var value = nextDecimalOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as decimal and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public BigDecimal nextDecimalOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDecimal(lowValue);
    }

    // string

    /**
     * get current column value as String and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public String nextString() throws IOException, TsurugiTransactionException {
        var value = nextStringOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as String and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public String nextString(String defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextStringOrNull();
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<String> nextStringOpt() throws IOException, TsurugiTransactionException {
        var value = nextStringOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as String and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public String nextStringOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toString(lowValue);
    }

    // byte[]

    /**
     * get current column value as byte[] and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public byte[] nextBytes() throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public byte[] nextBytes(byte[] defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<byte[]> nextBytesOpt() throws IOException, TsurugiTransactionException {
        var value = nextBytesOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as byte[] and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public byte[] nextBytesOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toBytes(lowValue);
    }

    // boolean[]

    /**
     * get current column value as boolean[] and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public boolean[] nextBits() throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public boolean[] nextBits(boolean[] defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<boolean[]> nextBitsOpt() throws IOException, TsurugiTransactionException {
        var value = nextBitsOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as boolean[] and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public boolean[] nextBitsOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toBits(lowValue);
    }

    // date

    /**
     * get current column value as date and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public LocalDate nextDate() throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public LocalDate nextDate(LocalDate defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<LocalDate> nextDateOpt() throws IOException, TsurugiTransactionException {
        var value = nextDateOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as date and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public LocalDate nextDateOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDate(lowValue);
    }

    // time

    /**
     * get current column value as time and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public LocalTime nextTime() throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public LocalTime nextTime(LocalTime defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<LocalTime> nextTimeOpt() throws IOException, TsurugiTransactionException {
        var value = nextTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as time and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public LocalTime nextTimeOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toTime(lowValue);
    }

    // dateTime

    /**
     * get current column value as dateTime and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public LocalDateTime nextDateTime() throws IOException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as dateTime and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public LocalDateTime nextDateTime(LocalDateTime defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as dateTime and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<LocalDateTime> nextDateTimeOpt() throws IOException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as dateTime and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public LocalDateTime nextDateTimeOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDateTime(lowValue);
    }

    // offset time

    /**
     * get current column value as offset time and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public OffsetTime nextOffsetTime() throws IOException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as offset time and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public OffsetTime nextOffsetTime(OffsetTime defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as offset time and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<OffsetTime> nextOffsetTimeOpt() throws IOException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as offset time and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public OffsetTime nextOffsetTimeOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toOffsetTime(lowValue);
    }

    // offset dateTime

    /**
     * get current column value as offset dateTime and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public OffsetDateTime nextOffsetDateTime() throws IOException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as offset dateTime and move next column
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public OffsetDateTime nextOffsetDateTime(OffsetDateTime defaultValue) throws IOException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as offset dateTime and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<OffsetDateTime> nextOffsetDateTimeOpt() throws IOException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as offset dateTime and move next column
     *
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public OffsetDateTime nextOffsetDateTimeOrNull() throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toOffsetDateTime(lowValue);
    }

    // zoned dateTime

    /**
     * get current column value as ZonedDateTime and move next column
     *
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    @Nonnull
    public ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    public ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone, ZonedDateTime defaultValue) throws IOException, TsurugiTransactionException {
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
     * @throws TsurugiTransactionException
     */
    @Nonnull
    public Optional<ZonedDateTime> nextZonedDateTimeOpt(@Nonnull ZoneId zone) throws IOException, TsurugiTransactionException {
        var value = nextZonedDateTimeOrNull(zone);
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as ZonedDateTime and move next column
     *
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    @Nullable
    public ZonedDateTime nextZonedDateTimeOrNull(@Nonnull ZoneId zone) throws IOException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toZonedDateTime(lowValue, zone);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + lowResultSet + "}";
    }
}
