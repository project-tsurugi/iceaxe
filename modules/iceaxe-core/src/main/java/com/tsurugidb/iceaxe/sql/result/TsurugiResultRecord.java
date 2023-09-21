package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.MessageFormat;
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
 * Tsurugi Result Record for {@link TsurugiQueryResult}.
 *
 * <p>
 * The methods of this class are classified into the following three groups. If you use a certain group of methods, you basically cannot use the other group's methods.
 * </p>
 * <ul>
 * <li><code>current column</code> group
 * <ul>
 * <li>Get the value while moving the current column by {@link #moveCurrentColumnNext()}.<br>
 * Each column value can be retrieved only once.</li>
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
 * <li><code>name</code> group
 * <ul>
 * <li>Get the value by specifying the column name.</li>
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
 * <li><code>next</code> group</li>
 * <ul>
 * <li>Move to the next column and get the value of the current column.<br>
 * Each column value can be retrieved only once.<br>
 * {@link #getCurrentColumnName()} and {@link #getCurrentColumnType()} can be used immediately after calling the next group method.</li>
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
 * This class is linked with {@link TsurugiQueryResult} and the instance is shared among multiple records.<br>
 * Therefore, this instance must not be held by the user program for the purpose of holding the value of the record.<br>
 * Also, this class cannot be used after closing {@link TsurugiQueryResult}.
 * </p>
 */
@NotThreadSafe
public class TsurugiResultRecord {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResultRecord.class);

    /**
     * column value.
     */
    protected /* record */ static class TsurugiResultColumnValue {
        private final int index;
        private final Object value;

        /**
         * Creates a new instance.
         *
         * @param index column index
         * @param value column value
         */
        public TsurugiResultColumnValue(int index, Object value) {
            this.index = index;
            this.value = value;
        }

        /**
         * get column index.
         *
         * @return column index
         */
        public int index() {
            return index;
        }

        /**
         * get column value.
         *
         * @return column value
         */
        public Object value() {
            return value;
        }
    }

    private final TsurugiQueryResult<?> ownerResult;
    private final ResultSet lowResultSet;
    private final IceaxeConvertUtil convertUtil;
    private int currentColumnIndex;
    private Map<String, TsurugiResultColumnValue> columnMap;

    /**
     * Creates a new instance.
     *
     * @param result       query result
     * @param lowResultSet low ResultSet
     * @param convertUtil  convert type utility
     */
    protected TsurugiResultRecord(TsurugiQueryResult<?> result, ResultSet lowResultSet, IceaxeConvertUtil convertUtil) {
        this.ownerResult = result;
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

    /**
     * get convert type utility.
     *
     * @return convert type utility
     */
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    /*
     * current column
     */

    /**
     * move the current column to the next.
     *
     * @return true if the next column exists
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @see #getCurrentColumnName()
     * @see #getCurrentColumnType()
     * @see #fetchCurrentColumnValue()
     */
    public boolean moveCurrentColumnNext() throws IOException, InterruptedException, TsurugiTransactionException {
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
            throw ownerResult.fillToTsurugiException(new TsurugiTransactionException(e));
        }
    }

    /**
     * get low column.
     *
     * @param index column index
     * @return low column
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    protected @Nonnull Column getLowColumn(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowColumnList = TsurugiQueryResult.getLowColumnList(ownerResult, lowResultSet);
        return lowColumnList.get(index);
    }

    /**
     * get current column name.
     *
     * @return column name
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @see #moveCurrentColumnNext()
     */
    public @Nonnull String getCurrentColumnName() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowColumn = getLowColumn(currentColumnIndex);
        return TsurugiQueryResult.getColumnName(lowColumn, currentColumnIndex);
    }

    /**
     * get current column data type.
     *
     * @return data type
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @see #moveCurrentColumnNext()
     */
    public @Nonnull TgDataType getCurrentColumnType() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowType = getCurrentColumnLowType();
        return TgDataType.of(lowType);
    }

    /**
     * get current column low type.
     *
     * @return low data type
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    protected @Nonnull AtomType getCurrentColumnLowType() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowColumn = getLowColumn(currentColumnIndex);
        return lowColumn.getAtomType();
    }

    /**
     * get current column value (take once).
     *
     * @return value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @see #moveCurrentColumnNext()
     */
    public @Nullable Object fetchCurrentColumnValue() throws IOException, InterruptedException, TsurugiTransactionException {
        if (lowResultSet.isNull()) {
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
            case TIME_OF_DAY_WITH_TIME_ZONE:
                return lowResultSet.fetchTimeOfDayWithTimeZoneValue();
            case TIME_POINT_WITH_TIME_ZONE:
                return lowResultSet.fetchTimePointWithTimeZoneValue();
            default:
                throw new UnsupportedOperationException("unsupported type error. lowType=" + lowType);
            }
        } catch (ServerException e) {
            throw ownerResult.fillToTsurugiException(new TsurugiTransactionException(e));
        }
    }

    /*
     * get by name
     */

    /**
     * get name list.
     *
     * @return list of column name
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull List<String> getNameList() throws IOException, InterruptedException, TsurugiTransactionException {
        return TsurugiQueryResult.getNameList(ownerResult, lowResultSet);
    }

    /**
     * get column value map.
     *
     * @return column value map
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    protected @Nonnull Map<String, TsurugiResultColumnValue> getColumnMap() throws IOException, InterruptedException, TsurugiTransactionException {
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

    /**
     * get column value.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    protected @Nonnull TsurugiResultColumnValue getColumn(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var map = getColumnMap();
        var column = map.get(name);
        if (column == null) {
            throw new IllegalArgumentException("not found column. name=" + name);
        }
        return column;
    }

    /**
     * get value.
     *
     * @param name column name
     * @return value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Object getValue(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var column = getColumn(name);
        return column.value();
    }

    /**
     * get data type.
     *
     * @param name column name
     * @return data type
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull TgDataType getType(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var column = getColumn(name);
        var lowColumn = getLowColumn(column.index());
        var lowType = lowColumn.getAtomType();
        return TgDataType.of(lowType);
    }

    private static void requireNonNull(String name, Object value, String getterName) {
        Objects.requireNonNull(value, () -> MessageFormat.format("TsurugiResultRecord.{0}({1}) is null", getterName, name));
    }

    // boolean

    /**
     * get column value as boolean.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public boolean getBoolean(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBooleanOrNull(name);
        requireNonNull(name, value, "getBoolean");
        return value;
    }

    /**
     * get column value as boolean.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public boolean getBoolean(String name, boolean defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Boolean> findBoolean(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBooleanOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Boolean getBooleanOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toBoolean(lowValue);
    }

    // int

    /**
     * get column value as int.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public int getInt(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getIntOrNull(name);
        requireNonNull(name, value, "getInt");
        return value;
    }

    /**
     * get column value as int.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public int getInt(String name, int defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Integer> findInt(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getIntOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as int.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Integer getIntOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toInt(lowValue);
    }

    // long

    /**
     * get column value as long.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public long getLong(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getLongOrNull(name);
        requireNonNull(name, value, "getLong");
        return value;
    }

    /**
     * get column value as long.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public long getLong(String name, long defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Long> findLong(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getLongOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as long.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Long getLongOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toLong(lowValue);
    }

    // float

    /**
     * get column value as float.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public float getFloat(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getFloatOrNull(name);
        requireNonNull(name, value, "getFloat");
        return value;
    }

    /**
     * get column value as float.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public float getFloat(String name, float defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Float> findFloat(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getFloatOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as float.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Float getFloatOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toFloat(lowValue);
    }

    // double

    /**
     * get column value as double.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public double getDouble(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDoubleOrNull(name);
        requireNonNull(name, value, "getDouble");
        return value;
    }

    /**
     * get column value as double.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public double getDouble(String name, float defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Double> findDouble(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDoubleOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as double.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Double getDoubleOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDouble(lowValue);
    }

    // decimal

    /**
     * get column value as decimal.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull BigDecimal getDecimal(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDecimalOrNull(name);
        requireNonNull(name, value, "getDecimal");
        return value;
    }

    /**
     * get column value as decimal.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public BigDecimal getDecimal(String name, BigDecimal defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<BigDecimal> findDecimal(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDecimalOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as decimal.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable BigDecimal getDecimalOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDecimal(lowValue);
    }

    // string

    /**
     * get column value as String.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull String getString(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getStringOrNull(name);
        requireNonNull(name, value, "getString");
        return value;
    }

    /**
     * get column value as String.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public String getString(String name, String defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<String> findString(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getStringOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as String.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable String getStringOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toString(lowValue);
    }

    // byte[]

    /**
     * get column value as byte[].
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull byte[] getBytes(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBytesOrNull(name);
        requireNonNull(name, value, "getBytes");
        return value;
    }

    /**
     * get column value as byte[].
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public byte[] getBytes(String name, byte[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<byte[]> findBytes(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBytesOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as byte[].
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable byte[] getBytesOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toBytes(lowValue);
    }

    // boolean[]

    /**
     * get column value as boolean[].
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull boolean[] getBits(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBitsOrNull(name);
        requireNonNull(name, value, "getBits");
        return value;
    }

    /**
     * get column value as boolean[].
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public boolean[] getBits(String name, boolean[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<boolean[]> findBits(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getBitsOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as boolean[].
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable boolean[] getBitsOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toBits(lowValue);
    }

    // date

    /**
     * get column value as date.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull LocalDate getDate(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateOrNull(name);
        requireNonNull(name, value, "getDate");
        return value;
    }

    /**
     * get column value as date.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public LocalDate getDate(String name, LocalDate defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<LocalDate> findDate(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as date.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable LocalDate getDateOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDate(lowValue);
    }

    // time

    /**
     * get column value as time.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull LocalTime getTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getTimeOrNull(name);
        requireNonNull(name, value, "getTime");
        return value;
    }

    /**
     * get column value as time.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public LocalTime getTime(String name, LocalTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<LocalTime> findTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as time.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable LocalTime getTimeOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toTime(lowValue);
    }

    // date time

    /**
     * get column value as dateTime.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull LocalDateTime getDateTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateTimeOrNull(name);
        requireNonNull(name, value, "getDateTime");
        return value;
    }

    /**
     * get column value as dateTime.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public LocalDateTime getDateTime(String name, LocalDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<LocalDateTime> findDateTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as dateTime.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable LocalDateTime getDateTimeOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toDateTime(lowValue);
    }

    // offset time

    /**
     * get column value as offset time.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull OffsetTime getOffsetTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(name);
        requireNonNull(name, value, "getOffsetTime");
        return value;
    }

    /**
     * get column value as offset time.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public OffsetTime getOffsetTime(String name, OffsetTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<OffsetTime> findOffsetTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as offset time.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable OffsetTime getOffsetTimeOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toOffsetTime(lowValue);
    }

    // offset dateTime

    /**
     * get column value as offset dateTime.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull OffsetDateTime getOffsetDateTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(name);
        requireNonNull(name, value, "getOffsetDateTime");
        return value;
    }

    /**
     * get column value as offset dateTime.
     *
     * @param name         column name
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public OffsetDateTime getOffsetDateTime(String name, OffsetDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<OffsetDateTime> findOffsetDateTime(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getOffsetDateTimeOrNull(name);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as offset dateTime.
     *
     * @param name column name
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable OffsetDateTime getOffsetDateTimeOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toOffsetDateTime(lowValue);
    }

    // zoned dateTime

    /**
     * get column value as ZonedDateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull ZonedDateTime getZonedDateTime(String name, ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getZonedDateTimeOrNull(name, zone);
        requireNonNull(name, value, "getZonedDateTime");
        return value;
    }

    /**
     * get column value as ZonedDateTime.
     *
     * @param name         column name
     * @param zone         time-zone
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public ZonedDateTime getZonedDateTime(String name, ZoneId zone, ZonedDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
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
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<ZonedDateTime> findZonedDateTime(String name, ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = getZonedDateTimeOrNull(name, zone);
        return Optional.ofNullable(value);
    }

    /**
     * get column value as ZonedDateTime.
     *
     * @param name column name
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable ZonedDateTime getZonedDateTimeOrNull(String name, ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = getValue(name);
        return convertUtil.toZonedDateTime(lowValue, zone);
    }

    /*
     * next
     */

    /**
     * move next column.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws IllegalStateException       if not found next column
     */
    public void nextColumn() throws IOException, InterruptedException, TsurugiTransactionException {
        boolean exists = moveCurrentColumnNext();
        if (!exists) {
            throw new IllegalStateException("not found next column");
        }
    }

    /**
     * get current column value and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    protected @Nullable Object nextLowValue() throws IOException, InterruptedException, TsurugiTransactionException {
        nextColumn();
        return fetchCurrentColumnValue();
    }

    private void requireNonNull(Object value, String getterName) {
        Objects.requireNonNull(value, () -> MessageFormat.format("TsurugiResultRecord.{0}({1}) is null", getterName, currentColumnIndex));
    }

    // boolean

    /**
     * get current column value as boolean and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public boolean nextBoolean() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBooleanOrNull();
        requireNonNull(value, "nextBoolean");
        return value;
    }

    /**
     * get current column value as boolean and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public boolean nextBoolean(boolean defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBooleanOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as boolean and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Boolean> nextBooleanOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBooleanOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as boolean and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Boolean nextBooleanOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toBoolean(lowValue);
    }

    // int

    /**
     * get current column value as int and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public int nextInt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextIntOrNull();
        requireNonNull(value, "nextInt");
        return value;
    }

    /**
     * get current column value as int and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public int nextInt(int defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextIntOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as int and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Integer> nextIntOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextIntOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as int and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Integer nextIntOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toInt(lowValue);
    }

    // long

    /**
     * get current column value as long and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public long nextLong() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextLongOrNull();
        requireNonNull(value, "nextLong");
        return value;
    }

    /**
     * get current column value as long and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public long nextLong(long defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextLongOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as long and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Long> nextLongOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextLongOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as long and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Long nextLongOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toLong(lowValue);
    }

    // float

    /**
     * get current column value as float and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public float nextFloat() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        requireNonNull(value, "nextFloat");
        return value;
    }

    /**
     * get current column value as float and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public float nextFloat(int defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as float and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Float> nextFloatOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextFloatOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as float and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Float nextFloatOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toFloat(lowValue);
    }

    // double

    /**
     * get current column value as double and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public double nextDouble() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        requireNonNull(value, "nextDouble");
        return value;
    }

    /**
     * get current column value as double and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public double nextDouble(int defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as double and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<Double> nextDoubleOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDoubleOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as double and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable Double nextDoubleOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDouble(lowValue);
    }

    // decimal

    /**
     * get current column value as decimal and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull BigDecimal nextDecimal() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDecimalOrNull();
        requireNonNull(value, "nextDecimal");
        return value;
    }

    /**
     * get current column value as decimal and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public BigDecimal nextDecimal(BigDecimal defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDecimalOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as decimal and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<BigDecimal> nextDecimalOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDecimalOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as decimal and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable BigDecimal nextDecimalOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDecimal(lowValue);
    }

    // string

    /**
     * get current column value as String and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull String nextString() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextStringOrNull();
        requireNonNull(value, "nextString");
        return value;
    }

    /**
     * get current column value as String and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public String nextString(String defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextStringOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as String and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<String> nextStringOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextStringOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as String and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable String nextStringOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toString(lowValue);
    }

    // byte[]

    /**
     * get current column value as byte[] and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull byte[] nextBytes() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBytesOrNull();
        requireNonNull(value, "nextBytes");
        return value;
    }

    /**
     * get current column value as byte[] and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public byte[] nextBytes(byte[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBytesOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as byte[] and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<byte[]> nextBytesOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBytesOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as byte[] and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable byte[] nextBytesOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toBytes(lowValue);
    }

    // boolean[]

    /**
     * get current column value as boolean[] and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull boolean[] nextBits() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBitsOrNull();
        requireNonNull(value, "nextBits");
        return value;
    }

    /**
     * get current column value as boolean[] and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public boolean[] nextBits(boolean[] defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBitsOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as boolean[] and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<boolean[]> nextBitsOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextBitsOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as boolean[] and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable boolean[] nextBitsOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toBits(lowValue);
    }

    // date

    /**
     * get current column value as date and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull LocalDate nextDate() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateOrNull();
        requireNonNull(value, "nextDate");
        return value;
    }

    /**
     * get current column value as date and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public LocalDate nextDate(LocalDate defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as date and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<LocalDate> nextDateOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as date and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable LocalDate nextDateOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDate(lowValue);
    }

    // time

    /**
     * get current column value as time and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull LocalTime nextTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextTimeOrNull();
        requireNonNull(value, "nextTime");
        return value;
    }

    /**
     * get current column value as time and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public LocalTime nextTime(LocalTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as time and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<LocalTime> nextTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as time and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable LocalTime nextTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toTime(lowValue);
    }

    // dateTime

    /**
     * get current column value as dateTime and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull LocalDateTime nextDateTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        requireNonNull(value, "nextDateTime");
        return value;
    }

    /**
     * get current column value as dateTime and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public LocalDateTime nextDateTime(LocalDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as dateTime and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<LocalDateTime> nextDateTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextDateTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as dateTime and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable LocalDateTime nextDateTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toDateTime(lowValue);
    }

    // offset time

    /**
     * get current column value as offset time and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull OffsetTime nextOffsetTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        requireNonNull(value, "nextOffsetTime");
        return value;
    }

    /**
     * get current column value as offset time and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public OffsetTime nextOffsetTime(OffsetTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as offset time and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<OffsetTime> nextOffsetTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as offset time and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable OffsetTime nextOffsetTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toOffsetTime(lowValue);
    }

    // offset dateTime

    /**
     * get current column value as offset dateTime and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull OffsetDateTime nextOffsetDateTime() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        requireNonNull(value, "nextOffsetDateTime");
        return value;
    }

    /**
     * get current column value as offset dateTime and move next column.
     *
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public OffsetDateTime nextOffsetDateTime(OffsetDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as offset dateTime and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<OffsetDateTime> nextOffsetDateTimeOpt() throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextOffsetDateTimeOrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as offset dateTime and move next column.
     *
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable OffsetDateTime nextOffsetDateTimeOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toOffsetDateTime(lowValue);
    }

    // zoned dateTime

    /**
     * get current column value as ZonedDateTime and move next column.
     *
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     * @throws NullPointerException        if value is null
     */
    public @Nonnull ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextZonedDateTimeOrNull(zone);
        requireNonNull(value, "nextZonedDateTime");
        return value;
    }

    /**
     * get current column value as ZonedDateTime and move next column.
     *
     * @param zone         time-zone
     * @param defaultValue value to return if column value is null
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public ZonedDateTime nextZonedDateTime(@Nonnull ZoneId zone, ZonedDateTime defaultValue) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextZonedDateTimeOrNull(zone);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * get current column value as ZonedDateTime and move next column.
     *
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nonnull Optional<ZonedDateTime> nextZonedDateTimeOpt(@Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var value = nextZonedDateTimeOrNull(zone);
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as ZonedDateTime and move next column.
     *
     * @param zone time-zone
     * @return column value
     * @throws IOException
     * @throws InterruptedException
     * @throws TsurugiTransactionException
     */
    public @Nullable ZonedDateTime nextZonedDateTimeOrNull(@Nonnull ZoneId zone) throws IOException, InterruptedException, TsurugiTransactionException {
        var lowValue = nextLowValue();
        return convertUtil.toZonedDateTime(lowValue, zone);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + lowResultSet + "}";
    }
}
