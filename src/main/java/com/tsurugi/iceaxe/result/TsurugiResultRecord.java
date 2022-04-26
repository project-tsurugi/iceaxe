package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.tsurugi.iceaxe.statement.TgDataType;
import com.tsurugi.iceaxe.util.IceaxeConvertUtil;

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
 * 各カラムの値は一度しか取得できない。</li>
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

    /* current column */

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
    public Object getCurrentColumnValue() throws IOException {
        if (lowResultSet.isNull()) {
            return null;
        }
        var lowType = lowResultSet.type();
        switch (lowType) {
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

    /* get by name */

    /**
     * get name list
     * 
     * @return list of column name
     * @throws IOException
     */
    public List<String> getNameList() throws IOException {
        return TsurugiResultSet.getNameList(lowResultSet);
    }

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
    public TgDataType getType(String name) throws IOException {
        var column = getColumn(name);
        var lowMeta = lowResultSet.getRecordMeta();
        var lowType = lowMeta.type(column.index());
        return TgDataType.of(lowType);
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
    public Integer getInt4OrNull(String name) throws IOException {
        var lowValue = getValue(name);
        return IceaxeConvertUtil.toInt4(lowValue);
    }

    // TODO int8, float4, float8, character

    /* next */

    protected Object nextLowValue() throws IOException {
        var hasColumn = lowResultSet.nextColumn();
        if (hasColumn) {
            return getCurrentColumnValue();
        }
        throw new IllegalStateException("not found next column");
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
    public Integer nextInt4OrNull() throws IOException {
        var lowValue = nextLowValue();
        return IceaxeConvertUtil.toInt4(lowValue);
    }

    // TODO int8, float4, float8, character

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + lowResultSet + "}";
    }
}
