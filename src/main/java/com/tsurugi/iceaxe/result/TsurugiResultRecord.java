package com.tsurugi.iceaxe.result;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.tsurugi.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Record for PreparedStatement
 */
public class TsurugiResultRecord {

    /** Map&lt;name, value&gt; */
    private final Map<String, Object> valueMap = new LinkedHashMap<>();
    private Iterator<Object> valueIterator;

    // internal
    public void add(String name, Object value) {
        valueMap.put(name, value);
    }

    protected Object getLowValue(String name) {
        var value = valueMap.get(name);
        if (value == null) {
            if (!valueMap.containsKey(name)) {
                throw new IllegalArgumentException("not found column. name=" + name);
            }
        }
        return value;
    }

    protected Iterator<Object> getIterator() {
        if (this.valueIterator == null) {
            this.valueIterator = valueMap.values().iterator();
        }
        return this.valueIterator;
    }

    public boolean hasNext() {
        return getIterator().hasNext();
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
        var lowValue = getLowValue(name);
        return IceaxeConvertUtil.toInt4(lowValue);
    }

    /**
     * get current column value as int and move next column
     * 
     * @return column value
     * @throws NullPointerException if value is null
     */
    public int nextInt4() {
        var value = nextInt4OrNull();
        Objects.requireNonNull(value);
        return value;
    }

    /**
     * get current column value as int and move next column
     * 
     * @param defaultValue value to return if column value is null
     * @return column value
     */
    public int nextInt4(int defaultValue) {
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
     */
    public Optional<Integer> nextInt4Opt() {
        var value = nextInt4OrNull();
        return Optional.ofNullable(value);
    }

    /**
     * get current column value as int and move next column
     * 
     * @return column value
     */
    public Integer nextInt4OrNull() {
        var lowValue = getIterator().next();
        return IceaxeConvertUtil.toInt4(lowValue);
    }

    // TODO int8, float4, float8, character
}
