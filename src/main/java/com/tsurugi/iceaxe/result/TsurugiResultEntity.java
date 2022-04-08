package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.tsurugi.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Result Record for PreparedStatement
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

    // TODO int8, float4, float8, character

    @Override
    public String toString() {
        return getClass().getSimpleName() + valueMap;
    }
}
