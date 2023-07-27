package com.tsurugidb.iceaxe.sql.parameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.tsubakuro.sql.Placeholders;

/**
 * Tsurugi Bind Variables for {@link TsurugiSqlPrepared}
 *
 * @see TgParameterMapping#of(TgBindVariables)
 */
public class TgBindVariables {

    /**
     * create bind variables
     *
     * @return bind variables
     */
    public static TgBindVariables of() {
        return new TgBindVariables();
    }

    /**
     * create bind variables
     *
     * @param variables bind variable
     * @return bind variables
     */
    public static TgBindVariables of(TgBindVariable<?>... variables) {
        var bv = new TgBindVariables();
        for (var variable : variables) {
            bv.add(variable);
        }
        return bv;
    }

    /**
     * create bind variables
     *
     * @param variables bind variable
     * @return bind variables
     */
    public static TgBindVariables of(Collection<? extends TgBindVariable<?>> variables) {
        var bv = new TgBindVariables();
        for (var variable : variables) {
            bv.add(variable);
        }
        return bv;
    }

    /**
     * convert to sql names
     *
     * @param variables bind variable
     * @return sql names
     */
    public static String toSqlNames(TgBindVariable<?>... variables) {
        return toSqlNames(",", variables);
    }

    /**
     * convert to sql names
     *
     * @param delimiter the delimiter to be used between each element
     * @param variables bind variable
     * @return sql names
     */
    public static String toSqlNames(String delimiter, TgBindVariable<?>... variables) {
        var sb = new StringBuilder();
        for (var variable : variables) {
            if (sb.length() != 0) {
                sb.append(delimiter);
            }
            sb.append(":");
            sb.append(variable.name());
        }
        return sb.toString();
    }

    /**
     * convert to sql names
     *
     * @param variables bind variable
     * @return sql names
     */
    public static String toSqlNames(Collection<? extends TgBindVariable<?>> variables) {
        return toSqlNames(",", variables);
    }

    /**
     * convert to sql names
     *
     * @param delimiter the delimiter to be used between each element
     * @param variables bind variable
     * @return sql names
     */
    public static String toSqlNames(String delimiter, Collection<? extends TgBindVariable<?>> variables) {
        var sb = new StringBuilder();
        for (var variable : variables) {
            if (sb.length() != 0) {
                sb.append(delimiter);
            }
            sb.append(":");
            sb.append(variable.name());
        }
        return sb.toString();
    }

    private final List<Placeholder> lowPlaceholderList = new ArrayList<>();
    /** Map&lt;name, type&gt; */
    private Map<String, TgDataType> typeMap;

    /**
     * add type(boolean)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addBoolean(@Nonnull String name) {
        addInternal(name, TgDataType.BOOLEAN);
        return this;
    }

    /**
     * add type(int)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addInt(@Nonnull String name) {
        addInternal(name, TgDataType.INT);
        return this;
    }

    /**
     * add type(long)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addLong(@Nonnull String name) {
        addInternal(name, TgDataType.LONG);
        return this;
    }

    /**
     * add type(float)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addFloat(@Nonnull String name) {
        addInternal(name, TgDataType.FLOAT);
        return this;
    }

    /**
     * add type(double)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addDouble(@Nonnull String name) {
        addInternal(name, TgDataType.DOUBLE);
        return this;
    }

    /**
     * add type(decimal)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addDecimal(@Nonnull String name) {
        addInternal(name, TgDataType.DECIMAL);
        return this;
    }

    /**
     * add type(String)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addString(@Nonnull String name) {
        addInternal(name, TgDataType.STRING);
        return this;
    }

    /**
     * add type(byte[])
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addBytes(@Nonnull String name) {
        addInternal(name, TgDataType.BYTES);
        return this;
    }

    /**
     * add type(boolean[])
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addBits(@Nonnull String name) {
        addInternal(name, TgDataType.BITS);
        return this;
    }

    /**
     * add type(date)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addDate(@Nonnull String name) {
        addInternal(name, TgDataType.DATE);
        return this;
    }

    /**
     * add type(time)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addTime(@Nonnull String name) {
        addInternal(name, TgDataType.TIME);
        return this;
    }

    /**
     * add type(dateTime)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addDateTime(@Nonnull String name) {
        addInternal(name, TgDataType.DATE_TIME);
        return this;
    }

    /**
     * add type(offset time)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addOffsetTime(@Nonnull String name) {
        addInternal(name, TgDataType.OFFSET_TIME);
        return this;
    }

    /**
     * add type(offset dateTime)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addOffsetDateTime(@Nonnull String name) {
        addInternal(name, TgDataType.OFFSET_DATE_TIME);
        return this;
    }

    /**
     * add type(zoned dateTime)
     *
     * @param name name
     * @return this
     */
    public TgBindVariables addZonedDateTime(@Nonnull String name) {
        addInternal(name, TgDataType.ZONED_DATE_TIME);
        return this;
    }

    /**
     * add type
     *
     * @param name name
     * @param type data type
     * @return this
     */
    public TgBindVariables add(@Nonnull String name, @Nonnull TgDataType type) {
        addInternal(name, type);
        return this;
    }

    /**
     * add type
     *
     * @param name name
     * @param type data type
     * @return this
     */
    public TgBindVariables add(@Nonnull String name, @Nonnull Class<?> type) {
        var tgType = getDataType(type);
        addInternal(name, tgType);
        return this;
    }

    protected TgDataType getDataType(Class<?> type) {
        var tgType = TgDataType.of(type);
        if (tgType == null) {
            throw new IllegalArgumentException("unsupported data type. type=" + type);
        }
        return tgType;
    }

    /**
     * add variable
     *
     * @param variable variable
     * @return this
     */
    public TgBindVariables add(@Nonnull TgBindVariable<?> variable) {
        var name = variable.name();
        var type = variable.type();
        addInternal(name, type);
        return this;
    }

    protected final void addInternal(@Nonnull String name, @Nonnull TgDataType type) {
        var lowPlaceholder = Placeholders.of(name, type.getLowDataType());
        lowPlaceholderList.add(lowPlaceholder);
        this.typeMap = null;
    }

    /**
     * add variable
     *
     * @param otherList variable list
     * @return this
     */
    public TgBindVariables add(@Nonnull TgBindVariables otherList) {
        for (var lowPlaceholder : otherList.lowPlaceholderList) {
            lowPlaceholderList.add(lowPlaceholder);
        }
        this.typeMap = null;
        return this;
    }

    /**
     * get sql names
     *
     * @return sql names
     */
    public String getSqlNames() {
        return getSqlNames(",");
    }

    /**
     * get sql names
     *
     * @param delimiter the delimiter to be used between each element
     * @return sql names
     */
    public String getSqlNames(String delimiter) {
        return lowPlaceholderList.stream().map(ph -> ":" + ph.getName()).collect(Collectors.joining(delimiter));
    }

    /**
     * convert to {@link Placeholder} list
     *
     * @return placeholder list
     */
    @IceaxeInternal
    public List<Placeholder> toLowPlaceholderList() {
        return this.lowPlaceholderList;
    }

    /**
     * get data type
     *
     * @param name name
     * @return data type
     */
    public TgDataType getDataType(@Nonnull String name) {
        if (this.typeMap == null) {
            synchronized (this) {
                var map = new HashMap<String, TgDataType>();
                for (var lowPlaceholder : lowPlaceholderList) {
                    var lowName = lowPlaceholder.getName();
                    var lowType = lowPlaceholder.getAtomType();
                    var type = TgDataType.of(lowType);
                    map.put(lowName, type);
                }
                this.typeMap = map;
            }
        }

        var type = typeMap.get(name);
        if (type == null) {
            throw new IllegalArgumentException("not found type. name=" + name);
        }
        return type;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + lowPlaceholderList;
    }
}
