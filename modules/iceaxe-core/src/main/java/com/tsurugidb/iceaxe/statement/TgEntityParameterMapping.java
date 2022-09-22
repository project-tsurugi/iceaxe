package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;;

/**
 * Tsurugi Parameter Mapping for Entity
 * 
 * @param <P> parameter type (e.g. Entity)
 */
public class TgEntityParameterMapping<P> extends TgParameterMapping<P> {

    /**
     * create Parameter Mapping
     * 
     * @param <P>   parameter type
     * @param clazz parameter class
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgEntityParameterMapping<P> of() {
        return new TgEntityParameterMapping<>();
    }

    /**
     * create Parameter Mapping
     * 
     * @param <P>   parameter type
     * @param clazz parameter class
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgEntityParameterMapping<P> of(Class<P> clazz) {
        return new TgEntityParameterMapping<>();
    }

    private final List<Placeholder> lowPlaceholderList = new ArrayList<>();
    private final List<BiFunction<P, IceaxeConvertUtil, Parameter>> parameterConverterList = new ArrayList<>();

    /**
     * Tsurugi Parameter Mapping
     */
    public TgEntityParameterMapping() {
        // do nothing
    }

    /**
     * set convert type utility
     * 
     * @param convertUtil convert type utility
     * @return this
     */
    public TgEntityParameterMapping<P> convertUtil(IceaxeConvertUtil convertUtil) {
        setConvertUtil(convertUtil);
        return this;
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> bool(String name, Function<P, Boolean> getter) {
        addVariable(name, TgDataType.BOOLEAN);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> bool(String name, Function<P, V> getter, Function<V, Boolean> converter) {
        return bool(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> int4(String name, Function<P, Integer> getter) {
        addVariable(name, TgDataType.INT4);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> int4(String name, Function<P, V> getter, Function<V, Integer> converter) {
        return int4(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> int8(String name, Function<P, Long> getter) {
        addVariable(name, TgDataType.INT8);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> int8(String name, Function<P, V> getter, Function<V, Long> converter) {
        return int8(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> float4(String name, Function<P, Float> getter) {
        addVariable(name, TgDataType.FLOAT4);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> float4(String name, Function<P, V> getter, Function<V, Float> converter) {
        return float4(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> float8(String name, Function<P, Double> getter) {
        addVariable(name, TgDataType.FLOAT8);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> float8(String name, Function<P, V> getter, Function<V, Double> converter) {
        return float8(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> decimal(String name, Function<P, BigDecimal> getter) {
        addVariable(name, TgDataType.DECIMAL);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> decimal(String name, Function<P, V> getter, Function<V, BigDecimal> converter) {
        return decimal(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> character(String name, Function<P, String> getter) {
        addVariable(name, TgDataType.CHARACTER);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> character(String name, Function<P, V> getter, Function<V, String> converter) {
        return character(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> bytes(String name, Function<P, byte[]> getter) {
        addVariable(name, TgDataType.BYTES);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> bytes(String name, Function<P, V> getter, Function<V, byte[]> converter) {
        return bytes(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> bits(String name, Function<P, boolean[]> getter) {
        addVariable(name, TgDataType.BITS);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> bits(String name, Function<P, V> getter, Function<V, boolean[]> converter) {
        return bits(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> date(String name, Function<P, LocalDate> getter) {
        addVariable(name, TgDataType.DATE);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> date(String name, Function<P, V> getter, Function<V, LocalDate> converter) {
        return date(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> time(String name, Function<P, LocalTime> getter) {
        addVariable(name, TgDataType.DATE);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> time(String name, Function<P, V> getter, Function<V, LocalTime> converter) {
        return time(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> offsetTime(String name, Function<P, OffsetTime> getter) {
        addVariable(name, TgDataType.OFFSET_TIME);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> offsetTime(String name, Function<P, V> getter, Function<V, OffsetTime> converter) {
        return offsetTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> dateTime(String name, Function<P, LocalDateTime> getter) {
        addVariable(name, TgDataType.DATE_TIME);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> dateTime(String name, Function<P, V> getter, Function<V, LocalDateTime> converter) {
        return dateTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> offsetDateTime(String name, Function<P, OffsetDateTime> getter) {
        addVariable(name, TgDataType.OFFSET_DATE_TIME);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> offsetDateTime(String name, Function<P, V> getter, Function<V, OffsetDateTime> converter) {
        return offsetDateTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param getter getter from parameter
     * @param offset time-offset
     * @return this
     */
    public TgEntityParameterMapping<P> zonedDateTime(String name, Function<P, ZonedDateTime> getter) {
        addVariable(name, TgDataType.OFFSET_DATE_TIME);
        parameterConverterList.add((parameter, convertUtil) -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param name     name
     * @param getter   getter from parameter
     * @param conveter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> zonedDateTime(String name, Function<P, V> getter, Function<V, ZonedDateTime> converter) {
        return zonedDateTime(name, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param type   type
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> add(String name, TgDataType type, Function<P, ?> getter) {
        addVariable(name, type);
        switch (type) {
        case BOOLEAN:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toBoolean(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case INT4:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toInt4(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case INT8:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toInt8(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case FLOAT4:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toFloat4(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case FLOAT8:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toFloat8(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case DECIMAL:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toDecimal(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case CHARACTER:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toCharacter(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case BYTES:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toBytes(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case BITS:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toBits(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case DATE:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toDate(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case TIME:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case DATE_TIME:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toDateTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case OFFSET_TIME:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toOffsetTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        case OFFSET_DATE_TIME:
            parameterConverterList.add((parameter, convertUtil) -> {
                var value = convertUtil.toOffsetDateTime(getter.apply(parameter));
                return IceaxeLowParameterUtil.create(name, value);
            });
            return this;
        default:
            throw new UnsupportedOperationException("unsupported type error. type=" + type);
        }
    }

    /**
     * add variable
     * 
     * @param name      name
     * @param type      type
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> add(String name, TgDataType type, Function<P, V> getter, Function<V, ?> converter) {
        return add(name, type, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param <V>    value type
     * @param name   name
     * @param type   type
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> add(String name, Class<?> type, Function<P, ?> getter) {
        var tgType = TgDataType.of(type);
        return add(name, tgType, getter);
    }

    /**
     * add variable
     * 
     * @param name      name
     * @param type      type
     * @param getter    getter from parameter
     * @param converter converter to database data type
     * @return this
     */
    public <V> TgEntityParameterMapping<P> add(String name, Class<?> type, Function<P, V> getter, Function<V, ?> converter) {
        return add(name, type, p -> {
            V value = getter.apply(p);
            return (value != null) ? converter.apply(value) : null;
        });
    }

    /**
     * add variable
     * 
     * @param <V>      value type
     * @param variable variable
     * @param getter   getter from parameter
     * @return this
     */
    public <V> TgEntityParameterMapping<P> add(TgVariable<V> variable, Function<P, V> getter) {
        // return add(variable.name(), variable.type(), getter);
        addVariable(variable.name(), variable.type());
        parameterConverterList.add((parameter, convertUtil) -> {
            V value = getter.apply(parameter);
            return variable.bind(value).toLowParameter();
        });
        return this;
    }

    protected void addVariable(String name, TgDataType type) {
        var lowVariable = Placeholder.newBuilder().setName(name).setAtomType(type.getLowDataType()).build();
        lowPlaceholderList.add(lowVariable);
    }

    @Override
    public List<Placeholder> toLowPlaceholderList() {
        return lowPlaceholderList;
    }

    @Override
    protected List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil) {
        var list = new ArrayList<Parameter>(parameterConverterList.size());
        for (var converter : parameterConverterList) {
            list.add(converter.apply(parameter, convertUtil));
        }
        return list;
    }
}
