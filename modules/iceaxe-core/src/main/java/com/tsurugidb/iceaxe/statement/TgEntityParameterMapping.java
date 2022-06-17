package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;
import com.tsurugidb.jogasaki.proto.SqlRequest.PlaceHolder;

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
    public static <P> TgEntityParameterMapping<P> of(Class<P> clazz) {
        return new TgEntityParameterMapping<>();
    }

    private final List<PlaceHolder> lowPlaceHolderList = new ArrayList<>();
    private final List<Function<P, Parameter>> parameterConverterList = new ArrayList<>();

    /**
     * Tsurugi Parameter Mapping
     */
    public TgEntityParameterMapping() {
        // do nothing
    }

    /**
     * add variable(boolean)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> bool(String name, Function<P, Boolean> getter) {
        addVariable(name, TgDataType.BOOLEAN);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(int)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> int4(String name, Function<P, Integer> getter) {
        addVariable(name, TgDataType.INT4);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(long)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> int8(String name, Function<P, Long> getter) {
        addVariable(name, TgDataType.INT8);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(float)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> float4(String name, Function<P, Float> getter) {
        addVariable(name, TgDataType.FLOAT4);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(double)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> float8(String name, Function<P, Double> getter) {
        addVariable(name, TgDataType.FLOAT8);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(decimal)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> decimal(String name, Function<P, BigDecimal> getter) {
        addVariable(name, TgDataType.DECIMAL);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(String)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> character(String name, Function<P, String> getter) {
        addVariable(name, TgDataType.CHARACTER);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(byte[])
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> bytes(String name, Function<P, byte[]> getter) {
        addVariable(name, TgDataType.BYTES);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(boolean[])
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> bits(String name, Function<P, boolean[]> getter) {
        addVariable(name, TgDataType.BITS);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(date)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> date(String name, Function<P, LocalDate> getter) {
        addVariable(name, TgDataType.DATE);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(time)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> time(String name, Function<P, LocalTime> getter) {
        addVariable(name, TgDataType.DATE);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(Instant)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> instant(String name, Function<P, Instant> getter) {
        addVariable(name, TgDataType.INSTANT);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable(ZonedDateTime)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @param offset time-offset
     * @return this
     */
    public TgEntityParameterMapping<P> zonedDateTime(String name, Function<P, ZonedDateTime> getter) {
        addVariable(name, TgDataType.INSTANT);
        parameterConverterList.add(parameter -> {
            var value = getter.apply(parameter);
            return IceaxeLowParameterUtil.create(name, value);
        });
        return this;
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param type   type
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> add(String name, TgDataType type, Function<P, Object> getter) {
        addVariable(name, type);
        switch (type) {
        case BOOLEAN:
            return bool(name, p -> IceaxeConvertUtil.toBoolean(getter.apply(p)));
        case INT4:
            return int4(name, p -> IceaxeConvertUtil.toInt4(getter.apply(p)));
        case INT8:
            return int8(name, p -> IceaxeConvertUtil.toInt8(getter.apply(p)));
        case FLOAT4:
            return float4(name, p -> IceaxeConvertUtil.toFloat4(getter.apply(p)));
        case FLOAT8:
            return float8(name, p -> IceaxeConvertUtil.toFloat8(getter.apply(p)));
        case DECIMAL:
            return decimal(name, p -> IceaxeConvertUtil.toDecimal(getter.apply(p)));
        case CHARACTER:
            return character(name, p -> IceaxeConvertUtil.toCharacter(getter.apply(p)));
        case BYTES:
            return bytes(name, p -> IceaxeConvertUtil.toBytes(getter.apply(p)));
        case BITS:
            return bits(name, p -> IceaxeConvertUtil.toBits(getter.apply(p)));
        case DATE:
            return date(name, p -> IceaxeConvertUtil.toDate(getter.apply(p)));
        case TIME:
            return time(name, p -> IceaxeConvertUtil.toTime(getter.apply(p)));
        case INSTANT:
            return instant(name, p -> IceaxeConvertUtil.toInstant(getter.apply(p)));
        default:
            throw new UnsupportedOperationException("unsupported type error. type=" + type);
        }
    }

    /**
     * add variable
     * 
     * @param name   name
     * @param type   type
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> add(String name, Class<?> type, Function<P, Object> getter) {
        var tgType = TgDataType.of(type);
        return add(name, tgType, getter);
    }

    protected void addVariable(String name, TgDataType type) {
        var lowVariable = PlaceHolder.newBuilder().setName(name).setType(type.getLowDataType()).build();
        lowPlaceHolderList.add(lowVariable);
    }

    @Override
    public List<PlaceHolder> toLowPlaceHolderList() {
        return lowPlaceHolderList;
    }

    @Override
    protected List<Parameter> toLowParameterList(P parameter) {
        var list = new ArrayList<Parameter>(parameterConverterList.size());
        for (var converter : parameterConverterList) {
            list.add(converter.apply(parameter));
        }
        return list;
    }
}
