package com.tsurugi.iceaxe.statement;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet.Parameter;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder.Variable;
import com.tsurugi.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Parameter Mapping
 * 
 * @param <P> parameter type
 */
public class TgParameterMapping<P> {

    /**
     * create Parameter Mapping
     * 
     * @param <P>   parameter type
     * @param clazz parameter class
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgParameterMapping<P> of(Class<P> clazz) {
        return new TgParameterMapping<>();
    }

    private final PlaceHolder.Builder lowPlaceHolderBuilder = PlaceHolder.newBuilder();
    private final List<Function<P, Parameter>> parameterConverterList = new ArrayList<>();

    /**
     * Tsurugi Parameter Mapping
     */
    public TgParameterMapping() {
        // do nothing
    }

    /**
     * add variable(int)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgParameterMapping<P> int4(String name, Function<P, Integer> getter) {
        addVariable(name, TgDataType.INT4);
        parameterConverterList.add(parameter -> {
            var builder = Parameter.newBuilder().setName(name);
            var value = getter.apply(parameter);
            if (value != null) {
                builder.setInt4Value(value);
            }
            return builder.build();
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
    public TgParameterMapping<P> int8(String name, Function<P, Long> getter) {
        addVariable(name, TgDataType.INT8);
        parameterConverterList.add(parameter -> {
            var builder = Parameter.newBuilder().setName(name);
            var value = getter.apply(parameter);
            if (value != null) {
                builder.setInt8Value(value);
            }
            return builder.build();
        });
        return this;
    }

    // TODO float4, float8

    /**
     * add variable(String)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgParameterMapping<P> character(String name, Function<P, String> getter) {
        addVariable(name, TgDataType.CHARACTER);
        parameterConverterList.add(parameter -> {
            var builder = Parameter.newBuilder().setName(name);
            var value = getter.apply(parameter);
            if (value != null) {
                builder.setCharacterValue(value);
            }
            return builder.build();
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
    public TgParameterMapping<P> add(String name, TgDataType type, Function<P, Object> getter) {
        addVariable(name, type);
        switch (type) {
        case INT4:
            return int4(name, p -> IceaxeConvertUtil.toInt4(getter.apply(p)));
        case INT8:
            return int8(name, p -> IceaxeConvertUtil.toInt8(getter.apply(p)));
        // TODO float4, float8
        case CHARACTER:
            return character(name, p -> IceaxeConvertUtil.toCharacter(getter.apply(p)));
        default:
            throw new UnsupportedOperationException();
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
    public TgParameterMapping<P> add(String name, Class<?> type, Function<P, Object> getter) {
        var tgType = TgDataType.of(type);
        return add(name, tgType, getter);
    }

    protected void addVariable(String name, TgDataType type) {
        var lowVariable = Variable.newBuilder().setName(name).setType(type.getLowDataType()).build();
        lowPlaceHolderBuilder.addVariables(lowVariable);
    }

    // internal
    public PlaceHolder toLowPlaceHolder() {
        return lowPlaceHolderBuilder.build();
    }

    // internal
    public ParameterSet toLowParameterSet(P parameter) {
        var lowBuilder = ParameterSet.newBuilder();
        for (var converter : parameterConverterList) {
            lowBuilder.addParameters(converter.apply(parameter));
        }
        return lowBuilder.build();
    }
}
