package com.tsurugidb.iceaxe.statement;

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
     * add variable(int)
     * 
     * @param name   name
     * @param getter getter from parameter
     * @return this
     */
    public TgEntityParameterMapping<P> int4(String name, Function<P, Integer> getter) {
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
    public TgEntityParameterMapping<P> int8(String name, Function<P, Long> getter) {
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
    public TgEntityParameterMapping<P> character(String name, Function<P, String> getter) {
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
    public TgEntityParameterMapping<P> add(String name, TgDataType type, Function<P, Object> getter) {
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
