package com.tsurugi.iceaxe.statement;

import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder;

/**
 * Tsurugi Parameter Mapping for Entity
 * 
 * @param <P> parameter type (e.g. Entity)
 */
public class TgConverterParameterMapping<P> extends TgParameterMapping<P> {

    /**
     * create Parameter Mapping
     * 
     * @param <P>                parameter type
     * @param variableList       variable definition
     * @param parameterConverter converter from P to Parameter
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgConverterParameterMapping<P> of(TgVariableList variableList, Function<P, TgParameterList> parameterConverter) {
        return new TgConverterParameterMapping<>(variableList, parameterConverter);
    }

    private final TgVariableList variableList;
    private final Function<P, TgParameterList> parameterConverter;

    /**
     * Tsurugi Parameter Mapping
     * 
     * @param variableList       variable definition
     * @param parameterConverter converter from P to Parameter
     */
    public TgConverterParameterMapping(TgVariableList variableList, Function<P, TgParameterList> parameterConverter) {
        this.variableList = variableList;
        this.parameterConverter = parameterConverter;
    }

    @Override
    public PlaceHolder toLowPlaceHolder() {
        return variableList.toLowPlaceHolder();
    }

    @Override
    protected ParameterSet toLowParameterSet(P parameter) {
        return parameterConverter.apply(parameter).toLowParameterSet();
    }
}
