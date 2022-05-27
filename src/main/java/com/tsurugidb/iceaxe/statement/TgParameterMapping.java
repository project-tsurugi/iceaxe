package com.tsurugidb.iceaxe.statement;

import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder;

/**
 * Tsurugi Parameter Mapping
 * 
 * @param <P> parameter type
 */
public abstract class TgParameterMapping<P> {

    /**
     * create Parameter Mapping
     * 
     * @param <P>   parameter type
     * @param clazz parameter class
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgEntityParameterMapping<P> of(Class<P> clazz) {
        return TgEntityParameterMapping.of(clazz);
    }

    /**
     * create Parameter Mapping
     * 
     * @param variableList variable definition
     * @return Tsurugi Parameter Mapping
     */
    public static TgParameterMapping<TgParameterList> of(TgVariableList variableList) {
        return of(variableList, TgParameterList.IDENTITY);
    }

    /**
     * create Parameter Mapping
     * 
     * @param <P>                parameter type
     * @param variableList       variable definition
     * @param parameterConverter converter from P to Parameter
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgParameterMapping<P> of(TgVariableList variableList, Function<P, TgParameterList> parameterConverter) {
        return new TgConverterParameterMapping<>(variableList, parameterConverter);
    }

    // internal
    public abstract PlaceHolder toLowPlaceHolder();

    // internal
    protected abstract ParameterSet toLowParameterSet(P parameter);
}
