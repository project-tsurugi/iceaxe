package com.tsurugidb.iceaxe.statement;

import java.util.List;
import java.util.function.Function;

import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;
import com.tsurugidb.jogasaki.proto.SqlRequest.PlaceHolder;

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
    public abstract List<PlaceHolder> toLowPlaceHolderList();

    // internal
    protected abstract List<Parameter> toLowParameterList(P parameter);
}
