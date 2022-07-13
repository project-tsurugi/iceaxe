package com.tsurugidb.iceaxe.statement;

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
    public List<PlaceHolder> toLowPlaceHolderList() {
        return variableList.toLowPlaceHolderList();
    }

    @Override
    protected List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil) {
        return parameterConverter.apply(parameter).toLowParameterList();
    }
}
