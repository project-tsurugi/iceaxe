package com.tsurugidb.iceaxe.sql.parameter.mapping;

import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;;

/**
 * Tsurugi Parameter Mapping for Entity.
 *
 * @param <P> parameter type (e.g. Entity)
 */
public class TgConverterParameterMapping<P> extends TgParameterMapping<P> {

    /**
     * create Parameter Mapping.
     *
     * @param <P>                parameter type
     * @param variables          bind variables
     * @param parameterConverter converter from P to Parameter
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgConverterParameterMapping<P> of(TgBindVariables variables, Function<P, TgBindParameters> parameterConverter) {
        return new TgConverterParameterMapping<>(variables, parameterConverter);
    }

    private final TgBindVariables variables;
    private final Function<P, TgBindParameters> parameterConverter;

    /**
     * Creates a new instance.
     *
     * @param variables          bind variables
     * @param parameterConverter converter from P to Parameter
     */
    public TgConverterParameterMapping(TgBindVariables variables, Function<P, TgBindParameters> parameterConverter) {
        this.variables = variables;
        this.parameterConverter = parameterConverter;
    }

    @Override
    public List<Placeholder> toLowPlaceholderList() {
        return variables.toLowPlaceholderList();
    }

    @Override
    public List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil) {
        return parameterConverter.apply(parameter).toLowParameterList();
    }
}
