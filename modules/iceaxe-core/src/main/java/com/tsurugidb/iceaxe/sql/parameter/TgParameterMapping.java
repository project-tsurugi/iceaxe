package com.tsurugidb.iceaxe.sql.parameter;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.sql.parameter.mapping.TgConverterParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;

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
    public static TgParameterMapping<TgBindParameters> of(TgBindVariable<?>... variableList) {
        return of(TgBindVariables.of(variableList));
    }

    /**
     * create Parameter Mapping
     *
     * @param variableList variable definition
     * @return Tsurugi Parameter Mapping
     */
    public static TgParameterMapping<TgBindParameters> of(Collection<? extends TgBindVariable<?>> variableList) {
        return of(TgBindVariables.of(variableList));
    }

    /**
     * create Parameter Mapping
     *
     * @param variableList variable definition
     * @return Tsurugi Parameter Mapping
     */
    public static TgParameterMapping<TgBindParameters> of(TgBindVariables variableList) {
        return of(variableList, TgBindParameters.IDENTITY);
    }

    /**
     * create Parameter Mapping
     *
     * @param <P>                parameter type
     * @param variableList       variable definition
     * @param parameterConverter converter from P to Parameter
     * @return Tsurugi Parameter Mapping
     */
    public static <P> TgParameterMapping<P> of(TgBindVariables variableList, Function<P, TgBindParameters> parameterConverter) {
        return new TgConverterParameterMapping<>(variableList, parameterConverter);
    }

    private IceaxeConvertUtil convertUtil = null;

    /**
     * set convert type utility
     *
     * @param convertUtil convert type utility
     */
    public TgParameterMapping<P> setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
        return this;
    }

    // internal
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    // internal
    public abstract List<Placeholder> toLowPlaceholderList();

    // internal
    public abstract List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil);
}
