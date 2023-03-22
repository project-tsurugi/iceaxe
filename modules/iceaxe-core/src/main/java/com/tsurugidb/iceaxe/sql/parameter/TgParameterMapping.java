package com.tsurugidb.iceaxe.sql.parameter;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgConverterParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEmptyParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgSingleParameterMapping;
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
     * @return parameter mapping
     */
    public static <P> TgEntityParameterMapping<P> of(Class<P> clazz) {
        return TgEntityParameterMapping.of(clazz);
    }

    /**
     * create Parameter Mapping
     *
     * @param variables bind variables
     * @return parameter mapping
     */
    public static TgParameterMapping<TgBindParameters> of(TgBindVariable<?>... variables) {
        if (variables.length == 0) {
            return TgEmptyParameterMapping.of();
        }
        return of(TgBindVariables.of(variables));
    }

    /**
     * create Parameter Mapping
     *
     * @param variables bind variables
     * @return parameter mapping
     */
    public static TgParameterMapping<TgBindParameters> of(Collection<? extends TgBindVariable<?>> variables) {
        return of(TgBindVariables.of(variables));
    }

    /**
     * create Parameter Mapping
     *
     * @param variables bind variables
     * @return parameter mapping
     */
    public static TgParameterMapping<TgBindParameters> of(TgBindVariables variables) {
        return of(variables, TgBindParameters.IDENTITY);
    }

    /**
     * create Parameter Mapping
     *
     * @param <P>                parameter type
     * @param variables          bind variables
     * @param parameterConverter converter from P to Parameter
     * @return parameter mapping
     */
    public static <P> TgParameterMapping<P> of(TgBindVariables variables, Function<P, TgBindParameters> parameterConverter) {
        return new TgConverterParameterMapping<>(variables, parameterConverter);
    }

    /**
     * create Parameter Mapping (single variable)
     *
     * @param <P>   parameter type
     * @param name  bind variable name
     * @param clazz parameter type
     * @return parameter mapping
     * @see TgSingleParameterMapping
     */
    public static <P> TgParameterMapping<P> of(String name, Class<P> clazz) {
        return TgSingleParameterMapping.of(name, clazz);
    }

    /**
     * create Parameter Mapping (single variable)
     *
     * @param <P>  parameter type
     * @param name bind variable name
     * @param type parameter type
     * @return parameter mapping
     * @see TgSingleParameterMapping
     */
    public static <P> TgParameterMapping<P> of(String name, TgDataType type) {
        return TgSingleParameterMapping.of(name, type);
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
