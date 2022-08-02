package com.tsurugidb.iceaxe.statement;

import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;
import com.tsurugidb.jogasaki.proto.SqlRequest.Placeholder;

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
    public static TgParameterMapping<TgParameterList> of(TgVariable<?>... variableList) {
        return of(TgVariableList.of(variableList));
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

    private IceaxeConvertUtil convertUtil = null;

    /**
     * set convert type utility
     * 
     * @param convertUtil convert type utility
     */
    public void setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
    }

    // internal
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    // internal
    public abstract List<Placeholder> toLowPlaceholderList();

    // internal
    protected abstract List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil);
}
