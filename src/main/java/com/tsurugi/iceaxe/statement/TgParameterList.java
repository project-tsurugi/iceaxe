package com.tsurugi.iceaxe.statement;

import java.util.function.Function;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;

/**
 * Tsurugi Parameter for PreparedStatement
 */
public interface TgParameterList {

    /**
     * create Tsurugi Parameter
     * 
     * @return Tsurugi Parameter
     */
    public static TgParameterListUncheck of() {
        return new TgParameterListUncheck();
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param parameters parameter
     * @return Tsurugi Parameter
     */
    public static TgParameterList of(TgParameter... parameters) {
        var parameterList = new TgParameterListUncheck();
        for (var parameter : parameters) {
            parameterList.add(parameter);
        }
        return parameterList;
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param variable variable definition
     * @return Tsurugi Parameter
     */
    public static TgParameterListWithVariable of(TgVariableList variable) {
        return new TgParameterListWithVariable(variable);
    }

    /**
     * a function that always returns its input argument.
     */
    public static final Function<TgParameterList, TgParameterList> IDENTITY = p -> p;

    // internal
    public ParameterSet toLowParameterSet();
}
