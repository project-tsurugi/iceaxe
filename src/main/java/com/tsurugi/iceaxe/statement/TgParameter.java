package com.tsurugi.iceaxe.statement;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;

/**
 * Tsurugi Parameter for PreparedStatement
 */
public interface TgParameter {

    /**
     * create Tsurugi Parameter
     * 
     * @return Tsurugi Parameter
     */
    public static TgParameterUncheck of() {
        return new TgParameterUncheck();
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param variable variable definition
     * @return Tsurugi Parameter
     */
    public static TgParameterWithVariable of(TgVariable variable) {
        return new TgParameterWithVariable(variable);
    }

    // internal
    public ParameterSet toLowParameterSet();
}
