package com.tsurugi.iceaxe.statement;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;

/**
 * Tsurugi convert to ParameterSet
 */
public interface TsurugiToLowParameterSet {

    /**
     * get Low ParameterSet
     * 
     * @return ParameterSet
     */
    public ParameterSet toLowParameterSet();
}
