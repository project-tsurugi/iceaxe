package com.tsurugi.iceaxe.statement;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet.Parameter;

/**
 * Tsurugi Parameter
 * 
 * @see TgParameterList#of(TgParameter...)
 */
public class TgParameter {

    /**
     * create Tsurugi Parameter(int)
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, Integer value) {
        var builder = Parameter.newBuilder().setName(name);
        if (value != null) {
            builder.setInt4Value(value);
        }
        return new TgParameter(builder.build());
    }

    /**
     * create Tsurugi Parameter(long)
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, Long value) {
        var builder = Parameter.newBuilder().setName(name);
        if (value != null) {
            builder.setInt8Value(value);
        }
        return new TgParameter(builder.build());
    }

    // TODO float, double

    /**
     * create Tsurugi Parameter(String)
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, String value) {
        var builder = Parameter.newBuilder().setName(name);
        if (value != null) {
            builder.setCharacterValue(value);
        }
        return new TgParameter(builder.build());
    }

    private final Parameter lowParameter;

    protected TgParameter(Parameter lowParameter) {
        this.lowParameter = lowParameter;
    }

    // internal
    public Parameter toLowParameter() {
        return this.lowParameter;
    }
}
