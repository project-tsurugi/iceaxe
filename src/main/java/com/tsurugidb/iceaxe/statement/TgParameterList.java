package com.tsurugidb.iceaxe.statement;

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

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int4(String name, int value);

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int4(String name, Integer value);

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int8(String name, long value);

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int8(String name, Long value);

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float4(String name, float value);

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float4(String name, Float value);

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float8(String name, double value);

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float8(String name, Double value);

    /**
     * add value(String)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList character(String name, String value);

    // internal
    public ParameterSet toLowParameterSet();
}
