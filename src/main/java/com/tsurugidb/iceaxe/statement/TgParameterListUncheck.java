package com.tsurugidb.iceaxe.statement;

import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;

/**
 * Tsurugi Parameter for PreparedStatement
 * 
 * <p>
 * Do not check with {@link TgVariableList}.
 * </p>
 */
public class TgParameterListUncheck implements TgParameterList {

    private final List<Parameter> lowParameterList = new ArrayList<>();

    /**
     * Tsurugi Parameter for PreparedStatement
     */
    public TgParameterListUncheck() {
        // do nothing
    }

    @Override
    public TgParameterListUncheck int4(String name, int value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setInt4Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck int4(String name, Integer value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt4Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck int8(String name, long value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setInt8Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck int8(String name, Long value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt8Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck float4(String name, float value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setFloat4Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck float4(String name, Float value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat4Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck float8(String name, double value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setFloat8Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck float8(String name, Double value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat8Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListUncheck character(String name, String value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setCharacterValue(value);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, int value) {
        return int4(name, value);
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, Integer value) {
        return int4(name, value);
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, long value) {
        return int8(name, value);
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, Long value) {
        return int8(name, value);
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, float value) {
        return float4(name, value);
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, Float value) {
        return float4(name, value);
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, double value) {
        return float8(name, value);
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, Double value) {
        return float8(name, value);
    }

    /**
     * add value(String)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListUncheck add(String name, String value) {
        return character(name, value);
    }

    /**
     * add parameter
     * 
     * @param parameter parameter
     * @return this
     */
    public TgParameterListUncheck add(TgParameter parameter) {
        lowParameterList.add(parameter.toLowParameter());
        return this;
    }

    protected void add(Parameter.Builder lowParameter) {
        lowParameterList.add(lowParameter.build());
    }

    // internal
    @Override
    public List<Parameter> toLowParameterList() {
        return this.lowParameterList;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + lowParameterList;
    }
}
