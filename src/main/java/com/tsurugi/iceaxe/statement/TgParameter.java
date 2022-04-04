package com.tsurugi.iceaxe.statement;

import java.util.HashMap;
import java.util.Map;

import com.nautilus_technologies.tsubakuro.protos.CommonProtos.DataType;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.PlaceHolder.Variable;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet.Parameter;

/**
 * Tsurugi Parameter for PreparedStatement
 */
public class TgParameter implements TsurugiToLowParameterSet {

    /**
     * create Tsurugi Parameter
     * 
     * @return Tsurugi Parameter
     */
    public static TgParameter of() {
        return new TgParameter();
    }

    private final ParameterSet.Builder lowBuilder = ParameterSet.newBuilder();
    private ParameterSet lowParameterSet;
    private Map<String, DataType> typeMap;

    /**
     * Tsurugi Parameter for PreparedStatement
     */
    public TgParameter() {
        // do nothing
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, int value) {
        var lowParameter = Parameter.newBuilder().setName(name).setInt4Value(value);
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
    public TgParameter set(String name, Integer value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt4Value(value);
        } else {
            put(name, DataType.INT4);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, long value) {
        var lowParameter = Parameter.newBuilder().setName(name).setInt8Value(value);
        add(lowParameter);
        return this;
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, Long value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt8Value(value);
        } else {
            put(name, DataType.INT8);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, float value) {
        var lowParameter = Parameter.newBuilder().setName(name).setFloat4Value(value);
        add(lowParameter);
        return this;
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, Float value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat4Value(value);
        } else {
            put(name, DataType.FLOAT4);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, double value) {
        var lowParameter = Parameter.newBuilder().setName(name).setFloat8Value(value);
        add(lowParameter);
        return this;
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, Double value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat8Value(value);
        } else {
            put(name, DataType.FLOAT8);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(String)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameter set(String name, String value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setCharacterValue(value);
        } else {
            put(name, DataType.CHARACTER);
        }
        add(lowParameter);
        return this;
    }

    protected void add(Parameter.Builder lowParameter) {
        lowBuilder.addParameters(lowParameter.build());
        this.lowParameterSet = null;
    }

    protected void put(String name, DataType type) {
        if (this.typeMap == null) {
            this.typeMap = new HashMap<>();
        }
        typeMap.put(name, type);
    }

    @Override
    public ParameterSet toLowParameterSet() {
        if (this.lowParameterSet == null) {
            this.lowParameterSet = lowBuilder.build();
        }
        return this.lowParameterSet;
    }

    // internal
    public PlaceHolder toLowPlaceHolder() {
        var paramSet = toLowParameterSet();
        var builder = PlaceHolder.newBuilder();
        for (Parameter param : paramSet.getParametersList()) {
            var type = getLowDataType(param);
            var variable = Variable.newBuilder().setName(param.getName()).setType(type).build();
            builder.addVariables(variable);
        }
        return builder.build();
    }

    protected DataType getLowDataType(Parameter param) {
        var valueCase = param.getValueCase();
        switch (valueCase) {
        case INT4_VALUE:
            return DataType.INT4;
        case INT8_VALUE:
            return DataType.INT8;
        case FLOAT4_VALUE:
            return DataType.FLOAT4;
        case FLOAT8_VALUE:
            return DataType.FLOAT8;
        case CHARACTER_VALUE:
            return DataType.CHARACTER;
        default:
            assert typeMap != null;
            return typeMap.get(param.getName());
        }
    }
}
