package com.tsurugidb.iceaxe.statement;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet.Parameter;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Parameter for PreparedStatement
 * 
 * <p>
 * Perform conversion to the type defined in {@link TgVariableList}.
 * </p>
 */
public class TgParameterListWithVariable implements TgParameterList {

    private final TgVariableList variable;
    private final ParameterSet.Builder lowBuilder = ParameterSet.newBuilder();
    private ParameterSet lowParameterSet;

    /**
     * Tsurugi Parameter for PreparedStatement
     */
    public TgParameterListWithVariable(TgVariableList variable) {
        this.variable = variable;
    }

    protected TgDataType getType(String name) {
        return variable.getDataType(name);
    }

    protected void checkType(String name, TgDataType type) {
        var variableType = getType(name);
        if (variableType != type) {
            throw new IllegalStateException("type unmatch. name=" + name + ", variable=" + variableType + ", type=" + type);
        }
    }

    @Override
    public TgParameterListWithVariable int4(String name, int value) {
        checkType(name, TgDataType.INT4);
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setInt4Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable int4(String name, Integer value) {
        checkType(name, TgDataType.INT4);
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt4Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable int8(String name, long value) {
        checkType(name, TgDataType.INT8);
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setInt8Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable int8(String name, Long value) {
        checkType(name, TgDataType.INT8);
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt8Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable float4(String name, float value) {
        checkType(name, TgDataType.FLOAT4);
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setFloat4Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable float4(String name, Float value) {
        checkType(name, TgDataType.FLOAT4);
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat4Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable float8(String name, double value) {
        checkType(name, TgDataType.FLOAT8);
        var lowParameter = Parameter.newBuilder().setName(name);
        lowParameter.setFloat8Value(value);
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable float8(String name, Double value) {
        checkType(name, TgDataType.FLOAT8);
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat8Value(value);
        }
        add(lowParameter);
        return this;
    }

    @Override
    public TgParameterListWithVariable character(String name, String value) {
        checkType(name, TgDataType.CHARACTER);
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setCharacterValue(value);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterListWithVariable add(String name, Object value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        setValueTo(lowParameter, name, value);
        add(lowParameter);
        return this;
    }

    protected void setValueTo(Parameter.Builder lowParameter, String name, Object value) {
        if (value == null) {
            return;
        }

        var type = getType(name);
        switch (type) {
        case INT4:
            var int4 = IceaxeConvertUtil.toInt4(value);
            lowParameter.setInt4Value(int4);
            break;
        case INT8:
            var int8 = IceaxeConvertUtil.toInt8(value);
            lowParameter.setInt8Value(int8);
            break;
        case FLOAT4:
            var float4 = IceaxeConvertUtil.toFloat4(value);
            lowParameter.setFloat4Value(float4);
            break;
        case FLOAT8:
            var float8 = IceaxeConvertUtil.toFloat8(value);
            lowParameter.setFloat8Value(float8);
            break;
        case CHARACTER:
            var character = IceaxeConvertUtil.toCharacter(value);
            lowParameter.setCharacterValue(character);
            break;
        default:
            throw new InternalError("unsupported type error. type=" + type);
        }
    }

    protected void add(Parameter.Builder lowParameter) {
        lowBuilder.addParameters(lowParameter.build());
        this.lowParameterSet = null;
    }

    // internal
    @Override
    public ParameterSet toLowParameterSet() {
        if (this.lowParameterSet == null) {
            this.lowParameterSet = lowBuilder.build();
        }
        return this.lowParameterSet;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + lowBuilder + "]";
    }
}
