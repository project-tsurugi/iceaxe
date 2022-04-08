package com.tsurugi.iceaxe.statement;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet.Parameter;
import com.tsurugi.iceaxe.util.IceaxeConvertUtil;

/**
 * Tsurugi Parameter for PreparedStatement
 * 
 * <p>
 * Perform conversion to the type defined in {@link TgVariable}.
 * </p>
 */
public class TgParameterWithVariable implements TgParameter {

    private final TgVariable variable;
    private final ParameterSet.Builder lowBuilder = ParameterSet.newBuilder();
    private ParameterSet lowParameterSet;

    /**
     * Tsurugi Parameter for PreparedStatement
     */
    public TgParameterWithVariable(TgVariable variable) {
        this.variable = variable;
    }

    protected TgDataType getType(String name) {
        return variable.getDataType(name);
    }

    /**
     * add value
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterWithVariable set(String name, Object value) {
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
