package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;

/**
 * Tsurugi Parameter for PreparedStatement
 * 
 * <p>
 * Perform conversion to the type defined in {@link TgVariableList}.
 * </p>
 */
public class TgParameterListWithVariable implements TgParameterList {

    private final TgVariableList variable;
    private final List<Parameter> lowParameterList = new ArrayList<>();

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
    public TgParameterListWithVariable bool(String name, boolean value) {
        checkType(name, TgDataType.BOOLEAN);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable bool(String name, Boolean value) {
        checkType(name, TgDataType.BOOLEAN);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable int4(String name, int value) {
        checkType(name, TgDataType.INT4);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable int4(String name, Integer value) {
        checkType(name, TgDataType.INT4);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable int8(String name, long value) {
        checkType(name, TgDataType.INT8);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable int8(String name, Long value) {
        checkType(name, TgDataType.INT8);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable float4(String name, float value) {
        checkType(name, TgDataType.FLOAT4);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable float4(String name, Float value) {
        checkType(name, TgDataType.FLOAT4);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable float8(String name, double value) {
        checkType(name, TgDataType.FLOAT8);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable float8(String name, Double value) {
        checkType(name, TgDataType.FLOAT8);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable decimal(String name, BigDecimal value) {
        checkType(name, TgDataType.DECIMAL);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable character(String name, String value) {
        checkType(name, TgDataType.CHARACTER);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable bytes(String name, byte[] value) {
        checkType(name, TgDataType.BYTES);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable bits(String name, boolean[] value) {
        checkType(name, TgDataType.BITS);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable date(String name, LocalDate value) {
        checkType(name, TgDataType.DATE);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable time(String name, LocalTime value) {
        checkType(name, TgDataType.TIME);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable instant(String name, Instant value) {
        checkType(name, TgDataType.INSTANT);
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    @Override
    public TgParameterListWithVariable zonedDateTime(String name, ZonedDateTime value) {
        checkType(name, TgDataType.INSTANT);
        add(IceaxeLowParameterUtil.create(name, value));
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
        add(getLowParameter(name, value));
        return this;
    }

    protected Parameter getLowParameter(String name, Object value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        var type = getType(name);
        switch (type) {
        case BOOLEAN:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toBoolean(value));
        case INT4:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toInt4(value));
        case INT8:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toInt8(value));
        case FLOAT4:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toFloat4(value));
        case FLOAT8:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toFloat8(value));
        case DECIMAL:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toDecimal(value));
        case CHARACTER:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toCharacter(value));
        case BYTES:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toBytes(value));
        case BITS:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toBits(value));
        case DATE:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toDate(value));
        case TIME:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toTime(value));
        case INSTANT:
            return IceaxeLowParameterUtil.create(name, IceaxeConvertUtil.toInstant(value));
        default:
            throw new InternalError("unsupported type error. type=" + type);
        }
    }

    protected void add(Parameter lowParameter) {
        lowParameterList.add(lowParameter);
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
