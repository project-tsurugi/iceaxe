package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;

/**
 * Tsurugi Parameter for PreparedStatement
 */
public class TgParameterList {

    /**
     * create Tsurugi Parameter
     * 
     * @return Tsurugi Parameter
     */
    public static TgParameterList of() {
        return new TgParameterList();
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param parameters parameter
     * @return Tsurugi Parameter
     */
    public static TgParameterList of(TgParameter... parameters) {
        var parameterList = new TgParameterList();
        for (var parameter : parameters) {
            parameterList.add(parameter);
        }
        return parameterList;
    }

    /**
     * a function that always returns its input argument.
     */
    public static final Function<TgParameterList, TgParameterList> IDENTITY = p -> p;

    private final List<Parameter> lowParameterList = new ArrayList<>();

    /**
     * Tsurugi Parameter for PreparedStatement
     */
    public TgParameterList() {
        // do nothing
    }

    /**
     * add value(boolean)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList bool(String name, boolean value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(boolean)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList bool(String name, Boolean value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int4(String name, int value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int4(String name, Integer value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int8(String name, long value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList int8(String name, Long value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float4(String name, float value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float4(String name, Float value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float8(String name, double value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList float8(String name, Double value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(decimal)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList decimal(String name, BigDecimal value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(String)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList character(String name, String value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(byte[])
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList bytes(String name, byte[] value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(boolean[])
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList bits(String name, boolean[] value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(date)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList date(String name, LocalDate value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(time)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList time(String name, LocalTime value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(Instant)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList instant(String name, Instant value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(ZonedDateTime)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList zonedDateTime(String name, ZonedDateTime value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(boolean)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, boolean value) {
        return bool(name, value);
    }

    /**
     * add value(boolean)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, Boolean value) {
        return bool(name, value);
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, int value) {
        return int4(name, value);
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, Integer value) {
        return int4(name, value);
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, long value) {
        return int8(name, value);
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, Long value) {
        return int8(name, value);
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, float value) {
        return float4(name, value);
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, Float value) {
        return float4(name, value);
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, double value) {
        return float8(name, value);
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, Double value) {
        return float8(name, value);
    }

    /**
     * add value(decimal)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, BigDecimal value) {
        return decimal(name, value);
    }

    /**
     * add value(String)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, String value) {
        return character(name, value);
    }

    /**
     * add value(byte[])
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, byte[] value) {
        return bytes(name, value);
    }

    /**
     * add value(boolean[])
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, boolean[] value) {
        return bits(name, value);
    }

    /**
     * add value(date)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, LocalDate value) {
        return date(name, value);
    }

    /**
     * add value(time)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, LocalTime value) {
        return time(name, value);
    }

    /**
     * add value(Instant)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, Instant value) {
        return instant(name, value);
    }

    /**
     * add value(ZonedDateTime)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(String name, ZonedDateTime value) {
        return zonedDateTime(name, value);
    }

    /**
     * add parameter
     * 
     * @param parameter parameter
     * @return this
     */
    public TgParameterList add(TgParameter parameter) {
        lowParameterList.add(parameter.toLowParameter());
        return this;
    }

    protected void add(Parameter lowParameter) {
        lowParameterList.add(lowParameter);
    }

    /**
     * add parameter
     * 
     * @param otherList parameter list
     * @return this
     */
    public TgParameterList add(TgParameterList otherList) {
        for (var p : otherList.toLowParameterList()) {
            lowParameterList.add(p);
        }
        return this;
    }

    // internal
    public List<Parameter> toLowParameterList() {
        return this.lowParameterList;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + lowParameterList;
    }
}
