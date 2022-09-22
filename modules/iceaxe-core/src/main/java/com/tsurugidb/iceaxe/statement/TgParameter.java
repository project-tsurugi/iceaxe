package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import com.tsurugidb.sql.proto.SqlRequest.Parameter;

/**
 * Tsurugi Parameter
 * 
 * @see TgParameterList#of(TgParameter...)
 */
public class TgParameter {

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, boolean value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, Boolean value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, int value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, Integer value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, long value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, Long value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, float value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, Float value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, double value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, Double value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, BigDecimal value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, String value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, byte[] value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, boolean[] value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, LocalDate value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, LocalTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, LocalDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, OffsetTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, OffsetDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     * 
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(String name, ZonedDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
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
