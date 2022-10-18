package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    public static TgParameter of(@Nonnull String name, boolean value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Boolean value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, int value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Integer value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, long value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Long value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, float value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Float value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, double value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Double value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable BigDecimal value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable String value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable byte[] value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable boolean[] value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable LocalDate value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable LocalTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable LocalDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable OffsetTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable OffsetDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable ZonedDateTime value) {
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
