package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.function.Supplier;

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
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, boolean.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Boolean value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Boolean.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, int value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, int.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Integer value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Integer.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, long value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, long.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Long value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Long.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, float value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, float.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Float value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Float.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, double value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, double.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable Double value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Double.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable BigDecimal value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, (value != null) ? value.toPlainString() : null, BigDecimal.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable String value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, String.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable byte[] value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, toString(value), byte[].class));
    }

    private static String toString(byte[] value) {
        if (value == null) {
            return null;
        }

        var sb = new StringBuilder(1 + 3 * value.length + 1);
        sb.append('[');
        for (byte b : value) {
            if (sb.charAt(sb.length() - 1) != '[') {
                sb.append(',');
            }
            int i = b & 0xff;
            if (i <= 0xf) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(i));
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable boolean[] value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, (value != null) ? Arrays.toString(value) : null, boolean[].class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable LocalDate value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, LocalDate.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable LocalTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, LocalTime.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable LocalDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, LocalDateTime.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable OffsetTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, OffsetTime.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable OffsetDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, OffsetDateTime.class));
    }

    /**
     * create Tsurugi Parameter
     *
     * @param name  name
     * @param value value
     * @return Tsurugi Parameter
     */
    public static TgParameter of(@Nonnull String name, @Nullable ZonedDateTime value) {
        return new TgParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, ZonedDateTime.class));
    }

    private final Parameter lowParameter;
    private final Supplier<String> stringSupplier;

    protected TgParameter(Parameter lowParameter, Supplier<String> stringSupplier) {
        this.lowParameter = lowParameter;
        this.stringSupplier = stringSupplier;
    }

    // internal
    public Parameter toLowParameter() {
        return this.lowParameter;
    }

    @Override
    public String toString() {
        return stringSupplier.get();
    }

    protected static String toString(String name, Object value, Class<?> type) {
        return name + "=" + value + "(" + type.getSimpleName() + ")";
    }
}
