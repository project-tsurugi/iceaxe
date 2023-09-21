package com.tsurugidb.iceaxe.sql.parameter;

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

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;

/**
 * Tsurugi Bind Parameter.
 *
 * @see TgBindParameters#of(TgBindParameter...)
 */
public class TgBindParameter {

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, boolean value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, boolean.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable Boolean value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Boolean.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, int value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, int.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable Integer value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Integer.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, long value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, long.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable Long value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Long.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, float value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, float.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable Float value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Float.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, double value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, double.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable Double value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, Double.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable BigDecimal value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, (value != null) ? value.toPlainString() : null, BigDecimal.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable String value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, String.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable byte[] value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, toString(value), byte[].class));
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
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable boolean[] value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, (value != null) ? Arrays.toString(value) : null, boolean[].class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable LocalDate value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, LocalDate.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable LocalTime value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, LocalTime.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable LocalDateTime value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, LocalDateTime.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable OffsetTime value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, OffsetTime.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable OffsetDateTime value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, OffsetDateTime.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable ZonedDateTime value) {
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), () -> toString(name, value, ZonedDateTime.class));
    }

    private final Parameter lowParameter;
    private final Supplier<String> stringSupplier;

    /**
     * Creates a new instance.
     *
     * @param lowParameter   low parameter
     * @param stringSupplier string supplier
     */
    protected TgBindParameter(Parameter lowParameter, Supplier<String> stringSupplier) {
        this.lowParameter = lowParameter;
        this.stringSupplier = stringSupplier;
    }

    /**
     * convert to {@link Parameter}.
     *
     * @return parameter
     */
    @IceaxeInternal
    public Parameter toLowParameter() {
        return this.lowParameter;
    }

    @Override
    public String toString() {
        return stringSupplier.get();
    }

    /**
     * to string.
     *
     * @param name  parameter name
     * @param value value
     * @param type  data type
     * @return string
     */
    protected static String toString(String name, Object value, Class<?> type) {
        return name + "=" + value + "(" + type.getSimpleName() + ")";
    }
}
