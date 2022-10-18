package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableBigDecimal;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;

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
    public TgParameterList bool(@Nonnull String name, boolean value) {
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
    public TgParameterList bool(@Nonnull String name, @Nullable Boolean value) {
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
    public TgParameterList int4(@Nonnull String name, int value) {
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
    public TgParameterList int4(@Nonnull String name, @Nullable Integer value) {
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
    public TgParameterList int8(@Nonnull String name, long value) {
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
    public TgParameterList int8(@Nonnull String name, @Nullable Long value) {
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
    public TgParameterList float4(@Nonnull String name, float value) {
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
    public TgParameterList float4(@Nonnull String name, @Nullable Float value) {
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
    public TgParameterList float8(@Nonnull String name, double value) {
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
    public TgParameterList float8(@Nonnull String name, @Nullable Double value) {
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
    public TgParameterList decimal(@Nonnull String name, @Nullable BigDecimal value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(decimal)
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale. see {@link TgVariableBigDecimal#DEFAULT_ROUNDING_MODE}
     * @return this
     */
    public TgParameterList decimal(@Nonnull String name, @Nullable BigDecimal value, int scale) {
        return decimal(name, value, scale, TgVariableBigDecimal.DEFAULT_ROUNDING_MODE);
    }

    /**
     * add value(decimal)
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale
     * @param mode  rounding mode
     * @return this
     */
    public TgParameterList decimal(@Nonnull String name, @Nullable BigDecimal value, int scale, @Nonnull RoundingMode mode) {
        var value0 = (value != null) ? value.setScale(scale, mode) : null;
        return decimal(name, value0);
    }

    /**
     * add value(String)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList character(@Nonnull String name, @Nullable String value) {
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
    public TgParameterList bytes(@Nonnull String name, @Nullable byte[] value) {
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
    public TgParameterList bits(@Nonnull String name, @Nullable boolean[] value) {
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
    public TgParameterList date(@Nonnull String name, @Nullable LocalDate value) {
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
    public TgParameterList time(@Nonnull String name, @Nullable LocalTime value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(dateTime)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList dateTime(@Nonnull String name, @Nullable LocalDateTime value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(offset time)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList offsetTime(@Nonnull String name, @Nullable OffsetTime value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(offset dateTime)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList offsetDateTime(@Nonnull String name, @Nullable OffsetDateTime value) {
        add(IceaxeLowParameterUtil.create(name, value));
        return this;
    }

    /**
     * add value(zoned dateTime)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList zonedDateTime(@Nonnull String name, @Nullable ZonedDateTime value) {
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
    public TgParameterList add(@Nonnull String name, boolean value) {
        return bool(name, value);
    }

    /**
     * add value(boolean)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable Boolean value) {
        return bool(name, value);
    }

    /**
     * add value(int)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, int value) {
        return int4(name, value);
    }

    /**
     * add value(int)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable Integer value) {
        return int4(name, value);
    }

    /**
     * add value(long)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, long value) {
        return int8(name, value);
    }

    /**
     * add value(long)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable Long value) {
        return int8(name, value);
    }

    /**
     * add value(float)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, float value) {
        return float4(name, value);
    }

    /**
     * add value(float)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable Float value) {
        return float4(name, value);
    }

    /**
     * add value(double)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, double value) {
        return float8(name, value);
    }

    /**
     * add value(double)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable Double value) {
        return float8(name, value);
    }

    /**
     * add value(decimal)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable BigDecimal value) {
        return decimal(name, value);
    }

    /**
     * add value(decimal)
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale. see {@link TgVariableBigDecimal#DEFAULT_ROUNDING_MODE}
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable BigDecimal value, int scale) {
        return decimal(name, value, scale);
    }

    /**
     * add value(decimal)
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale
     * @param mode  rounding mode
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable BigDecimal value, int scale, RoundingMode mode) {
        return decimal(name, value, scale, mode);
    }

    /**
     * add value(String)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable String value) {
        return character(name, value);
    }

    /**
     * add value(byte[])
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable byte[] value) {
        return bytes(name, value);
    }

    /**
     * add value(boolean[])
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable boolean[] value) {
        return bits(name, value);
    }

    /**
     * add value(date)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable LocalDate value) {
        return date(name, value);
    }

    /**
     * add value(time)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable LocalTime value) {
        return time(name, value);
    }

    /**
     * add value(dateTime)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable LocalDateTime value) {
        return dateTime(name, value);
    }

    /**
     * add value(offset time)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable OffsetTime value) {
        return offsetTime(name, value);
    }

    /**
     * add value(offset dateTime)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable OffsetDateTime value) {
        return offsetDateTime(name, value);
    }

    /**
     * add value(zoned dateTime)
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterList add(@Nonnull String name, @Nullable ZonedDateTime value) {
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
