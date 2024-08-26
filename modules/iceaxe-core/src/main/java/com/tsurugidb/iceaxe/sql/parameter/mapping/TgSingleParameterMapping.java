package com.tsurugidb.iceaxe.sql.parameter.mapping;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.BiFunction;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.IceaxeLowParameterUtil;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableBigDecimal;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.tsubakuro.sql.Placeholders;;

/**
 * Tsurugi Parameter Mapping for single variable.
 *
 * @param <P> parameter type (e.g. Integer, String)
 */
public class TgSingleParameterMapping<P> extends TgParameterMapping<P> {

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<Boolean> ofBoolean(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.BOOLEAN, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<Integer> ofInt(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.INT, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<Long> ofLong(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.LONG, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<Float> ofFloat(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.FLOAT, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<Double> ofDouble(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.DOUBLE, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<BigDecimal> ofDecimal(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.DECIMAL, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name  bind variable name
     * @param scale scale
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<BigDecimal> ofDecimal(String name, int scale) {
        return ofDecimal(name, scale, TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE);
    }

    /**
     * create parameter mapping.
     *
     * @param name  bind variable name
     * @param scale scale
     * @param mode  rounding mode
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<BigDecimal> ofDecimal(String name, int scale, RoundingMode mode) {
        return new TgSingleParameterMapping<>(name, TgDataType.DECIMAL, (name0, value) -> IceaxeLowParameterUtil.create(name0, TgBindVariableBigDecimal.roundValue(value, scale, mode)));
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<String> ofString(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.STRING, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<byte[]> ofBytes(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.BYTES, IceaxeLowParameterUtil::create);
    }

    /**
     * <em>This method is not yet implemented:</em>
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<boolean[]> ofBits(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.BITS, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<LocalDate> ofDate(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.DATE, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<LocalTime> ofTime(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.TIME, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<LocalDateTime> ofDateTime(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.DATE_TIME, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<OffsetTime> ofOffsetTime(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.OFFSET_TIME, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<OffsetDateTime> ofOffsetDateTime(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.OFFSET_DATE_TIME, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param name bind variable name
     * @return parameter mapping
     */
    public static TgSingleParameterMapping<ZonedDateTime> ofZonedDateTime(String name) {
        return new TgSingleParameterMapping<>(name, TgDataType.ZONED_DATE_TIME, IceaxeLowParameterUtil::create);
    }

    /**
     * create parameter mapping.
     *
     * @param <P>   parameter type
     * @param name  bind variable name
     * @param clazz parameter type
     * @return parameter mapping
     */
    public static <P> TgSingleParameterMapping<P> of(String name, Class<P> clazz) {
        TgDataType type;
        try {
            type = TgDataType.of(clazz);
        } catch (UnsupportedOperationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        return of(name, type);
    }

    /**
     * create parameter mapping.
     *
     * @param <P>  parameter type
     * @param name bind variable name
     * @param type parameter type
     * @return parameter mapping
     */
    public static <P> TgSingleParameterMapping<P> of(String name, TgDataType type) {
        @SuppressWarnings("unchecked")
        var r = (TgSingleParameterMapping<P>) ofSingleRaw(name, type);
        return r;
    }

    private static TgSingleParameterMapping<?> ofSingleRaw(String name, TgDataType type) {
        switch (type) {
        case BOOLEAN:
            return ofBoolean(name);
        case INT:
            return ofInt(name);
        case LONG:
            return ofLong(name);
        case FLOAT:
            return ofFloat(name);
        case DOUBLE:
            return ofDouble(name);
        case DECIMAL:
            return ofDecimal(name);
        case STRING:
            return ofString(name);
        case BYTES:
            return ofBytes(name);
        case BITS:
            return ofBits(name);
        case DATE:
            return ofDate(name);
        case TIME:
            return ofTime(name);
        case DATE_TIME:
            return ofDateTime(name);
        case OFFSET_TIME:
            return ofOffsetTime(name);
        case OFFSET_DATE_TIME:
            return ofOffsetDateTime(name);
        case ZONED_DATE_TIME:
            return ofZonedDateTime(name);
        default:
            throw new IllegalArgumentException(MessageFormat.format("unsupported type. type={0}", type));
        }
    }

    /**
     * create parameter mapping.
     *
     * @param <P>      parameter type
     * @param variable bind variable
     * @return parameter mapping
     */
    public static <P> TgSingleParameterMapping<P> of(TgBindVariable<P> variable) {
        return new TgSingleParameterMapping<>(variable.name(), variable.type(), (name, value) -> variable.bind(value).toLowParameter());
    }

    private final String name;
    private final TgDataType type;
    private final BiFunction<String, P, Parameter> lowParameterGenerator;

    /**
     * Creates a new instance.
     *
     * @param name      bind variable name
     * @param type      parameter type
     * @param generator parameter generator
     */
    public TgSingleParameterMapping(String name, TgDataType type, BiFunction<String, P, Parameter> generator) {
        this.name = name;
        this.type = type;
        this.lowParameterGenerator = generator;
    }

    @Override
    public List<Placeholder> toLowPlaceholderList() {
        var lowPlaceholder = Placeholders.of(name, type.getLowDataType());
        return List.of(lowPlaceholder);
    }

    @Override
    public List<Parameter> toLowParameterList(P parameter, IceaxeConvertUtil convertUtil) {
        var lowParameter = lowParameterGenerator.apply(name, parameter);
        return List.of(lowParameter);
    }

    @Override
    public String toString() {
        return "TgSingleParameterMapping[:" + name + "/*" + type + "*/]";
    }
}
