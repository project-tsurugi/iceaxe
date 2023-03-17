package com.tsurugidb.iceaxe.sql.parameter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TgDataType;

/**
 * Tsurugi Bind Variable
 *
 * @param <T> data type
 * @see TgBindVariables#of(TgBindVariable...)
 */
public abstract class TgBindVariable<T> {

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableBoolean ofBoolean(@Nonnull String name) {
        return new TgBindVariableBoolean(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Boolean&gt;
     */
    public static class TgBindVariableBoolean extends TgBindVariable<Boolean> {

        protected TgBindVariableBoolean(@Nonnull String name) {
            super(name, TgDataType.BOOLEAN);
        }

        /**
         * bind value
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(boolean value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindParameter bind(@Nullable Boolean value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableBoolean clone(@Nonnull String name) {
            return new TgBindVariableBoolean(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableInteger ofInt(@Nonnull String name) {
        return new TgBindVariableInteger(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Integer&gt;
     */
    public static class TgBindVariableInteger extends TgBindVariable<Integer> {

        protected TgBindVariableInteger(@Nonnull String name) {
            super(name, TgDataType.INT);
        }

        /**
         * bind value
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(int value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindParameter bind(@Nullable Integer value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableInteger clone(@Nonnull String name) {
            return new TgBindVariableInteger(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLong ofLong(@Nonnull String name) {
        return new TgBindVariableLong(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Long&gt;
     */
    public static class TgBindVariableLong extends TgBindVariable<Long> {

        protected TgBindVariableLong(@Nonnull String name) {
            super(name, TgDataType.LONG);
        }

        /**
         * bind value
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(long value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindParameter bind(@Nullable Long value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableLong clone(@Nonnull String name) {
            return new TgBindVariableLong(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableFloat ofFloat(@Nonnull String name) {
        return new TgBindVariableFloat(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Float&gt;
     */
    public static class TgBindVariableFloat extends TgBindVariable<Float> {

        protected TgBindVariableFloat(@Nonnull String name) {
            super(name, TgDataType.FLOAT);
        }

        /**
         * bind value
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(float value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindParameter bind(@Nullable Float value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableFloat clone(@Nonnull String name) {
            return new TgBindVariableFloat(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableDouble ofDouble(@Nonnull String name) {
        return new TgBindVariableDouble(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Double&gt;
     */
    public static class TgBindVariableDouble extends TgBindVariable<Double> {

        protected TgBindVariableDouble(@Nonnull String name) {
            super(name, TgDataType.DOUBLE);
        }

        /**
         * bind value
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(double value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindParameter bind(@Nullable Double value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableDouble clone(@Nonnull String name) {
            return new TgBindVariableDouble(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable (don't round)
     */
    public static TgBindVariableBigDecimal ofDecimal(@Nonnull String name) {
        return new TgBindVariableBigDecimal(name);
    }

    /**
     * create bind variable
     *
     * @param name  name
     * @param scale rounding scale. see {@link TgBindVariableBigDecimal#DEFAULT_ROUNDING_MODE}
     * @return bind variable
     */
    public static TgBindVariableBigDecimal ofDecimal(@Nonnull String name, int scale) {
        return ofDecimal(name, scale, TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE);
    }

    /**
     * create bind variable
     *
     * @param name  name
     * @param scale rounding scale
     * @param mode  rounding mode
     * @return bind variable
     */
    public static TgBindVariableBigDecimal ofDecimal(@Nonnull String name, int scale, RoundingMode mode) {
        return new TgBindVariableBigDecimal(name, scale, mode);
    }

    /**
     * Tsurugi Bind Variable&lt;BigDecimal&gt;
     */
    public static class TgBindVariableBigDecimal extends TgBindVariable<BigDecimal> {

        /**
         * default rounding mode.
         *
         * @see TgBindVariable#ofDecimal(String, int)
         */
        public static final RoundingMode DEFAULT_ROUNDING_MODE = RoundingMode.DOWN;

        private final int scale;
        private RoundingMode roundingMode;

        protected TgBindVariableBigDecimal(@Nonnull String name) {
            this(name, Integer.MAX_VALUE, null);
        }

        protected TgBindVariableBigDecimal(@Nonnull String name, int scale, @Nullable RoundingMode mode) {
            super(name, TgDataType.DECIMAL);
            this.scale = scale;
            this.roundingMode = mode;
        }

        /**
         * get scale.
         *
         * @return scale
         */
        public int scale() {
            return this.scale;
        }

        /**
         * get rounding mode.
         *
         * @return rounding mode
         */
        @Nullable
        public RoundingMode roundingMode() {
            return this.roundingMode;
        }

        /**
         * bind value
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(long value) {
            return bind(BigDecimal.valueOf(value));
        }

        /**
         * bind value
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(double value) {
            return bind(BigDecimal.valueOf(value));
        }

        @Override
        public TgBindParameter bind(@Nullable BigDecimal value) {
            return TgBindParameter.of(name(), roundValue(value, scale, roundingMode));
        }

        /**
         * round value
         *
         * @param value        value
         * @param scale        scale
         * @param roundingMode rounding mode
         * @return rounded value
         */
        public static BigDecimal roundValue(@Nullable BigDecimal value, int scale, RoundingMode roundingMode) {
            if (value != null && roundingMode != null) {
                return value.setScale(scale, roundingMode);
            } else {
                return value;
            }
        }

        @Override
        public TgBindVariableBigDecimal clone(@Nonnull String name) {
            return new TgBindVariableBigDecimal(name, scale, roundingMode);
        }

        @Override
        protected String typeComment() {
            String base = super.typeComment();
            if (this.roundingMode == null) {
                return base;
            }
            return String.format("%s<%s %d>", base, roundingMode, scale);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableString ofString(@Nonnull String name) {
        return new TgBindVariableString(name);
    }

    /**
     * Tsurugi Bind Variable&lt;String&gt;
     */
    public static class TgBindVariableString extends TgBindVariable<String> {

        protected TgBindVariableString(@Nonnull String name) {
            super(name, TgDataType.STRING);
        }

        @Override
        public TgBindParameter bind(@Nullable String value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableString clone(@Nonnull String name) {
            return new TgBindVariableString(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableBytes ofBytes(@Nonnull String name) {
        return new TgBindVariableBytes(name);
    }

    /**
     * Tsurugi Bind Variable&lt;byte[]&gt;
     */
    public static class TgBindVariableBytes extends TgBindVariable<byte[]> {

        protected TgBindVariableBytes(@Nonnull String name) {
            super(name, TgDataType.BYTES);
        }

        @Override
        public TgBindParameter bind(@Nullable byte[] value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableBytes clone(@Nonnull String name) {
            return new TgBindVariableBytes(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariable<boolean[]> ofBits(@Nonnull String name) {
        return new TgBindVariableBits(name);
    }

    /**
     * Tsurugi Bind Variable&lt;boolean[]&gt;
     */
    public static class TgBindVariableBits extends TgBindVariable<boolean[]> {

        protected TgBindVariableBits(@Nonnull String name) {
            super(name, TgDataType.BITS);
        }

        @Override
        public TgBindParameter bind(@Nullable boolean[] value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableBits clone(@Nonnull String name) {
            return new TgBindVariableBits(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLocalDate ofDate(@Nonnull String name) {
        return new TgBindVariableLocalDate(name);
    }

    /**
     * Tsurugi Bind Variable&lt;LocalDate&gt;
     */
    public static class TgBindVariableLocalDate extends TgBindVariable<LocalDate> {

        protected TgBindVariableLocalDate(@Nonnull String name) {
            super(name, TgDataType.DATE);
        }

        @Override
        public TgBindParameter bind(@Nullable LocalDate value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableLocalDate clone(@Nonnull String name) {
            return new TgBindVariableLocalDate(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLocalTime ofTime(@Nonnull String name) {
        return new TgBindVariableLocalTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;LocalTime&gt;
     */
    public static class TgBindVariableLocalTime extends TgBindVariable<LocalTime> {

        protected TgBindVariableLocalTime(@Nonnull String name) {
            super(name, TgDataType.TIME);
        }

        @Override
        public TgBindParameter bind(@Nullable LocalTime value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableLocalTime clone(@Nonnull String name) {
            return new TgBindVariableLocalTime(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLocalDateTime ofDateTime(@Nonnull String name) {
        return new TgBindVariableLocalDateTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;LocalDateTime&gt;
     */
    public static class TgBindVariableLocalDateTime extends TgBindVariable<LocalDateTime> {

        protected TgBindVariableLocalDateTime(@Nonnull String name) {
            super(name, TgDataType.DATE_TIME);
        }

        @Override
        public TgBindParameter bind(@Nullable LocalDateTime value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableLocalDateTime clone(@Nonnull String name) {
            return new TgBindVariableLocalDateTime(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableOffsetTime ofOffsetTime(@Nonnull String name) {
        return new TgBindVariableOffsetTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;OffsetTime&gt;
     */
    public static class TgBindVariableOffsetTime extends TgBindVariable<OffsetTime> {

        protected TgBindVariableOffsetTime(@Nonnull String name) {
            super(name, TgDataType.OFFSET_TIME);
        }

        @Override
        public TgBindParameter bind(@Nullable OffsetTime value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableOffsetTime clone(@Nonnull String name) {
            return new TgBindVariableOffsetTime(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableOffsetDateTime ofOffsetDateTime(@Nonnull String name) {
        return new TgBindVariableOffsetDateTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;OffsetDateTime&gt;
     */
    public static class TgBindVariableOffsetDateTime extends TgBindVariable<OffsetDateTime> {

        protected TgBindVariableOffsetDateTime(@Nonnull String name) {
            super(name, TgDataType.OFFSET_DATE_TIME);
        }

        @Override
        public TgBindParameter bind(@Nullable OffsetDateTime value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableOffsetDateTime clone(@Nonnull String name) {
            return new TgBindVariableOffsetDateTime(name);
        }
    }

    /**
     * create bind variable
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableZonedDateTime ofZonedDateTime(@Nonnull String name) {
        return new TgBindVariableZonedDateTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;ZonedDateTime&gt;
     */
    public static class TgBindVariableZonedDateTime extends TgBindVariable<ZonedDateTime> {

        protected TgBindVariableZonedDateTime(@Nonnull String name) {
            super(name, TgDataType.ZONED_DATE_TIME);
        }

        @Override
        public TgBindParameter bind(@Nullable ZonedDateTime value) {
            return TgBindParameter.of(name(), value);
        }

        @Override
        public TgBindVariableZonedDateTime clone(@Nonnull String name) {
            return new TgBindVariableZonedDateTime(name);
        }
    }

    private final String name;
    private final TgDataType type;

    protected TgBindVariable(@Nonnull String name, @Nonnull TgDataType type) {
        this.name = name;
        this.type = type;
    }

    /**
     * get name
     *
     * @return name
     */
    @Nonnull
    public String name() {
        return this.name;
    }

    /**
     * get name
     *
     * @return name
     */
    @Nonnull
    public String sqlName() {
        return ":" + this.name;
    }

    /**
     * get type
     *
     * @return type
     */
    @Nonnull
    public TgDataType type() {
        return this.type;
    }

    /**
     * bind value
     *
     * @param value value
     * @return bind parameter
     */
    @Nonnull
    public abstract TgBindParameter bind(@Nullable T value);

    /**
     * copy with the same type
     *
     * @param name name
     * @return bind variable
     */
    @Nonnull
    public abstract TgBindVariable<T> clone(@Nonnull String name);

    @Override
    public String toString() {
        return sqlName() + "/*" + typeComment() + "*/";
    }

    protected String typeComment() {
        return String.valueOf(type);
    }
}
