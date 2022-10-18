package com.tsurugidb.iceaxe.statement;

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

/**
 * Tsurugi Variable
 *
 * @param <T> data type
 * @see TgVariableList#of(TgVariable...)
 */
public abstract class TgVariable<T> {

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableBoolean ofBoolean(@Nonnull String name) {
        return new TgVariableBoolean(name);
    }

    /**
     * Tsurugi Variable&lt;Boolean&gt;
     */
    public static class TgVariableBoolean extends TgVariable<Boolean> {

        protected TgVariableBoolean(@Nonnull String name) {
            super(name, TgDataType.BOOLEAN);
        }

        /**
         * bind value
         *
         * @param value value
         * @return Tsurugi Parameter
         */
        public TgParameter bind(boolean value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgParameter bind(@Nullable Boolean value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableBoolean copy(@Nonnull String name) {
            return new TgVariableBoolean(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableInteger ofInt4(@Nonnull String name) {
        return new TgVariableInteger(name);
    }

    /**
     * Tsurugi Variable&lt;Integer&gt;
     */
    public static class TgVariableInteger extends TgVariable<Integer> {

        protected TgVariableInteger(@Nonnull String name) {
            super(name, TgDataType.INT4);
        }

        /**
         * bind value
         *
         * @param value value
         * @return Tsurugi Parameter
         */
        public TgParameter bind(int value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgParameter bind(@Nullable Integer value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableInteger copy(@Nonnull String name) {
            return new TgVariableInteger(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableLong ofInt8(@Nonnull String name) {
        return new TgVariableLong(name);
    }

    /**
     * Tsurugi Variable&lt;Long&gt;
     */
    public static class TgVariableLong extends TgVariable<Long> {

        protected TgVariableLong(@Nonnull String name) {
            super(name, TgDataType.INT8);
        }

        /**
         * bind value
         *
         * @param value value
         * @return Tsurugi Parameter
         */
        public TgParameter bind(long value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgParameter bind(@Nullable Long value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableLong copy(@Nonnull String name) {
            return new TgVariableLong(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableFloat ofFloat4(@Nonnull String name) {
        return new TgVariableFloat(name);
    }

    /**
     * Tsurugi Variable&lt;Float&gt;
     */
    public static class TgVariableFloat extends TgVariable<Float> {

        protected TgVariableFloat(@Nonnull String name) {
            super(name, TgDataType.FLOAT4);
        }

        /**
         * bind value
         *
         * @param value value
         * @return Tsurugi Parameter
         */
        public TgParameter bind(float value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgParameter bind(@Nullable Float value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableFloat copy(@Nonnull String name) {
            return new TgVariableFloat(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableDouble ofFloat8(@Nonnull String name) {
        return new TgVariableDouble(name);
    }

    /**
     * Tsurugi Variable&lt;Double&gt;
     */
    public static class TgVariableDouble extends TgVariable<Double> {

        protected TgVariableDouble(@Nonnull String name) {
            super(name, TgDataType.FLOAT8);
        }

        /**
         * bind value
         *
         * @param value value
         * @return Tsurugi Parameter
         */
        public TgParameter bind(double value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgParameter bind(@Nullable Double value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableDouble copy(@Nonnull String name) {
            return new TgVariableDouble(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable (don't round)
     */
    public static TgVariableBigDecimal ofDecimal(@Nonnull String name) {
        return new TgVariableBigDecimal(name);
    }

    /**
     * create Tsurugi Variable
     *
     * @param name  name
     * @param scale rounding scale. see {@link TgVariableBigDecimal#DEFAULT_ROUNDING_MODE}
     * @return Tsurugi Variable
     */
    public static TgVariableBigDecimal ofDecimal(@Nonnull String name, int scale) {
        return ofDecimal(name, scale, TgVariableBigDecimal.DEFAULT_ROUNDING_MODE);
    }

    /**
     * create Tsurugi Variable
     *
     * @param name  name
     * @param scale rounding scale
     * @param mode  rounding mode
     * @return Tsurugi Variable
     */
    public static TgVariableBigDecimal ofDecimal(@Nonnull String name, int scale, RoundingMode mode) {
        return new TgVariableBigDecimal(name, scale, mode);
    }

    /**
     * Tsurugi Variable&lt;BigDecimal&gt;
     */
    public static class TgVariableBigDecimal extends TgVariable<BigDecimal> {

        /**
         * default rounding mode.
         *
         * @see TgVariable#ofDecimal(String, int)
         */
        public static final RoundingMode DEFAULT_ROUNDING_MODE = RoundingMode.DOWN;

        private final int scale;
        private RoundingMode roundingMode;

        protected TgVariableBigDecimal(@Nonnull String name) {
            this(name, Integer.MAX_VALUE, null);
        }

        protected TgVariableBigDecimal(@Nonnull String name, int scale, @Nullable RoundingMode mode) {
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
         * @return Tsurugi Parameter
         */
        public TgParameter bind(long value) {
            return bind(BigDecimal.valueOf(value));
        }

        /**
         * bind value
         *
         * @param value value
         * @return Tsurugi Parameter
         */
        public TgParameter bind(double value) {
            return bind(BigDecimal.valueOf(value));
        }

        @Override
        public TgParameter bind(@Nullable BigDecimal value) {
            var value0 = (value != null && roundingMode != null) ? value.setScale(scale, roundingMode) : value;
            return TgParameter.of(name(), value0);
        }

        @Override
        public TgVariableBigDecimal copy(@Nonnull String name) {
            return new TgVariableBigDecimal(name, scale, roundingMode);
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
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableString ofCharacter(@Nonnull String name) {
        return new TgVariableString(name);
    }

    /**
     * Tsurugi Variable&lt;String&gt;
     */
    public static class TgVariableString extends TgVariable<String> {

        protected TgVariableString(@Nonnull String name) {
            super(name, TgDataType.CHARACTER);
        }

        @Override
        public TgParameter bind(@Nullable String value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableString copy(@Nonnull String name) {
            return new TgVariableString(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableBytes ofBytes(@Nonnull String name) {
        return new TgVariableBytes(name);
    }

    /**
     * Tsurugi Variable&lt;byte[]&gt;
     */
    public static class TgVariableBytes extends TgVariable<byte[]> {

        protected TgVariableBytes(@Nonnull String name) {
            super(name, TgDataType.BYTES);
        }

        @Override
        public TgParameter bind(@Nullable byte[] value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableBytes copy(@Nonnull String name) {
            return new TgVariableBytes(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<boolean[]> ofBits(@Nonnull String name) {
        return new TgVariableBits(name);
    }

    /**
     * Tsurugi Variable&lt;boolean[]&gt;
     */
    public static class TgVariableBits extends TgVariable<boolean[]> {

        protected TgVariableBits(@Nonnull String name) {
            super(name, TgDataType.BITS);
        }

        @Override
        public TgParameter bind(@Nullable boolean[] value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableBits copy(@Nonnull String name) {
            return new TgVariableBits(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableLocalDate ofDate(@Nonnull String name) {
        return new TgVariableLocalDate(name);
    }

    /**
     * Tsurugi Variable&lt;LocalDate&gt;
     */
    public static class TgVariableLocalDate extends TgVariable<LocalDate> {

        protected TgVariableLocalDate(@Nonnull String name) {
            super(name, TgDataType.DATE);
        }

        @Override
        public TgParameter bind(@Nullable LocalDate value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableLocalDate copy(@Nonnull String name) {
            return new TgVariableLocalDate(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableLocalTime ofTime(@Nonnull String name) {
        return new TgVariableLocalTime(name);
    }

    /**
     * Tsurugi Variable&lt;LocalTime&gt;
     */
    public static class TgVariableLocalTime extends TgVariable<LocalTime> {

        protected TgVariableLocalTime(@Nonnull String name) {
            super(name, TgDataType.TIME);
        }

        @Override
        public TgParameter bind(@Nullable LocalTime value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableLocalTime copy(@Nonnull String name) {
            return new TgVariableLocalTime(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableLocalDateTime ofDateTime(@Nonnull String name) {
        return new TgVariableLocalDateTime(name);
    }

    /**
     * Tsurugi Variable&lt;LocalDateTime&gt;
     */
    public static class TgVariableLocalDateTime extends TgVariable<LocalDateTime> {

        protected TgVariableLocalDateTime(@Nonnull String name) {
            super(name, TgDataType.DATE_TIME);
        }

        @Override
        public TgParameter bind(@Nullable LocalDateTime value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableLocalDateTime copy(@Nonnull String name) {
            return new TgVariableLocalDateTime(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableOffsetTime ofOffsetTime(@Nonnull String name) {
        return new TgVariableOffsetTime(name);
    }

    /**
     * Tsurugi Variable&lt;OffsetTime&gt;
     */
    public static class TgVariableOffsetTime extends TgVariable<OffsetTime> {

        protected TgVariableOffsetTime(@Nonnull String name) {
            super(name, TgDataType.OFFSET_TIME);
        }

        @Override
        public TgParameter bind(@Nullable OffsetTime value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableOffsetTime copy(@Nonnull String name) {
            return new TgVariableOffsetTime(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableOffsetDateTime ofOffsetDateTime(@Nonnull String name) {
        return new TgVariableOffsetDateTime(name);
    }

    /**
     * Tsurugi Variable&lt;OffsetDateTime&gt;
     */
    public static class TgVariableOffsetDateTime extends TgVariable<OffsetDateTime> {

        protected TgVariableOffsetDateTime(@Nonnull String name) {
            super(name, TgDataType.OFFSET_DATE_TIME);
        }

        @Override
        public TgParameter bind(@Nullable OffsetDateTime value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableOffsetDateTime copy(@Nonnull String name) {
            return new TgVariableOffsetDateTime(name);
        }
    }

    /**
     * create Tsurugi Variable
     *
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableZonedDateTime ofZonedDateTime(@Nonnull String name) {
        return new TgVariableZonedDateTime(name);
    }

    /**
     * Tsurugi Variable&lt;ZonedDateTime&gt;
     */
    public static class TgVariableZonedDateTime extends TgVariable<ZonedDateTime> {

        protected TgVariableZonedDateTime(@Nonnull String name) {
            super(name, TgDataType.OFFSET_DATE_TIME);
        }

        @Override
        public TgParameter bind(@Nullable ZonedDateTime value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableZonedDateTime copy(@Nonnull String name) {
            return new TgVariableZonedDateTime(name);
        }
    }

    private final String name;
    private final TgDataType type;

    protected TgVariable(@Nonnull String name, @Nonnull TgDataType type) {
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
     * @return Tsurugi Parameter
     */
    @Nonnull
    public abstract TgParameter bind(@Nullable T value);

    /**
     * copy with the same type
     *
     * @param name name
     * @return variable
     */
    @Nonnull
    public abstract TgVariable<T> copy(@Nonnull String name);

    @Override
    public String toString() {
        return sqlName() + "/*" + typeComment() + "*/";
    }

    protected String typeComment() {
        return String.valueOf(type);
    }
}
