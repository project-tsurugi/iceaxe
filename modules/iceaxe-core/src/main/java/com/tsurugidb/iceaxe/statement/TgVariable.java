package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

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
    public static TgVariableBoolean ofBoolean(String name) {
        return new TgVariableBoolean(name);
    }

    /**
     * Tsurugi Variable&lt;Boolean&gt;
     */
    public static class TgVariableBoolean extends TgVariable<Boolean> {

        protected TgVariableBoolean(String name) {
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
        public TgParameter bind(Boolean value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableBoolean copy(String name) {
            return new TgVariableBoolean(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableInteger ofInt4(String name) {
        return new TgVariableInteger(name);
    }

    /**
     * Tsurugi Variable&lt;Integer&gt;
     */
    public static class TgVariableInteger extends TgVariable<Integer> {

        protected TgVariableInteger(String name) {
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
        public TgParameter bind(Integer value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableInteger copy(String name) {
            return new TgVariableInteger(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableLong ofInt8(String name) {
        return new TgVariableLong(name);
    }

    /**
     * Tsurugi Variable&lt;Long&gt;
     */
    public static class TgVariableLong extends TgVariable<Long> {

        protected TgVariableLong(String name) {
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
        public TgParameter bind(Long value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableLong copy(String name) {
            return new TgVariableLong(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableFloat ofFloat4(String name) {
        return new TgVariableFloat(name);
    }

    /**
     * Tsurugi Variable&lt;Float&gt;
     */
    public static class TgVariableFloat extends TgVariable<Float> {

        protected TgVariableFloat(String name) {
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
        public TgParameter bind(Float value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableFloat copy(String name) {
            return new TgVariableFloat(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableDouble ofFloat8(String name) {
        return new TgVariableDouble(name);
    }

    /**
     * Tsurugi Variable&lt;Double&gt;
     */
    public static class TgVariableDouble extends TgVariable<Double> {

        protected TgVariableDouble(String name) {
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
        public TgParameter bind(Double value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableDouble copy(String name) {
            return new TgVariableDouble(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableBigDecimal ofDecimal(String name) {
        return new TgVariableBigDecimal(name);
    }

    /**
     * Tsurugi Variable&lt;BigDecimal&gt;
     */
    public static class TgVariableBigDecimal extends TgVariable<BigDecimal> {

        protected TgVariableBigDecimal(String name) {
            super(name, TgDataType.DECIMAL);
        }

        @Override
        public TgParameter bind(BigDecimal value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableBigDecimal copy(String name) {
            return new TgVariableBigDecimal(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableString ofCharacter(String name) {
        return new TgVariableString(name);
    }

    /**
     * Tsurugi Variable&lt;String&gt;
     */
    public static class TgVariableString extends TgVariable<String> {

        protected TgVariableString(String name) {
            super(name, TgDataType.CHARACTER);
        }

        @Override
        public TgParameter bind(String value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableString copy(String name) {
            return new TgVariableString(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableBytes ofBytes(String name) {
        return new TgVariableBytes(name);
    }

    /**
     * Tsurugi Variable&lt;byte[]&gt;
     */
    public static class TgVariableBytes extends TgVariable<byte[]> {

        protected TgVariableBytes(String name) {
            super(name, TgDataType.BYTES);
        }

        @Override
        public TgParameter bind(byte[] value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableBytes copy(String name) {
            return new TgVariableBytes(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<boolean[]> ofBits(String name) {
        return new TgVariableBits(name);
    }

    /**
     * Tsurugi Variable&lt;boolean[]&gt;
     */
    public static class TgVariableBits extends TgVariable<boolean[]> {

        protected TgVariableBits(String name) {
            super(name, TgDataType.BITS);
        }

        @Override
        public TgParameter bind(boolean[] value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableBits copy(String name) {
            return new TgVariableBits(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableLocalDate ofDate(String name) {
        return new TgVariableLocalDate(name);
    }

    /**
     * Tsurugi Variable&lt;LocalDate&gt;
     */
    public static class TgVariableLocalDate extends TgVariable<LocalDate> {

        protected TgVariableLocalDate(String name) {
            super(name, TgDataType.DATE);
        }

        @Override
        public TgParameter bind(LocalDate value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableLocalDate copy(String name) {
            return new TgVariableLocalDate(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableLocalTime ofTime(String name) {
        return new TgVariableLocalTime(name);
    }

    /**
     * Tsurugi Variable&lt;LocalTime&gt;
     */
    public static class TgVariableLocalTime extends TgVariable<LocalTime> {

        protected TgVariableLocalTime(String name) {
            super(name, TgDataType.TIME);
        }

        @Override
        public TgParameter bind(LocalTime value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableLocalTime copy(String name) {
            return new TgVariableLocalTime(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableInstant ofInstant(String name) {
        return new TgVariableInstant(name);
    }

    /**
     * Tsurugi Variable&lt;Instant&gt;
     */
    public static class TgVariableInstant extends TgVariable<Instant> {

        protected TgVariableInstant(String name) {
            super(name, TgDataType.INSTANT);
        }

        @Override
        public TgParameter bind(Instant value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableInstant copy(String name) {
            return new TgVariableInstant(name);
        }
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariableZonedDateTime ofZonedDateTime(String name) {
        return new TgVariableZonedDateTime(name);
    }

    /**
     * Tsurugi Variable&lt;ZonedDateTime&gt;
     */
    public static class TgVariableZonedDateTime extends TgVariable<ZonedDateTime> {

        protected TgVariableZonedDateTime(String name) {
            super(name, TgDataType.INSTANT);
        }

        @Override
        public TgParameter bind(ZonedDateTime value) {
            return TgParameter.of(name(), value);
        }

        @Override
        public TgVariableZonedDateTime copy(String name) {
            return new TgVariableZonedDateTime(name);
        }
    }

    private final String name;
    private final TgDataType type;

    protected TgVariable(String name, TgDataType type) {
        this.name = name;
        this.type = type;
    }

    /**
     * get name
     * 
     * @return name
     */
    public String name() {
        return this.name;
    }

    /**
     * get name
     * 
     * @return name
     */
    public String sqlName() {
        return ":" + this.name;
    }

    /**
     * get type
     * 
     * @return type
     */
    public TgDataType type() {
        return this.type;
    }

    /**
     * bind value
     * 
     * @param value value
     * @return Tsurugi Parameter
     */
    public abstract TgParameter bind(T value);

    /**
     * copy with the same type
     * 
     * @param name name
     * @return variable
     */
    public abstract TgVariable<T> copy(String name);

    @Override
    public String toString() {
        return sqlName() + "/*" + type + "*/";
    }
}
