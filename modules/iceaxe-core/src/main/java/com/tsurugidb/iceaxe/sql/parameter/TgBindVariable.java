/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.sql.parameter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;

/**
 * Tsurugi Bind Variable.
 *
 * @param <T> data type
 * @see TgBindVariables#of(TgBindVariable...)
 */
public abstract class TgBindVariable<T> {

    /**
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableBoolean ofBoolean(@Nonnull String name) {
        return new TgBindVariableBoolean(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Boolean&gt;.
     */
    public static class TgBindVariableBoolean extends TgBindVariable<Boolean> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableBoolean(@Nonnull String name) {
            super(name, TgDataType.BOOLEAN);
        }

        /**
         * bind value.
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableInteger ofInt(@Nonnull String name) {
        return new TgBindVariableInteger(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Integer&gt;.
     */
    public static class TgBindVariableInteger extends TgBindVariable<Integer> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableInteger(@Nonnull String name) {
            super(name, TgDataType.INT);
        }

        /**
         * bind value.
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLong ofLong(@Nonnull String name) {
        return new TgBindVariableLong(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Long&gt;.
     */
    public static class TgBindVariableLong extends TgBindVariable<Long> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableLong(@Nonnull String name) {
            super(name, TgDataType.LONG);
        }

        /**
         * bind value.
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableFloat ofFloat(@Nonnull String name) {
        return new TgBindVariableFloat(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Float&gt;.
     */
    public static class TgBindVariableFloat extends TgBindVariable<Float> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableFloat(@Nonnull String name) {
            super(name, TgDataType.FLOAT);
        }

        /**
         * bind value.
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableDouble ofDouble(@Nonnull String name) {
        return new TgBindVariableDouble(name);
    }

    /**
     * Tsurugi Bind Variable&lt;Double&gt;.
     */
    public static class TgBindVariableDouble extends TgBindVariable<Double> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableDouble(@Nonnull String name) {
            super(name, TgDataType.DOUBLE);
        }

        /**
         * bind value.
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable (don't round)
     */
    public static TgBindVariableBigDecimal ofDecimal(@Nonnull String name) {
        return new TgBindVariableBigDecimal(name);
    }

    /**
     * create bind variable.
     *
     * @param name  name
     * @param scale rounding scale. see {@link TgBindVariableBigDecimal#DEFAULT_ROUNDING_MODE}
     * @return bind variable
     */
    public static TgBindVariableBigDecimal ofDecimal(@Nonnull String name, int scale) {
        return ofDecimal(name, scale, TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE);
    }

    /**
     * create bind variable.
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
     * Tsurugi Bind Variable&lt;BigDecimal&gt;.
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

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableBigDecimal(@Nonnull String name) {
            this(name, Integer.MAX_VALUE, null);
        }

        /**
         * Creates a new instance.
         *
         * @param name  name
         * @param scale scale
         * @param mode  rounding mode
         */
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
         * bind value.
         *
         * @param value value
         * @return bind parameter
         */
        public TgBindParameter bind(long value) {
            return bind(BigDecimal.valueOf(value));
        }

        /**
         * bind value.
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
         * round value.
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableString ofString(@Nonnull String name) {
        return new TgBindVariableString(name);
    }

    /**
     * Tsurugi Bind Variable&lt;String&gt;.
     */
    public static class TgBindVariableString extends TgBindVariable<String> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableBytes ofBytes(@Nonnull String name) {
        return new TgBindVariableBytes(name);
    }

    /**
     * Tsurugi Bind Variable&lt;byte[]&gt;.
     */
    public static class TgBindVariableBytes extends TgBindVariable<byte[]> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * <em>This method is not yet implemented:</em> create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariable<boolean[]> ofBits(@Nonnull String name) {
        return new TgBindVariableBits(name);
    }

    /**
     * <em>This class is not yet implemented:</em> Tsurugi Bind Variable&lt;boolean[]&gt;.
     */
    public static class TgBindVariableBits extends TgBindVariable<boolean[]> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLocalDate ofDate(@Nonnull String name) {
        return new TgBindVariableLocalDate(name);
    }

    /**
     * Tsurugi Bind Variable&lt;LocalDate&gt;.
     */
    public static class TgBindVariableLocalDate extends TgBindVariable<LocalDate> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLocalTime ofTime(@Nonnull String name) {
        return new TgBindVariableLocalTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;LocalTime&gt;.
     */
    public static class TgBindVariableLocalTime extends TgBindVariable<LocalTime> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableLocalDateTime ofDateTime(@Nonnull String name) {
        return new TgBindVariableLocalDateTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;LocalDateTime&gt;.
     */
    public static class TgBindVariableLocalDateTime extends TgBindVariable<LocalDateTime> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableOffsetTime ofOffsetTime(@Nonnull String name) {
        return new TgBindVariableOffsetTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;OffsetTime&gt;.
     */
    public static class TgBindVariableOffsetTime extends TgBindVariable<OffsetTime> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableOffsetDateTime ofOffsetDateTime(@Nonnull String name) {
        return new TgBindVariableOffsetDateTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;OffsetDateTime&gt;.
     */
    public static class TgBindVariableOffsetDateTime extends TgBindVariable<OffsetDateTime> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     */
    public static TgBindVariableZonedDateTime ofZonedDateTime(@Nonnull String name) {
        return new TgBindVariableZonedDateTime(name);
    }

    /**
     * Tsurugi Bind Variable&lt;ZonedDateTime&gt;.
     */
    public static class TgBindVariableZonedDateTime extends TgBindVariable<ZonedDateTime> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
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

    /**
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     * @since X.X.X
     */
    public static TgBindVariableBlob ofBlob(@Nonnull String name) {
        return new TgBindVariableBlob(name);
    }

    /**
     * Tsurugi Bind Variable&lt;TgBlob&gt;.
     *
     * @since X.X.X
     */
    public static class TgBindVariableBlob extends TgBindVariable<TgBlob> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableBlob(@Nonnull String name) {
            super(name, TgDataType.BLOB);
        }

        @Override
        public TgBindParameter bind(@Nullable TgBlob value) {
            return TgBindParameter.of(name(), value);
        }

        /**
         * bind value.
         *
         * @param path path
         * @return bind parameter
         */
        public TgBindParameter bind(@Nullable Path path) {
            return TgBindParameter.ofBlob(name(), path);
        }

        /**
         * bind value.
         *
         * @param is input stream
         * @return bind parameter
         * @throws IOException if an I/O error occurs when reading or writing
         */
        public TgBindParameter bind(@Nullable InputStream is) throws IOException {
            return TgBindParameter.ofBlob(name(), is);
        }

        /**
         * bind value.
         *
         * @param value value
         * @return bind parameter
         * @throws IOException if an I/O error occurs writing to the file
         */
        public TgBindParameter bind(@Nullable byte[] value) throws IOException {
            return TgBindParameter.ofBlob(name(), value);
        }

        @Override
        public TgBindVariableBlob clone(@Nonnull String name) {
            return new TgBindVariableBlob(name);
        }
    }

    /**
     * create bind variable.
     *
     * @param name name
     * @return bind variable
     * @since X.X.X
     */
    public static TgBindVariableClob ofClob(@Nonnull String name) {
        return new TgBindVariableClob(name);
    }

    /**
     * Tsurugi Bind Variable&lt;TgClob&gt;.
     *
     * @since X.X.X
     */
    public static class TgBindVariableClob extends TgBindVariable<TgClob> {

        /**
         * Creates a new instance.
         *
         * @param name name
         */
        protected TgBindVariableClob(@Nonnull String name) {
            super(name, TgDataType.CLOB);
        }

        @Override
        public TgBindParameter bind(@Nullable TgClob value) {
            return TgBindParameter.of(name(), value);
        }

        /**
         * bind value.
         *
         * @param path path
         * @return bind parameter
         */
        public TgBindParameter bind(@Nullable Path path) {
            return TgBindParameter.ofClob(name(), path);
        }

        /**
         * bind value.
         *
         * @param reader reader
         * @return bind parameter
         * @throws IOException if an I/O error occurs when reading or writing
         */
        public TgBindParameter bind(@Nullable Reader reader) throws IOException {
            return TgBindParameter.ofClob(name(), reader);
        }

        /**
         * bind value.
         *
         * @param value value
         * @return bind parameter
         * @throws IOException if an I/O error occurs writing to the file
         */
        public TgBindParameter bind(@Nullable String value) throws IOException {
            return TgBindParameter.ofClob(name(), value);
        }

        @Override
        public TgBindVariableClob clone(@Nonnull String name) {
            return new TgBindVariableClob(name);
        }
    }

    private final String name;
    private final TgDataType type;

    /**
     * Creates a new instance.
     *
     * @param name name
     * @param type type
     */
    protected TgBindVariable(@Nonnull String name, @Nonnull TgDataType type) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
    }

    /**
     * get name.
     *
     * @return name
     */
    public @Nonnull String name() {
        return this.name;
    }

    /**
     * get SQL name.
     *
     * @return SQL name
     */
    public @Nonnull String sqlName() {
        return ":" + this.name;
    }

    /**
     * get type.
     *
     * @return type
     */
    public @Nonnull TgDataType type() {
        return this.type;
    }

    /**
     * bind value.
     *
     * @param value value
     * @return bind parameter
     */
    public abstract @Nonnull TgBindParameter bind(@Nullable T value);

    /**
     * copy with the same type.
     *
     * @param name name
     * @return bind variable
     */
    public abstract @Nonnull TgBindVariable<T> clone(@Nonnull String name);

    @Override
    public String toString() {
        return sqlName() + "/*" + typeComment() + "*/";
    }

    /**
     * get type comment.
     *
     * @return comment
     */
    protected String typeComment() {
        return String.valueOf(type);
    }
}
