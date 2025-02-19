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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableBigDecimal;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;

/**
 * Tsurugi Bind Parameters for {@link TsurugiSqlPrepared}.
 *
 * @see TsurugiSqlPreparedQuery#execute(com.tsurugidb.iceaxe.transaction.TsurugiTransaction, Object)
 * @see TsurugiSqlPreparedStatement#execute(com.tsurugidb.iceaxe.transaction.TsurugiTransaction, Object)
 */
public class TgBindParameters {

    /**
     * create bind parameters.
     *
     * @return bind parameters
     */
    public static TgBindParameters of() {
        return new TgBindParameters();
    }

    /**
     * create bind parameters.
     *
     * @param parameters bind parameter
     * @return bind parameters
     */
    public static TgBindParameters of(TgBindParameter... parameters) {
        var bp = new TgBindParameters();
        for (var parameter : parameters) {
            bp.add(parameter);
        }
        return bp;
    }

    /**
     * create bind parameters.
     *
     * @param parameters bind parameter
     * @return bind parameters
     */
    public static TgBindParameters of(Collection<? extends TgBindParameter> parameters) {
        var bp = new TgBindParameters();
        for (var parameter : parameters) {
            bp.add(parameter);
        }
        return bp;
    }

    /**
     * a function that always returns its input argument.
     */
    public static final Function<TgBindParameters, TgBindParameters> IDENTITY = p -> p;

    private final List<TgBindParameter> parameterList = new ArrayList<>();

    /**
     * Creates a new instance.
     */
    public TgBindParameters() {
        // do nothing
    }

    /**
     * add value(boolean).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addBoolean(@Nonnull String name, boolean value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(boolean).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addBoolean(@Nonnull String name, @Nullable Boolean value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(int).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addInt(@Nonnull String name, int value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(int).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addInt(@Nonnull String name, @Nullable Integer value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(long).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addLong(@Nonnull String name, long value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(long).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addLong(@Nonnull String name, @Nullable Long value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(float).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addFloat(@Nonnull String name, float value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(float).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addFloat(@Nonnull String name, @Nullable Float value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(double).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addDouble(@Nonnull String name, double value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(double).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addDouble(@Nonnull String name, @Nullable Double value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(decimal).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addDecimal(@Nonnull String name, @Nullable BigDecimal value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(decimal).
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale. see {@link TgBindVariableBigDecimal#DEFAULT_ROUNDING_MODE}
     * @return this
     */
    public TgBindParameters addDecimal(@Nonnull String name, @Nullable BigDecimal value, int scale) {
        return addDecimal(name, value, scale, TgBindVariableBigDecimal.DEFAULT_ROUNDING_MODE);
    }

    /**
     * add value(decimal).
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale
     * @param mode  rounding mode
     * @return this
     */
    public TgBindParameters addDecimal(@Nonnull String name, @Nullable BigDecimal value, int scale, @Nonnull RoundingMode mode) {
        var value0 = (value != null) ? value.setScale(scale, mode) : null;
        return addDecimal(name, value0);
    }

    /**
     * add value(String).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addString(@Nonnull String name, @Nullable String value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(byte[]).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addBytes(@Nonnull String name, @Nullable byte[] value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * <em>This method is not yet implemented:</em> add value(boolean[]).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addBits(@Nonnull String name, @Nullable boolean[] value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(date).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addDate(@Nonnull String name, @Nullable LocalDate value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(time).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addTime(@Nonnull String name, @Nullable LocalTime value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(dateTime).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addDateTime(@Nonnull String name, @Nullable LocalDateTime value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(offset time).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addOffsetTime(@Nonnull String name, @Nullable OffsetTime value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(offset dateTime).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addOffsetDateTime(@Nonnull String name, @Nullable OffsetDateTime value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(zoned dateTime).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters addZonedDateTime(@Nonnull String name, @Nullable ZonedDateTime value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(BLOB).
     *
     * @param name  name
     * @param value value
     * @return this
     * @since X.X.X
     */
    public TgBindParameters addBlob(@Nonnull String name, @Nullable TgBlob value) {
        add(TgBindParameter.of(name, value));
        return this;
    }

    /**
     * add value(BLOB).
     *
     * @param name name
     * @param path path
     * @return this
     * @since X.X.X
     */
    public TgBindParameters addBlob(@Nonnull String name, @Nullable Path path) {
        add(TgBindParameter.ofBlob(name, path));
        return this;
    }

    /**
     * add value(BLOB).
     *
     * @param name name
     * @param is   input stream
     * @return this
     * @throws IOException if an I/O error occurs when reading or writing
     * @since X.X.X
     */
    public TgBindParameters addBlob(@Nonnull String name, @Nullable InputStream is) throws IOException {
        add(TgBindParameter.ofBlob(name, is));
        return this;
    }

    /**
     * add value(BLOB).
     *
     * @param name  name
     * @param value value
     * @return this
     * @throws IOException if an I/O error occurs writing to the file
     * @since X.X.X
     */
    public TgBindParameters addBlob(@Nonnull String name, @Nullable byte[] value) throws IOException {
        add(TgBindParameter.ofBlob(name, value));
        return this;
    }

    /**
     * add value(boolean).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, boolean value) {
        return addBoolean(name, value);
    }

    /**
     * add value(boolean).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable Boolean value) {
        return addBoolean(name, value);
    }

    /**
     * add value(int).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, int value) {
        return addInt(name, value);
    }

    /**
     * add value(int).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable Integer value) {
        return addInt(name, value);
    }

    /**
     * add value(long).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, long value) {
        return addLong(name, value);
    }

    /**
     * add value(long).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable Long value) {
        return addLong(name, value);
    }

    /**
     * add value(float).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, float value) {
        return addFloat(name, value);
    }

    /**
     * add value(float).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable Float value) {
        return addFloat(name, value);
    }

    /**
     * add value(double).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, double value) {
        return addDouble(name, value);
    }

    /**
     * add value(double).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable Double value) {
        return addDouble(name, value);
    }

    /**
     * add value(decimal).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable BigDecimal value) {
        return addDecimal(name, value);
    }

    /**
     * add value(decimal).
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale. see {@link TgBindVariableBigDecimal#DEFAULT_ROUNDING_MODE}
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable BigDecimal value, int scale) {
        return addDecimal(name, value, scale);
    }

    /**
     * add value(decimal).
     *
     * @param name  name
     * @param value value
     * @param scale rounding scale
     * @param mode  rounding mode
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable BigDecimal value, int scale, RoundingMode mode) {
        return addDecimal(name, value, scale, mode);
    }

    /**
     * add value(String).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable String value) {
        return addString(name, value);
    }

    /**
     * add value(byte[]).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable byte[] value) {
        return addBytes(name, value);
    }

    /**
     * <em>This method is not yet implemented:</em> add value(boolean[]).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable boolean[] value) {
        return addBits(name, value);
    }

    /**
     * add value(date).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable LocalDate value) {
        return addDate(name, value);
    }

    /**
     * add value(time).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable LocalTime value) {
        return addTime(name, value);
    }

    /**
     * add value(dateTime).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable LocalDateTime value) {
        return addDateTime(name, value);
    }

    /**
     * add value(offset time).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable OffsetTime value) {
        return addOffsetTime(name, value);
    }

    /**
     * add value(offset dateTime).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable OffsetDateTime value) {
        return addOffsetDateTime(name, value);
    }

    /**
     * add value(zoned dateTime).
     *
     * @param name  name
     * @param value value
     * @return this
     */
    public TgBindParameters add(@Nonnull String name, @Nullable ZonedDateTime value) {
        return addZonedDateTime(name, value);
    }

    /**
     * add value(BLOB).
     *
     * @param name  name
     * @param value value
     * @return this
     * @since X.X.X
     */
    public TgBindParameters add(@Nonnull String name, @Nullable TgBlob value) {
        return addBlob(name, value);
    }

    /**
     * add value(Path).
     *
     * @param name name
     * @param type type
     * @param path path
     * @return this
     * @since X.X.X
     */
    public TgBindParameters add(@Nonnull String name, TgDataType type, @Nullable Path path) {
        switch (type) {
        case BLOB:
            return addBlob(name, path);
        case CLOB:
            // TODO CLOB
        default:
            throw new IllegalArgumentException(MessageFormat.format("unsupported type. type={0}", type));
        }
    }

    /**
     * add parameter.
     *
     * @param parameter parameter
     * @return this
     */
    public TgBindParameters add(TgBindParameter parameter) {
        parameterList.add(parameter);
        return this;
    }

    /**
     * add parameter.
     *
     * @param other parameters
     * @return this
     */
    public TgBindParameters add(TgBindParameters other) {
        parameterList.addAll(other.parameterList);
        return this;
    }

    /**
     * convert to {@link Parameter} list.
     *
     * @param closeableSet Closeable set for execute finished
     * @return parameter list
     */
    @IceaxeInternal
    public List<Parameter> toLowParameterList(IceaxeCloseableSet closeableSet) {
        var list = new ArrayList<Parameter>(parameterList.size());
        for (var parameter : parameterList) {
            list.add(parameter.toLowParameter(closeableSet));
        }
        return list;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + parameterList;
    }
}
