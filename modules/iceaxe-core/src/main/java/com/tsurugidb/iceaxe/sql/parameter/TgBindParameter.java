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
import java.nio.file.Path;
import java.text.MessageFormat;
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

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
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

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     * @since X.X.X
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable TgBlob value) {
        var closeable = (value != null && value.isDeleteOnExecuteFinished()) ? value : null;
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), closeable, () -> toString(name, value, TgBlob.class));
    }

    /**
     * create bind parameter.
     *
     * @param name name
     * @param path path
     * @return bind parameter
     * @since X.X.X
     */
    public static TgBindParameter ofBlob(@Nonnull String name, @Nullable Path path) {
        return new TgBindParameter(IceaxeLowParameterUtil.createBlob(name, path), () -> toString(name, path, Path.class));
    }

    /**
     * create bind parameter.
     *
     * @param name name
     * @param is   input stream
     * @return bind parameter
     * @throws IOException if an I/O error occurs when reading or writing
     * @since X.X.X
     */
    public static TgBindParameter ofBlob(@Nonnull String name, @Nullable InputStream is) throws IOException {
        TgBlob blob;
        if (is == null) {
            blob = null;
        } else {
            var factory = IceaxeObjectFactory.getDefaultInstance();
            blob = factory.createBlob(is, true);
        }
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, blob), blob, () -> toString(name, is, InputStream.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     * @throws IOException if an I/O error occurs writing to the file
     * @since X.X.X
     */
    public static TgBindParameter ofBlob(@Nonnull String name, @Nullable byte[] value) throws IOException {
        TgBlob blob;
        if (value == null) {
            blob = null;
        } else {
            var factory = IceaxeObjectFactory.getDefaultInstance();
            blob = factory.createBlob(value, true);
        }
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, blob), blob, () -> toString(name, value, byte[].class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     * @since X.X.X
     */
    public static TgBindParameter of(@Nonnull String name, @Nullable TgClob value) {
        var closeable = (value != null && value.isDeleteOnExecuteFinished()) ? value : null;
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, value), closeable, () -> toString(name, value, TgClob.class));
    }

    /**
     * create bind parameter.
     *
     * @param name name
     * @param path path
     * @return bind parameter
     * @since X.X.X
     */
    public static TgBindParameter ofClob(@Nonnull String name, @Nullable Path path) {
        return new TgBindParameter(IceaxeLowParameterUtil.createClob(name, path), () -> toString(name, path, Path.class));
    }

    /**
     * create bind parameter.
     *
     * @param name   name
     * @param reader reader
     * @return bind parameter
     * @throws IOException if an I/O error occurs when reading or writing
     * @since X.X.X
     */
    public static TgBindParameter ofClob(@Nonnull String name, @Nullable Reader reader) throws IOException {
        TgClob clob;
        if (reader == null) {
            clob = null;
        } else {
            var factory = IceaxeObjectFactory.getDefaultInstance();
            clob = factory.createClob(reader, true);
        }
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, clob), clob, () -> toString(name, reader, Reader.class));
    }

    /**
     * create bind parameter.
     *
     * @param name  name
     * @param value value
     * @return bind parameter
     * @throws IOException if an I/O error occurs writing to the file
     * @since X.X.X
     */
    public static TgBindParameter ofClob(@Nonnull String name, @Nullable String value) throws IOException {
        TgClob clob;
        if (value == null) {
            clob = null;
        } else {
            var factory = IceaxeObjectFactory.getDefaultInstance();
            clob = factory.createClob(value, true);
        }
        return new TgBindParameter(IceaxeLowParameterUtil.create(name, clob), clob, () -> toString(name, value, String.class));
    }

    /**
     * create bind parameter.
     *
     * @param name name
     * @param type type
     * @param path value
     * @return bind parameter
     * @since X.X.X
     */
    public static TgBindParameter of(@Nonnull String name, @Nonnull TgDataType type, @Nullable Path path) {
        switch (type) {
        case BLOB:
            return ofBlob(name, path);
        case CLOB:
            return ofClob(name, path);
        default:
            throw new IllegalArgumentException(MessageFormat.format("unsupported type. type={0}", type));
        }
    }

    private final Parameter lowParameter;
    private final IceaxeTimeoutCloseable closeable;
    private final Supplier<String> stringSupplier;

    /**
     * Creates a new instance.
     *
     * @param lowParameter   low parameter
     * @param stringSupplier string supplier
     */
    protected TgBindParameter(Parameter lowParameter, Supplier<String> stringSupplier) {
        this(lowParameter, null, stringSupplier);
    }

    /**
     * Creates a new instance.
     *
     * @param lowParameter   low parameter
     * @param closeable      object to close on execute finished
     * @param stringSupplier string supplier
     * @since X.X.X
     */
    protected TgBindParameter(Parameter lowParameter, IceaxeTimeoutCloseable closeable, Supplier<String> stringSupplier) {
        this.lowParameter = lowParameter;
        this.closeable = closeable;
        this.stringSupplier = stringSupplier;
    }

    /**
     * convert to {@link Parameter}.
     *
     * @param closeableSet Closeable set for execute finished
     * @return parameter
     */
    @IceaxeInternal
    public Parameter toLowParameter(IceaxeCloseableSet closeableSet) {
        if (this.closeable != null) {
            closeableSet.add(closeable);
        }
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
