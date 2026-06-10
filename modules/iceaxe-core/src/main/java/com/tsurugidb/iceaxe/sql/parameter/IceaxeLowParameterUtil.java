/*
 * Copyright 2023-2026 Project Tsurugi.
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.Parameters;

/**
 * {@link Parameter} utility.
 */
@IceaxeInternal
public final class IceaxeLowParameterUtil {

    private IceaxeLowParameterUtil() {
        // do nothing
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, boolean value) {
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable Boolean value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, int value) {
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable Integer value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, long value) {
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable Long value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, float value) {
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable Float value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, double value) {
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable Double value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable BigDecimal value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable String value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable byte[] value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable boolean[] value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable LocalDate value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable LocalTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable LocalDateTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable OffsetTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable OffsetDateTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    /**
     * create parameter.
     *
     * @param name  parameter name
     * @param value value
     * @return parameter
     */
    public static Parameter create(@Nonnull String name, @Nullable ZonedDateTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        var offsetDateTime = value.toOffsetDateTime();
        return Parameters.of(name, offsetDateTime);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param value   value
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.8.0
     */
    public static Parameter create(@Nonnull String name, @Nullable TgBlob value, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return create0(name, value, context);
    }

    @SuppressWarnings("removal")
    private static Parameter create0(String name, TgBlob value, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (value.isDeleteOnExecuteFinished()) {
            context.closeableSet().add(value);
        }

        var remoteBlob = value.upload(context.session(), null);
        context.closeableSet().add(remoteBlob);

        var lowLobInfo = remoteBlob.getLowLargeObjectInfo();
        return Parameters.blobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param value   value
     * @param context context
     * @return parameter
     * @since 1.16.0
     */
    public static Parameter create(@Nonnull String name, @Nullable TgRemoteBlob value, IceaxeLowParameterGenerateContext context) {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        context.closeableSet().add(value);
        var lowLobInfo = value.getLowLargeObjectInfo();
        return Parameters.blobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param path    path
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.8.0
     */
    public static Parameter createBlob(@Nonnull String name, @Nullable Path path, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (path == null) {
            return Parameters.ofNull(name);
        }

        var lobFactory = context.session().getLobFactory();
        var remoteBlob = lobFactory.uploadBlob(path);
        context.closeableSet().add(remoteBlob);

        var lowLobInfo = remoteBlob.getLowLargeObjectInfo();
        return Parameters.blobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param is      input stream
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.16.0
     */
    public static Parameter createBlob(@Nonnull String name, @Nullable InputStream is, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (is == null) {
            return Parameters.ofNull(name);
        }

        var lobFactory = context.session().getLobFactory();
        var remoteBlob = lobFactory.uploadBlob(is);
        context.closeableSet().add(remoteBlob);

        var lowLobInfo = remoteBlob.getLowLargeObjectInfo();
        return Parameters.blobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param value   value
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.16.0
     */
    public static Parameter createBlob(@Nonnull String name, @Nullable byte[] value, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        var lobFactory = context.session().getLobFactory();
        var remoteBlob = lobFactory.uploadBlob(value);
        context.closeableSet().add(remoteBlob);

        var lowLobInfo = remoteBlob.getLowLargeObjectInfo();
        return Parameters.blobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param value   value
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.8.0
     */
    public static Parameter create(@Nonnull String name, @Nullable TgClob value, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return create0(name, value, context);
    }

    @SuppressWarnings("removal")
    private static Parameter create0(String name, TgClob value, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (value.isDeleteOnExecuteFinished()) {
            context.closeableSet().add(value);
        }

        var remoteClob = value.upload(context.session(), null);
        context.closeableSet().add(remoteClob);

        var lowLobInfo = remoteClob.getLowLargeObjectInfo();
        return Parameters.clobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param value   value
     * @param context context
     * @return parameter
     * @since 1.16.0
     */
    public static Parameter create(@Nonnull String name, @Nullable TgRemoteClob value, IceaxeLowParameterGenerateContext context) {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        context.closeableSet().add(value);
        var lowLobInfo = value.getLowLargeObjectInfo();
        return Parameters.clobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param path    path
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.8.0
     */
    public static Parameter createClob(@Nonnull String name, @Nullable Path path, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (path == null) {
            return Parameters.ofNull(name);
        }

        var lobFactory = context.session().getLobFactory();
        var remoteClob = lobFactory.uploadClob(path);
        context.closeableSet().add(remoteClob);

        var lowLobInfo = remoteClob.getLowLargeObjectInfo();
        return Parameters.clobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param reader  reader
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.16.0
     */
    public static Parameter createClob(@Nonnull String name, @Nullable Reader reader, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (reader == null) {
            return Parameters.ofNull(name);
        }

        var lobFactory = context.session().getLobFactory();
        var remoteClob = lobFactory.uploadClob(reader);
        context.closeableSet().add(remoteClob);

        var lowLobInfo = remoteClob.getLowLargeObjectInfo();
        return Parameters.clobOf(name, lowLobInfo);
    }

    /**
     * create parameter.
     *
     * @param name    parameter name
     * @param value   value
     * @param context context
     * @return parameter
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     * @since 1.16.0
     */
    public static Parameter createClob(@Nonnull String name, @Nullable String value, IceaxeLowParameterGenerateContext context) throws IOException, InterruptedException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        var lobFactory = context.session().getLobFactory();
        var remoteClob = lobFactory.uploadClob(value);
        context.closeableSet().add(remoteClob);

        var lowLobInfo = remoteClob.getLowLargeObjectInfo();
        return Parameters.clobOf(name, lowLobInfo);
    }
}
