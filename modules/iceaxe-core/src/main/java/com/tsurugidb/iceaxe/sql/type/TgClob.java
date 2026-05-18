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
package com.tsurugidb.iceaxe.sql.type;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * CLOB.
 *
 * @since 1.8.0
 */
public interface TgClob extends IceaxeTimeoutCloseable {

    /**
     * Creates a new instance.
     *
     * @param path path
     * @return instance
     */
    public static TgClob of(Path path) {
        return new TgClobPath(path);
    }

    /**
     * Creates a new instance.
     *
     * @param value value
     * @return instance
     * @since 1.16.0
     */
    public static TgClob of(String value) {
        return new TgClobString(value);
    }

    /**
     * get path.
     *
     * @return path
     */
    public @Nullable Path getPath();

    /**
     * Whether temporary file or not.
     *
     * @return {@code true} when temporary file
     */
    public boolean isTempFile();

    /**
     * Whether delete on execute finished.
     *
     * @return {@code true}: delete temporary file on execute finished
     * @deprecated For removal in a future release. (It will always be {@code false})
     */
    @Deprecated(since = "1.16.0", forRemoval = true)
    public boolean isDeleteOnExecuteFinished();

    /**
     * Returns a reader.
     *
     * @return reader
     * @throws IOException if an I/O error occurs
     */
    public Reader openReader() throws IOException;

    /**
     * Copy the large object to the file indicated by the given path.
     *
     * @param destination the path of the destination file
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(Path destination) throws IOException;

    /**
     * Reads string.
     *
     * @return value
     * @throws IOException if an I/O error occurs
     */
    public String readString() throws IOException;

    /**
     * Upload CLOB.
     *
     * @param session session
     * @param timeout timeout
     * @return uploaded CLOB
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    @IceaxeInternal
    public TgRemoteClob upload(TsurugiSession session, TgTimeValue timeout) throws IOException, InterruptedException;

    @Override
    public abstract void close() throws IOException;

    @Override
    public default void close(long timeoutNanos) throws IOException {
        close();
    }
}
