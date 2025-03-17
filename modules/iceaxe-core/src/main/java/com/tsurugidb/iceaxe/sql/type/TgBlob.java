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
package com.tsurugidb.iceaxe.sql.type;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;

/**
 * BLOB.
 *
 * @since 1.8.0
 */
public interface TgBlob extends IceaxeTimeoutCloseable {

    /**
     * Creates a new instance.
     *
     * @param path path
     * @return instance
     */
    public static TgBlob of(Path path) {
        return new TgBlobPath(path);
    }

    /**
     * get path.
     *
     * @return path
     */
    public Path getPath();

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
     */
    public boolean isDeleteOnExecuteFinished();

    /**
     * Returns an input stream.
     *
     * @return input stream
     * @throws IOException if an I/O error occurs
     */
    public InputStream openInputStream() throws IOException;

    /**
     * Copy the large object to the file indicated by the given path.
     *
     * @param destination the path of the destination file
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(Path destination) throws IOException;

    /**
     * Reads all bytes.
     *
     * @return value
     * @throws IOException if an I/O error occurs
     */
    public byte[] readAllBytes() throws IOException;

    @Override
    public abstract void close() throws IOException;

    @Override
    public default void close(long timeoutNanos) throws IOException {
        close();
    }
}
