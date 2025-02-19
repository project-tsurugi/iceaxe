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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * object factory.
 *
 * @since X.X.X
 */
public class IceaxeObjectFactory {

    private static IceaxeObjectFactory defaultInstance = new IceaxeObjectFactory();

    /**
     * get default factory.
     *
     * @return object factory
     */
    public static IceaxeObjectFactory getDefaultInstance() {
        return defaultInstance;
    }

    /**
     * set default factory.
     *
     * @param factory object factory
     */
    public static void setDefaultInstance(@Nonnull IceaxeObjectFactory factory) {
        defaultInstance = Objects.requireNonNull(factory);
    }

    private Path tempDirectory;

    /**
     * set temporary directory.
     *
     * @param dir directory
     */
    public void setTempDirectory(Path dir) {
        this.tempDirectory = dir;
    }

    /**
     * get temporary directory.
     *
     * @return directory
     */
    public @Nonnull Path getTempDirectory() {
        var dir = this.tempDirectory;
        if (dir == null) {
            dir = Path.of(System.getProperty("java.io.tmpdir"));
            setTempDirectory(dir);
        }
        return dir;
    }

    /**
     * create temporary file.
     *
     * @return file
     * @throws IOException if an I/O error occurs
     */
    public Path createTempFile() throws IOException {
        return Files.createTempFile(getTempDirectory(), "iceaxe-object-temp-", ".dat");
    }

    /**
     * creates a new TgBlob instance.
     *
     * @param path path
     * @return TgBlob instance
     */
    public TgBlob createBlob(Path path) {
        return new TgBlobPath(path);
    }

    /**
     * creates a new TgBlob instance.
     *
     * @param is                      input stream
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgBlob instance
     * @throws IOException if an I/O error occurs when reading or writing
     */
    public TgBlob createBlob(InputStream is, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFile();
        Files.copy(is, file, StandardCopyOption.REPLACE_EXISTING);
        return new TgBlobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgBlob instance.
     *
     * @param value                   value
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgBlob instance
     * @throws IOException if an I/O error occurs writing to the file
     */
    public TgBlob createBlob(byte[] value, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFile();
        Files.write(file, value, StandardOpenOption.TRUNCATE_EXISTING);
        return new TgBlobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgBlob instance from TgBlobReference.
     *
     * @param value TgBlobReference
     * @return TgBlob instance
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public TgBlob createBlob(TgBlobReference value) throws IOException, InterruptedException, TsurugiTransactionException {
        try (value) {
            var file = createTempFile();
            value.copyTo(file);
            return new TgBlobTempFile(file, false);
        }
    }
}
