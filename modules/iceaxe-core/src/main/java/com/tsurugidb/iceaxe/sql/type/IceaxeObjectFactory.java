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
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeFileUtil;

/**
 * object factory.
 *
 * @since 1.8.0
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

    private final long pid = ProcessHandle.current().pid();
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
     * create temporary file path.
     *
     * @return file
     */
    public Path createTempFilePath() {
        String s = "iceaxe-object-temp-" + pid //
                + "-" + Thread.currentThread().getId() //
                + "-" + System.currentTimeMillis() //
                + "-" + System.nanoTime() //
                + ".dat";
        return getTempDirectory().resolve(s);
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
        var file = createTempFilePath();
        Files.copy(is, file);
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
        var file = createTempFilePath();
        Files.write(file, value);
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
            var file = createTempFilePath();
            value.copyTo(file);
            return new TgBlobTempFile(file, false);
        }
    }

    /**
     * creates a new TgClob instance.
     *
     * @param path path
     * @return TgClob instance
     */
    public TgClob createClob(Path path) {
        return new TgClobPath(path);
    }

    private static final int READ_BUFFER_SIZE = 4 * 1024;

    /**
     * creates a new TgClob instance.
     *
     * @param reader                  reader
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgClob instance
     * @throws IOException if an I/O error occurs when reading or writing
     */
    public TgClob createClob(Reader reader, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFilePath();
        try (var writer = Files.newBufferedWriter(file)) {
            var buffer = new char[READ_BUFFER_SIZE];
            for (;;) {
                int len = reader.read(buffer);
                if (len < 0) {
                    break;
                }
                writer.write(buffer, 0, len);
            }
        }
        return new TgClobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgClob instance.
     *
     * @param value                   value
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgClob instance
     * @throws IOException if an I/O error occurs writing to the file
     */
    public TgClob createClob(String value, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFilePath();
        IceaxeFileUtil.writeString(file, value);
        return new TgClobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgClob instance from TgClobReference.
     *
     * @param value TgClobReference
     * @return TgClob instance
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public TgClob createClob(TgClobReference value) throws IOException, InterruptedException, TsurugiTransactionException {
        try (value) {
            var file = createTempFilePath();
            value.copyTo(file);
            return new TgClobTempFile(file, false);
        }
    }
}
