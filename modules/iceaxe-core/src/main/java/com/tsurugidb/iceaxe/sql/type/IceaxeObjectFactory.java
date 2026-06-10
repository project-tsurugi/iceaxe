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
import java.text.MessageFormat;
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
    private TgLobPersistenceType defaultPersistenceType = TgLobPersistenceType.FILE;
    private Path tempDirectory;
    private boolean deleteTempFileOnExit = false;

    /**
     * set default persistence type.
     *
     * @param persistenceType persistence type
     * @since 1.16.0
     */
    public void setDefaultPersistenceType(@Nonnull TgLobPersistenceType persistenceType) {
        this.defaultPersistenceType = Objects.requireNonNull(persistenceType);
    }

    /**
     * get default persistence type.
     *
     * @return persistence type
     * @since 1.16.0
     */
    public TgLobPersistenceType getDefaultPersistenceType() {
        return this.defaultPersistenceType;
    }

    /**
     * get persistence type.
     *
     * @param persistenceType persistence type
     * @return persistence type
     * @since 1.16.0
     */
    protected TgLobPersistenceType getPersistenceType(TgLobPersistenceType persistenceType) {
        if (persistenceType != null) {
            return persistenceType;
        }
        return this.defaultPersistenceType;
    }

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
        var path = getTempDirectory().resolve(s);
        if (this.deleteTempFileOnExit) {
            path.toFile().deleteOnExit();
        }
        return path;
    }

    /**
     * set whether delete temporary file on exit.
     *
     * @param delete {@code true}: delete temporary file on exit
     * @since 1.16.0
     */
    public void setDeleteTempFileOnExit(boolean delete) {
        this.deleteTempFileOnExit = delete;
    }

    /**
     * whether delete temporary file on exit.
     *
     * @return {@code true}: delete temporary file on exit
     * @since 1.16.0
     */
    public boolean isDeleteTempFileOnExit() {
        return this.deleteTempFileOnExit;
    }

    /**
     * creates a new TgBlob instance.
     *
     * @param path path
     * @return TgBlob instance
     * @deprecated use {@link #createBlob(Path, TgLobPersistenceType)} instead
     */
    @Deprecated(since = "1.16.0", forRemoval = true)
    public TgBlob createBlob(Path path) {
        return new TgBlobPath(path);
    }

    /**
     * creates a new TgBlob instance.
     *
     * @param path            path
     * @param persistenceType persistence type
     * @return TgBlob instance
     * @throws IOException if an I/O error occurs when reading the file
     * @since 1.16.0
     */
    public TgBlob createBlob(Path path, TgLobPersistenceType persistenceType) throws IOException {
        var type = getPersistenceType(persistenceType);
        switch (type) {
        case FILE:
            return new TgBlobPath(path);
        case MEMORY:
            byte[] value = Files.readAllBytes(path);
            return new TgBlobBytes(value);
        default:
            throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
        }
    }

    /**
     * creates a new TgBlob instance.
     *
     * @param is                      input stream
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgBlob instance
     * @throws IOException if an I/O error occurs when reading or writing
     * @deprecated use {@link #createBlob(InputStream, TgLobPersistenceType)} or {@link TsurugiLobFactory#uploadBlob(InputStream)} instead
     */
    @Deprecated(since = "1.16.0", forRemoval = true)
    public TgBlob createBlob(InputStream is, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFilePath();
        IceaxeFileUtil.write(file, is);
        return new TgBlobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgBlob instance.
     *
     * <p>
     * If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgBlob is closed.
     * </p>
     *
     * @param is              input stream
     * @param persistenceType persistence type
     * @return TgBlob instance
     * @throws IOException if an I/O error occurs when reading or writing
     * @since 1.16.0
     */
    public TgBlob createBlob(InputStream is, TgLobPersistenceType persistenceType) throws IOException {
        var type = getPersistenceType(persistenceType);
        switch (type) {
        case FILE:
            var file = createTempFilePath();
            IceaxeFileUtil.write(file, is);
            return new TgBlobTempFile(file, false);
        case MEMORY:
            byte[] value = IceaxeFileUtil.readAllBytes(is);
            return new TgBlobBytes(value);
        default:
            throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
        }
    }

    /**
     * creates a new TgBlob instance.
     *
     * @param value                   value
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgBlob instance
     * @throws IOException if an I/O error occurs writing to the file
     * @deprecated use {@link #createBlob(byte[], TgLobPersistenceType)} or {@link TsurugiLobFactory#uploadBlob(byte[])} instead
     */
    @Deprecated(since = "1.16.0", forRemoval = true)
    public TgBlob createBlob(byte[] value, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFilePath();
        Files.write(file, value);
        return new TgBlobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgBlob instance.
     *
     * <p>
     * If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgBlob is closed.
     * </p>
     *
     * @param value           value
     * @param persistenceType persistence type
     * @return TgBlob instance
     * @throws IOException if an I/O error occurs writing to the file
     * @since 1.16.0
     */
    public TgBlob createBlob(byte[] value, TgLobPersistenceType persistenceType) throws IOException {
        var type = getPersistenceType(persistenceType);
        switch (type) {
        case FILE:
            var file = createTempFilePath();
            Files.write(file, value);
            return new TgBlobTempFile(file, false);
        case MEMORY:
            return new TgBlobBytes(value);
        default:
            throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
        }
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
        return createBlob(value, null);
    }

    /**
     * creates a new TgBlob instance from TgBlobReference.
     *
     * <p>
     * If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgBlob is closed.
     * </p>
     *
     * @param value           TgBlobReference
     * @param persistenceType persistence type
     * @return TgBlob instance
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     * @since 1.16.0
     */
    public TgBlob createBlob(TgBlobReference value, TgLobPersistenceType persistenceType) throws IOException, InterruptedException, TsurugiTransactionException {
        try (value) {
            var type = getPersistenceType(persistenceType);
            switch (type) {
            case FILE:
                var file = createTempFilePath();
                value.copyTo(file);
                return new TgBlobTempFile(file, false);
            case MEMORY:
                byte[] bytes = value.readAllBytes();
                return new TgBlobBytes(bytes);
            default:
                throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
            }
        }
    }

    /**
     * creates a new TgClob instance.
     *
     * @param path path
     * @return TgClob instance
     * @deprecated use {@link #createClob(Path, TgLobPersistenceType)} instead
     */
    @Deprecated(since = "1.16.0", forRemoval = true)
    public TgClob createClob(Path path) {
        return new TgClobPath(path);
    }

    /**
     * creates a new TgClob instance.
     *
     * @param path            path
     * @param persistenceType persistence type
     * @return TgClob instance
     * @throws IOException if an I/O error occurs when reading the file
     * @since 1.16.0
     */
    public TgClob createClob(Path path, TgLobPersistenceType persistenceType) throws IOException {
        var type = getPersistenceType(persistenceType);
        switch (type) {
        case FILE:
            return new TgClobPath(path);
        case MEMORY:
            String value = IceaxeFileUtil.readString(path);
            return new TgClobString(value);
        default:
            throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
        }
    }

    /**
     * creates a new TgClob instance.
     *
     * @param reader                  reader
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgClob instance
     * @throws IOException if an I/O error occurs when reading or writing
     * @deprecated use {@link #createClob(Reader, TgLobPersistenceType)} or {@link TsurugiLobFactory#uploadClob(Reader)} instead
     */
    @Deprecated(since = "1.16.0", forRemoval = true)
    public TgClob createClob(Reader reader, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFilePath();
        IceaxeFileUtil.write(file, reader);
        return new TgClobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgClob instance.
     *
     * <p>
     * If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgClob is closed.
     * </p>
     *
     * @param reader          reader
     * @param persistenceType persistence type
     * @return TgClob instance
     * @throws IOException if an I/O error occurs when reading or writing
     * @since 1.16.0
     */
    public TgClob createClob(Reader reader, TgLobPersistenceType persistenceType) throws IOException {
        var type = getPersistenceType(persistenceType);
        switch (type) {
        case FILE:
            var file = createTempFilePath();
            IceaxeFileUtil.write(file, reader);
            return new TgClobTempFile(file, false);
        case MEMORY:
            String value = IceaxeFileUtil.readString(reader);
            return new TgClobString(value);
        default:
            throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
        }
    }

    /**
     * creates a new TgClob instance.
     *
     * @param value                   value
     * @param deleteOnExecuteFinished delete on execute finished
     * @return TgClob instance
     * @throws IOException if an I/O error occurs writing to the file
     * @deprecated use {@link #createClob(String, TgLobPersistenceType)} or {@link TsurugiLobFactory#uploadClob(String)} instead
     */
    @Deprecated(since = "1.16.0", forRemoval = true)
    public TgClob createClob(String value, boolean deleteOnExecuteFinished) throws IOException {
        var file = createTempFilePath();
        IceaxeFileUtil.write(file, value);
        return new TgClobTempFile(file, deleteOnExecuteFinished);
    }

    /**
     * creates a new TgClob instance.
     *
     * <p>
     * If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgClob is closed.
     * </p>
     *
     * @param value           value
     * @param persistenceType persistence type
     * @return TgClob instance
     * @throws IOException if an I/O error occurs writing to the file
     * @since 1.16.0
     */
    public TgClob createClob(String value, TgLobPersistenceType persistenceType) throws IOException {
        var type = getPersistenceType(persistenceType);
        switch (type) {
        case FILE:
            var file = createTempFilePath();
            IceaxeFileUtil.write(file, value);
            return new TgClobTempFile(file, false);
        case MEMORY:
            return new TgClobString(value);
        default:
            throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
        }
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
        return createClob(value, null);
    }

    /**
     * creates a new TgClob instance from TgClobReference.
     *
     * <p>
     * If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgClob is closed.
     * </p>
     *
     * @param value           TgClobReference
     * @param persistenceType persistence type
     * @return TgClob instance
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     * @since 1.16.0
     */
    public TgClob createClob(TgClobReference value, TgLobPersistenceType persistenceType) throws IOException, InterruptedException, TsurugiTransactionException {
        try (value) {
            var type = getPersistenceType(persistenceType);
            switch (type) {
            case FILE:
                var file = createTempFilePath();
                value.copyTo(file);
                return new TgClobTempFile(file, false);
            case MEMORY:
                String s = value.readString();
                return new TgClobString(s);
            default:
                throw new IllegalArgumentException(MessageFormat.format("Unsupported persistence type: {0}", type));
            }
        }
    }
}
