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
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * CLOB using temporary file.
 *
 * @since 1.8.0
 */
public class TgClobTempFile implements TgClob {

    private final Path path;
    private final boolean deleteOnExecuteFinished;

    /**
     * Creates a new instance.
     *
     * @param path                    temporary file path
     * @param deleteOnExecuteFinished delete temporary file on execute finished
     */
    protected TgClobTempFile(Path path, boolean deleteOnExecuteFinished) {
        this.path = path;
        this.deleteOnExecuteFinished = deleteOnExecuteFinished;
    }

    @Override
    public Path getPath() {
        return this.path;
    }

    @Override
    public boolean isTempFile() {
        return true;
    }

    @Override
    public boolean isDeleteOnExecuteFinished() {
        return this.deleteOnExecuteFinished;
    }

    @Override
    public Reader openReader() throws IOException {
        return Files.newBufferedReader(path);
    }

    @Override
    public void copyTo(Path destination) throws IOException {
        Files.copy(path, destination);
    }

    @Override
    public String readString() throws IOException {
        return Files.readString(path);
    }

    @Override
    public void close() throws IOException {
        Files.deleteIfExists(path);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + path + ", deleteOnExecuteFinished=" + deleteOnExecuteFinished + ")";
    }
}
