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

/**
 * BLOB holding path.
 *
 * @since X.X.X
 */
public class TgBlobPath implements TgBlob {

    private final Path path;

    /**
     * Creates a new instance.
     *
     * @param path path
     */
    protected TgBlobPath(Path path) {
        this.path = path;
    }

    @Override
    public Path getPath() {
        return this.path;
    }

    @Override
    public boolean isTempFile() {
        return false;
    }

    @Override
    public boolean isDeleteOnExecuteFinished() {
        return false;
    }

    @Override
    public InputStream openInputStream() throws IOException {
        return Files.newInputStream(path);
    }

    @Override
    public void copyTo(Path destination) throws IOException {
        Files.copy(path, destination);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return Files.readAllBytes(path);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + path + ")";
    }
}
