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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * BLOB holding byte[].
 *
 * @since 1.16.0
 */
public class TgBlobBytes implements TgBlob {

    private final byte[] value;

    /**
     * Creates a new instance.
     *
     * @param value value
     */
    protected TgBlobBytes(byte[] value) {
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public Path getPath() {
        return null;
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
        return new ByteArrayInputStream(value);
    }

    @Override
    public void copyTo(Path destination) throws IOException {
        Files.write(destination, value);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return this.value;
    }

    @Override
    public TgRemoteBlob upload(TsurugiSession session, TgTimeValue timeout) throws IOException, InterruptedException {
        var lobFactory = session.getLobFactory();
        return lobFactory.uploadBlob(value, timeout);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + Arrays.toString(value) + ")";
    }
}
