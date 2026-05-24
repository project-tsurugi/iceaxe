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
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Objects;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeFileUtil;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * CLOB holding String.
 *
 * @since 1.16.0
 */
public class TgClobString implements TgClob {

    private final String value;

    /**
     * Creates a new instance.
     *
     * @param value value
     */
    protected TgClobString(String value) {
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
    public Reader openReader() throws IOException {
        return new StringReader(value);
    }

    @Override
    public void copyTo(Path destination) throws IOException {
        IceaxeFileUtil.write(destination, value);
    }

    @Override
    public String readString() throws IOException {
        return this.value;
    }

    @Override
    public TgRemoteClob upload(TsurugiSession session, TgTimeValue timeout) throws IOException, InterruptedException {
        var lobFactory = session.getLobFactory();
        return lobFactory.uploadClob(value, timeout);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + value + ")";
    }
}
