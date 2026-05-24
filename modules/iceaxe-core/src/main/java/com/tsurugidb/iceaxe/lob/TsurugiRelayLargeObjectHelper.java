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
package com.tsurugidb.iceaxe.lob;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;

/**
 * Tsurugi large object helper for blob-relay-service.
 *
 * @since 1.16.0
 */
public class TsurugiRelayLargeObjectHelper extends TsurugiLargeObjectHelper {

    @Override
    public TgRemoteBlob uploadBlob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (path == null) {
            return null;
        }

        var lowLargeObjectClient = getLowLargeObjectClient(session);
        var future = lowLargeObjectClient.upload(path);
        return uploadBlob(future, timeout);
    }

    @Override
    public TgRemoteBlob uploadBlob(TsurugiSession session, InputStream is, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (is == null) {
            return null;
        }

        try (is) {
            var lowLargeObjectClient = getLowLargeObjectClient(session);
            var future = lowLargeObjectClient.upload(is);
            return uploadBlob(future, timeout);
        }
    }

    @Override
    public TgRemoteBlob uploadBlob(TsurugiSession session, byte[] value, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var lowLargeObjectClient = getLowLargeObjectClient(session);
        var future = lowLargeObjectClient.upload(new ByteArrayInputStream(value));
        return uploadBlob(future, timeout);
    }

    @Override
    public TgRemoteClob uploadClob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (path == null) {
            return null;
        }

        var lowLargeObjectClient = getLowLargeObjectClient(session);
        var future = lowLargeObjectClient.upload(path);
        return uploadClob(future, timeout);
    }

    @Override
    public TgRemoteClob uploadClob(TsurugiSession session, Reader reader, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (reader == null) {
            return null;
        }

        try (reader) {
            var lowLargeObjectClient = getLowLargeObjectClient(session);
            var future = lowLargeObjectClient.upload(reader);
            return uploadClob(future, timeout);
        }
    }

    @Override
    public TgRemoteClob uploadClob(TsurugiSession session, String value, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var lowLargeObjectClient = getLowLargeObjectClient(session);
        var future = lowLargeObjectClient.upload(new StringReader(value));
        return uploadClob(future, timeout);
    }
}
