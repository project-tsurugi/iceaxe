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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgLobTransferType;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlobInfo;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClobInfo;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;

class TsurugiLargeObjectHelperTest {

    // A test to verify that unused TgRemoteBlob and TgRemoteClob objects are closed when the TsurugiSession is closed.
    @Test
    void close() throws Exception {
        var remoteBlob = new TgRemoteBlobInfo(null) {
            private boolean closed = false;

            @Override
            public void close(long timeoutNanos) throws IOException {
                this.closed = true;
                super.close(timeoutNanos);
            }
        };
        var remoteClob = new TgRemoteClobInfo(null) {
            private boolean closed = false;

            @Override
            public void close(long timeoutNanos) throws IOException {
                this.closed = true;
                super.close(timeoutNanos);
            }
        };

        var helper = new TsurugiLargeObjectHelper() {
            @Override
            public TgRemoteBlob uploadBlob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
                addChild(remoteBlob);
                return remoteBlob;
            }

            @Override
            public TgRemoteBlob uploadBlob(TsurugiSession session, InputStream is, IceaxeTimeout timeout) throws IOException, InterruptedException {
                addChild(remoteBlob);
                return remoteBlob;
            }

            @Override
            public TgRemoteBlob uploadBlob(TsurugiSession session, byte[] value, IceaxeTimeout timeout) throws IOException, InterruptedException {
                addChild(remoteBlob);
                return remoteBlob;
            }

            @Override
            public TgRemoteClob uploadClob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
                addChild(remoteClob);
                return remoteClob;
            }

            @Override
            public TgRemoteClob uploadClob(TsurugiSession session, Reader reader, IceaxeTimeout timeout) throws IOException, InterruptedException {
                addChild(remoteClob);
                return remoteClob;
            }

            @Override
            public TgRemoteClob uploadClob(TsurugiSession session, String value, IceaxeTimeout timeout) throws IOException, InterruptedException {
                addChild(remoteClob);
                return remoteClob;
            }
        };

        var sessionOption = TgSessionOption.of();
        try (var session = new TestTsurugiSession(sessionOption) {
            @Override
            public TgLobTransferType getLobTransferType() throws IOException, InterruptedException {
                return TgLobTransferType.RELAY;
            }
        }) {
            session.setLargeObjectHelperFactory(new TsurugiLargeObjectHelperFactory() {
                @Override
                public TsurugiLargeObjectHelper createHelper(TgLobTransferType lobTransferType) {
                    return helper;
                }
            });
            var lobFactory = session.getLobFactory();

            var blob = lobFactory.uploadBlob(Path.of("dummy"));
            assertSame(remoteBlob, blob);
            assertFalse(remoteBlob.closed);

            var clob = lobFactory.uploadClob(Path.of("dummy"));
            assertSame(remoteClob, clob);
            assertFalse(remoteClob.closed);
        }

        assertTrue(remoteBlob.closed);
        assertTrue(remoteClob.closed);
    }
}
