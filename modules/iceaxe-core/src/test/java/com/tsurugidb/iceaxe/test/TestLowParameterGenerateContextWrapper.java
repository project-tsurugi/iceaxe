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
package com.tsurugidb.iceaxe.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;

import com.tsurugidb.iceaxe.lob.TsurugiLargeObjectHelper;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.IceaxeLowParameterGenerateContext;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlobInfo;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClobInfo;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.tsubakuro.common.BlobRelayReference;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;

public class TestLowParameterGenerateContextWrapper {

    private IceaxeLowParameterGenerateContext context;
    private LargeObjectInfo lowLobInfo;

    public IceaxeLowParameterGenerateContext context() {
        var sessionOption = TgSessionOption.of();
        var session = new TsurugiSession(null, sessionOption) {
            @Override
            public TsurugiLargeObjectHelper getLargeObjectHelper() throws IOException, InterruptedException {
                return new TsurugiLargeObjectHelper() {
                    @Override
                    public TgRemoteBlob uploadBlob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
                        return new TgRemoteBlobInfo(createLowLargeObjectInfo(path));
                    }

                    @Override
                    public TgRemoteBlob uploadBlob(TsurugiSession session, InputStream is, IceaxeTimeout timeout) throws IOException, InterruptedException {
                        return new TgRemoteBlobInfo(createLowLargeObjectInfo(is));
                    }

                    @Override
                    public TgRemoteBlob uploadBlob(TsurugiSession session, byte[] value, IceaxeTimeout timeout) throws IOException, InterruptedException {
                        return new TgRemoteBlobInfo(createLowLargeObjectInfo(value));
                    }

                    @Override
                    public TgRemoteClob uploadClob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
                        return new TgRemoteClobInfo(createLowLargeObjectInfo(path));
                    }

                    @Override
                    public TgRemoteClob uploadClob(TsurugiSession session, Reader reader, IceaxeTimeout timeout) throws IOException, InterruptedException {
                        return new TgRemoteClobInfo(createLowLargeObjectInfo(reader));
                    }

                    @Override
                    public TgRemoteClob uploadClob(TsurugiSession session, String value, IceaxeTimeout timeout) throws IOException, InterruptedException {
                        return new TgRemoteClobInfo(createLowLargeObjectInfo(value));
                    }
                };
            }
        };

        var convertUtil = new IceaxeConvertUtil();
        var closeableSet = new IceaxeCloseableSet();
        this.context = new IceaxeLowParameterGenerateContext(session, convertUtil, closeableSet);

        return this.context;
    }

    public LargeObjectInfo createLowLargeObjectInfo(Path path) {
        if (this.lowLobInfo == null) {
            String serverPath = path.toString();
            this.lowLobInfo = new LargeObjectInfo() {
                @Override
                public InfoType getInfoType() {
                    return InfoType.SERVER_PATH;
                }

                @Override
                public String getServerPath() {
                    return serverPath;
                }

                @Override
                public String toString() {
                    return "LargeObjectInfo(" + serverPath + ")";
                }
            };
        }
        return this.lowLobInfo;
    }

    public LargeObjectInfo createLowLargeObjectInfo(Object value) {
        if (this.lowLobInfo == null) {
            this.lowLobInfo = new LargeObjectInfo() {
                @Override
                public InfoType getInfoType() {
                    return InfoType.BLOB_RELAY_REFERENCE;
                }

                @Override
                public BlobRelayReference getBlobRelayReference() {
                    return new BlobRelayReference(1, value.hashCode(), 123);
                }

                @Override
                public String toString() {
                    return "LargeObjectInfo(BlobRelayTestMock)";
                }
            };
        }
        return this.lowLobInfo;
    }

    public LargeObjectInfo lowLargeObjectInfo() {
        return this.lowLobInfo;
    }
}
