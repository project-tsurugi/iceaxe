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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlobTempFile;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClobTempFile;
import com.tsurugidb.iceaxe.util.IceaxeFileUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;

/**
 * Tsurugi large object helper for privileged mode.
 *
 * @since 1.16.0
 */
public class TsurugiPrivilegedLargeObjectHelper extends TsurugiLargeObjectHelper {

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

        var file = createTempFilePath(session);
        IceaxeFileUtil.write(file, is);
        return uploadBlobTempFile(session, file, timeout);
    }

    @Override
    public TgRemoteBlob uploadBlob(TsurugiSession session, byte[] value, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var file = createTempFilePath(session);
        Files.write(file, value);
        return uploadBlobTempFile(session, file, timeout);
    }

    /**
     * Creates a temporary file path.
     *
     * @param session Tsurugi session
     * @return temporary file path
     */
    protected Path createTempFilePath(TsurugiSession session) {
        var objectFactory = session.getLobFactory().getIceaxeObjectFactory();
        return objectFactory.createTempFilePath();
    }

    /**
     * Upload BLOB using temporary file.
     *
     * @param session Tsurugi session
     * @param path    temporary file path
     * @param timeout timeout
     * @return Uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for upload result
     */
    protected TgRemoteBlob uploadBlobTempFile(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
        var lowLargeObjectClient = getLowLargeObjectClient(session);
        var future = lowLargeObjectClient.upload(path);
        var lowLargeObjectInfo = getLowLargeObjectInfo(future, timeout, IceaxeErrorCode.BLOB_UPLOAD_TIMEOUT, IceaxeErrorCode.BLOB_CLOSE_TIMEOUT);
        var blob = new TgRemoteBlobTempFile(lowLargeObjectInfo, path);
        addChild(blob);
        return blob;
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

        var file = createTempFilePath(session);
        IceaxeFileUtil.write(file, reader);
        return uploadClobTempFile(session, file, timeout);
    }

    @Override
    public TgRemoteClob uploadClob(TsurugiSession session, String value, IceaxeTimeout timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var file = createTempFilePath(session);
        IceaxeFileUtil.write(file, value);
        return uploadClobTempFile(session, file, timeout);
    }

    /**
     * Upload CLOB using temporary file.
     *
     * @param session Tsurugi session
     * @param path    temporary file path
     * @param timeout timeout
     * @return Uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for upload result
     */
    protected TgRemoteClob uploadClobTempFile(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException {
        var lowLargeObjectClient = getLowLargeObjectClient(session);
        var future = lowLargeObjectClient.upload(path);
        var lowLargeObjectInfo = getLowLargeObjectInfo(future, timeout, IceaxeErrorCode.CLOB_UPLOAD_TIMEOUT, IceaxeErrorCode.CLOB_CLOSE_TIMEOUT);
        var clob = new TgRemoteClobTempFile(lowLargeObjectInfo, path);
        addChild(clob);
        return clob;
    }
}
