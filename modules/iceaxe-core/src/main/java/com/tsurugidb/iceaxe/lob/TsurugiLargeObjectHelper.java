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
import java.nio.file.Path;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlobInfo;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClobInfo;
import com.tsurugidb.iceaxe.sql.type.TgRemoteLobInfo;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi large object helper.
 *
 * @since 1.16.0
 */
public abstract class TsurugiLargeObjectHelper implements IceaxeTimeoutCloseable {

    private final IceaxeCloseableSet closeableSet = new IceaxeCloseableSet();

    /**
     * Uploads a BLOB.
     *
     * @param session Tsurugi session
     * @param path    Path of the file to be uploaded
     * @param timeout Timeout for the upload operation
     * @return uploaded BLOB
     * @throws IOException          If an I/O error occurs during the upload
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete
     */
    public abstract TgRemoteBlob uploadBlob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException;

    /**
     * Uploads a BLOB.
     *
     * @param session Tsurugi session
     * @param is      InputStream for the content to be uploaded
     * @param timeout Timeout for the upload operation
     * @return uploaded BLOB
     * @throws IOException          If an I/O error occurs during the upload
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete
     */
    public abstract TgRemoteBlob uploadBlob(TsurugiSession session, InputStream is, IceaxeTimeout timeout) throws IOException, InterruptedException;

    /**
     * Uploads a BLOB.
     *
     * @param session Tsurugi session
     * @param value   Byte array containing the content to be uploaded
     * @param timeout Timeout for the upload operation
     * @return uploaded BLOB
     * @throws IOException          If an I/O error occurs during the upload
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete
     */
    public abstract TgRemoteBlob uploadBlob(TsurugiSession session, byte[] value, IceaxeTimeout timeout) throws IOException, InterruptedException;

    /**
     * Uploads a CLOB.
     *
     * @param session Tsurugi session
     * @param path    Path of the file to be uploaded
     * @param timeout Timeout for the upload operation
     * @return uploaded CLOB
     * @throws IOException          If an I/O error occurs during the upload
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete
     */
    public abstract TgRemoteClob uploadClob(TsurugiSession session, Path path, IceaxeTimeout timeout) throws IOException, InterruptedException;

    /**
     * Uploads a CLOB.
     *
     * @param session Tsurugi session
     * @param reader  Reader for the content to be uploaded
     * @param timeout Timeout for the upload operation
     * @return uploaded CLOB
     * @throws IOException          If an I/O error occurs during the upload
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete
     */
    public abstract TgRemoteClob uploadClob(TsurugiSession session, Reader reader, IceaxeTimeout timeout) throws IOException, InterruptedException;

    /**
     * Uploads a CLOB.
     *
     * @param session Tsurugi session
     * @param value   String containing the content to be uploaded
     * @param timeout Timeout for the upload operation
     * @return uploaded CLOB
     * @throws IOException          If an I/O error occurs during the upload
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete
     */
    public abstract TgRemoteClob uploadClob(TsurugiSession session, String value, IceaxeTimeout timeout) throws IOException, InterruptedException;

    /**
     * Gets the low-level LargeObjectClient from the Tsurugi session.
     *
     * @param session Tsurugi session
     * @return Low-level LargeObjectClient
     * @throws IOException          If an I/O error occurs while retrieving the LargeObjectClient
     * @throws InterruptedException if interrupted while communicating to the server
     */
    @IceaxeInternal
    protected LargeObjectClient getLowLargeObjectClient(TsurugiSession session) throws IOException, InterruptedException {
        var lowSession = session.getLowSession();
        return lowSession.getLargeObjectClient();
    }

    /**
     * Waits for the upload operation to complete and retrieves the BLOB.
     *
     * @param future  Future response containing the LargeObjectInfo for the uploaded BLOB
     * @param timeout Timeout for waiting for the upload to complete
     * @return uploaded BLOB
     * @throws IOException          If an I/O error occurs while retrieving the LargeObjectInfo or closing the future response
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete or closing the future response
     */
    protected TgRemoteBlob uploadBlob(FutureResponse<LargeObjectInfo> future, IceaxeTimeout timeout) throws IOException, InterruptedException {
        var lowLargeObjectInfo = getLowLargeObjectInfo(future, timeout, IceaxeErrorCode.BLOB_UPLOAD_TIMEOUT, IceaxeErrorCode.BLOB_CLOSE_TIMEOUT);
        var blob = new TgRemoteBlobInfo(lowLargeObjectInfo);
        addChild(blob);
        return blob;
    }

    /**
     * Waits for the upload operation to complete and retrieves the CLOB.
     *
     * @param future  Future response containing the LargeObjectInfo for the uploaded CLOB
     * @param timeout Timeout for waiting for the upload to complete
     * @return uploaded CLOB
     * @throws IOException          If an I/O error occurs while retrieving the LargeObjectInfo or closing the future response
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete or closing the future response
     */
    protected TgRemoteClob uploadClob(FutureResponse<LargeObjectInfo> future, IceaxeTimeout timeout) throws IOException, InterruptedException {
        var lowLargeObjectInfo = getLowLargeObjectInfo(future, timeout, IceaxeErrorCode.CLOB_UPLOAD_TIMEOUT, IceaxeErrorCode.CLOB_CLOSE_TIMEOUT);
        var clob = new TgRemoteClobInfo(lowLargeObjectInfo);
        addChild(clob);
        return clob;
    }

    /**
     * Waits for the upload operation to complete and retrieves the LargeObjectInfo.
     *
     * @param future                Future response containing the LargeObjectInfo for the uploaded large object
     * @param timeout               Timeout for waiting for the upload to complete
     * @param timeoutErrorCode      Error code to use if the upload operation times out
     * @param closeTimeoutErrorCode Error code to use if closing the future response times out
     * @return LargeObjectInfo for the uploaded large object
     * @throws IOException          If an I/O error occurs while retrieving the LargeObjectInfo or closing the future response
     * @throws InterruptedException If the thread is interrupted while waiting for the upload to complete or closing the future response
     */
    @IceaxeInternal
    protected LargeObjectInfo getLowLargeObjectInfo(FutureResponse<LargeObjectInfo> future, IceaxeTimeout timeout, IceaxeErrorCode timeoutErrorCode, IceaxeErrorCode closeTimeoutErrorCode)
            throws IOException, InterruptedException {
        return IceaxeIoUtil.getAndCloseFuture(future, timeout, timeoutErrorCode, closeTimeoutErrorCode);
    }

    /**
     * Adds the given large object to the closeable set.
     *
     * @param lob Large object
     */
    protected void addChild(TgRemoteLobInfo lob) {
        lob.setOwner(this.closeableSet);
    }

    @Override
    public void close() throws Exception {
        close(0);
    }

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException {
        IceaxeIoUtil.close(timeoutNanos, closeableSet, IceaxeErrorCode.LOB_HELPER_CLOSE_ERROR, t -> {
        });
    }
}
