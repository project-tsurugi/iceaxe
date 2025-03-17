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
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.LargeObjectCache;
import com.tsurugidb.tsubakuro.sql.Transaction;

/**
 * BLOB used in query result.
 *
 * @since 1.8.0
 */
public class TgBlobReference implements IceaxeTimeoutCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TgBlobReference.class);

    /**
     * Creates a new instance.
     *
     * @param transaction  transaction
     * @param lowReference low reference
     * @return instance
     * @throws IOException if transaction already closed
     */
    @IceaxeInternal
    public static TgBlobReference of(@Nonnull TsurugiTransaction transaction, @Nonnull BlobReference lowReference) throws IOException {
        var blob = new TgBlobReference(transaction, lowReference);
        blob.initialize();
        return blob;
    }

    private final TsurugiTransaction ownerTransaction;
    private final BlobReference lowReference;
    private final IceaxeTimeout timeout;

    private LargeObjectCache lowLargeObjectCache;

    /**
     * Creates a new instance.
     *
     * @param transaction  transaction
     * @param lowReference low reference
     */
    @IceaxeInternal
    protected TgBlobReference(@Nonnull TsurugiTransaction transaction, @Nonnull BlobReference lowReference) {
        this.ownerTransaction = Objects.requireNonNull(transaction);
        this.lowReference = Objects.requireNonNull(lowReference);
        var sessionOption = transaction.getSessionOption();
        this.timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.BLOB_GET);
    }

    /**
     * initialize.
     * <p>
     * Call this method only once after construct.
     * </p>
     *
     * @throws IOException if transaction already closed
     */
    @IceaxeInternal
    protected void initialize() throws IOException {
        ownerTransaction.addChild(this);
    }

    /**
     * get low transaction.
     *
     * @return low SQL client
     * @throws IOException          if an I/O error occurs while communicating to the server
     * @throws InterruptedException if interrupted while communicating to the server
     */
    @IceaxeInternal
    protected Transaction getLowTransaction() throws IOException, InterruptedException {
        return ownerTransaction.getLowTransaction();
    }

    /**
     * Returns an input stream.
     *
     * @return input stream
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public InputStream openInputStream() throws IOException, InterruptedException, TsurugiTransactionException {
        LOG.trace("SqlClient.openInputStream start");
        var future = getLowTransaction().openInputStream(lowReference);
        LOG.trace("SqlClient.openInputStream started");
        var is = IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.BLOB_GET_TIMEOUT, IceaxeErrorCode.BLOB_CLOSE_TIMEOUT);
        LOG.trace("SqlClient.openInputStream end");
        return is;
    }

    /**
     * Copy the large object to the file indicated by the given path.
     *
     * @param destination the path of the destination file
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public void copyTo(Path destination) throws IOException, InterruptedException, TsurugiTransactionException {
        LOG.trace("SqlClient.copyTo start");
        var future = getLowTransaction().copyTo(lowReference, destination);
        LOG.trace("SqlClient.copyTo started");
        IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.BLOB_GET_TIMEOUT, IceaxeErrorCode.BLOB_CLOSE_TIMEOUT);
        LOG.trace("SqlClient.copyTo end");
    }

    /**
     * Copy the large object to the file indicated by the given path.
     *
     * @param destination the path of the destination file
     * @param useCache    {@code true}: use large object cache if exists. {@code false}: same as {@link #copyTo(Path)}.
     * @return {@code true} if large object cache is used
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public boolean copyTo(Path destination, boolean useCache) throws IOException, InterruptedException, TsurugiTransactionException {
        if (useCache) {
            var lowCache = getLowLargeObjectCache();
            var pathOpt = lowCache.find();
            if (pathOpt.isPresent()) {
                var path = pathOpt.get();
                Files.copy(path, destination);
                return true;
            }
        }

        copyTo(destination);
        return false;
    }

    /**
     * Reads all bytes.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public byte[] readAllBytes() throws IOException, InterruptedException, TsurugiTransactionException {
        try (var is = openInputStream()) {
            return is.readAllBytes();
        }
    }

    /**
     * Reads all bytes.
     *
     * @return value
     * @param useCache {@code true}: use large object cache if exists. {@code false}: same as {@link #readAllBytes()}.
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public byte[] readAllBytes(boolean useCache) throws IOException, InterruptedException, TsurugiTransactionException {
        if (useCache) {
            var lowCache = getLowLargeObjectCache();
            var pathOpt = lowCache.find();
            if (pathOpt.isPresent()) {
                var path = pathOpt.get();
                return Files.readAllBytes(path);
            }
        }

        return readAllBytes();
    }

    /**
     * get low large object cache.
     *
     * @return low large object cache
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    protected LargeObjectCache getLowLargeObjectCache() throws IOException, InterruptedException, TsurugiTransactionException {
        var lowCache = this.lowLargeObjectCache;
        if (lowCache == null) {
            LOG.trace("SqlClient.getLargeObjectCache start");
            var future = getLowTransaction().getLargeObjectCache(lowReference);
            LOG.trace("SqlClient.getLargeObjectCache started");
            var timeout = new IceaxeTimeout(ownerTransaction.getSessionOption(), TgTimeoutKey.BLOB_CACHE_GET);
            lowCache = IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.BLOB_CACHE_GET_TIMEOUT, IceaxeErrorCode.BLOB_CACHE_CLOSE_TIMEOUT);
            LOG.trace("SqlClient.getLargeObjectCache end");
            this.lowLargeObjectCache = lowCache;
        }
        return lowCache;
    }

    /**
     * convert to TgBlob.
     *
     * @param objectFactory object factory
     * @return TgBlob instance
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public TgBlob toBlob(IceaxeObjectFactory objectFactory) throws IOException, InterruptedException, TsurugiTransactionException {
        return objectFactory.createBlob(this);
    }

    @Override
    public void close() throws IOException, InterruptedException, TsurugiTransactionException {
        var timeout = new IceaxeTimeout(ownerTransaction.getSessionOption(), TgTimeoutKey.BLOB_CLOSE);
        close(timeout.getNanos());
    }

    @Override
    public void close(long timeoutNanos) throws IOException, InterruptedException, TsurugiTransactionException {
        ownerTransaction.removeChild(this);

        IceaxeIoUtil.closeInTransaction(timeoutNanos, IceaxeErrorCode.BLOB_CLOSE_TIMEOUT, IceaxeErrorCode.BLOB_CLOSE_ERROR, lowLargeObjectCache);
    }
}
