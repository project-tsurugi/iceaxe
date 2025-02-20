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

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.OverridingMethodsMustInvokeSuper;

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
import com.tsurugidb.tsubakuro.sql.ClobReference;
import com.tsurugidb.tsubakuro.sql.LargeObjectCache;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * CLOB used in query result.
 *
 * @since X.X.X
 */
public class TgClobReference implements IceaxeTimeoutCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TgClobReference.class);

    /**
     * Creates a new instance.
     *
     * @param transaction  transaction
     * @param lowReference low reference
     * @return instance
     * @throws IOException if transaction already closed
     */
    @IceaxeInternal
    public static TgClobReference of(@Nonnull TsurugiTransaction transaction, @Nonnull ClobReference lowReference) throws IOException {
        var clob = new TgClobReference(transaction, lowReference);
        clob.initialize();
        return clob;
    }

    private final TsurugiTransaction ownerTransaction;
    private final ClobReference lowReference;
    private final IceaxeTimeout timeout;

    private LargeObjectCache lowLargeObjectCache;

    /**
     * Creates a new instance.
     *
     * @param transaction  transaction
     * @param lowReference low reference
     */
    @IceaxeInternal
    protected TgClobReference(@Nonnull TsurugiTransaction transaction, @Nonnull ClobReference lowReference) {
        this.ownerTransaction = Objects.requireNonNull(transaction);
        this.lowReference = Objects.requireNonNull(lowReference);
        var sessionOption = transaction.getSessionOption();
        this.timeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.CLOB_GET);
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
     * get low SQL client.
     *
     * @return low SQL client
     * @throws IOException          if an I/O error occurs while communicating to the server
     * @throws InterruptedException if interrupted while communicating to the server
     */
    protected SqlClient getLowSqlClient() throws IOException, InterruptedException {
        return ownerTransaction.getSession().getLowSqlClient();
    }

    /**
     * Returns a reader.
     *
     * @return input stream
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public Reader openReader() throws IOException, InterruptedException, TsurugiTransactionException {
        LOG.trace("SqlClient.openReader start");
        var future = getLowSqlClient().openReader(lowReference);
        LOG.trace("SqlClient.openReader started");
        var is = IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.CLOB_GET_TIMEOUT, IceaxeErrorCode.CLOB_CLOSE_TIMEOUT);
        LOG.trace("SqlClient.openReader end");
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
        var future = getLowSqlClient().copyTo(lowReference, destination);
        LOG.trace("SqlClient.copyTo started");
        IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.CLOB_GET_TIMEOUT, IceaxeErrorCode.CLOB_CLOSE_TIMEOUT);
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

    private static final int STRING_BUFFER_INIT_SIZE = 4 * 1024;
    private static final int READ_BUFFER_SIZE = 4 * 1024;

    /**
     * Reads string.
     *
     * @return value
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public String readString() throws IOException, InterruptedException, TsurugiTransactionException {
        try (var writer = new CharArrayWriter(STRING_BUFFER_INIT_SIZE); //
                var reader = openReader()) {
            var buffer = new char[READ_BUFFER_SIZE];
            for (;;) {
                int len = reader.read(buffer);
                if (len < 0) {
                    break;
                }
                writer.write(buffer, 0, len);
            }
            return writer.toString();
        }
    }

    /**
     * Reads string.
     *
     * @return value
     * @param useCache {@code true}: use large object cache if exists. {@code false}: same as {@link #readString()}.
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public String readString(boolean useCache) throws IOException, InterruptedException, TsurugiTransactionException {
        if (useCache) {
            var lowCache = getLowLargeObjectCache();
            var pathOpt = lowCache.find();
            if (pathOpt.isPresent()) {
                var path = pathOpt.get();
                return Files.readString(path);
            }
        }

        return readString();
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
            var future = getLowSqlClient().getLargeObjectCache(lowReference);
            LOG.trace("SqlClient.getLargeObjectCache started");
            var timeout = new IceaxeTimeout(ownerTransaction.getSessionOption(), TgTimeoutKey.CLOB_CACHE_GET);
            lowCache = IceaxeIoUtil.getAndCloseFutureInTransaction(future, timeout, IceaxeErrorCode.CLOB_CACHE_GET_TIMEOUT, IceaxeErrorCode.CLOB_CACHE_CLOSE_TIMEOUT);
            LOG.trace("SqlClient.getLargeObjectCache end");
            this.lowLargeObjectCache = lowCache;
        }
        return lowCache;
    }

    /**
     * convert to TgClob.
     *
     * @param objectFactory object factory
     * @return TgClob instance
     * @throws IOException                 if an I/O error occurs
     * @throws InterruptedException        if interrupted while processing the request
     * @throws TsurugiTransactionException if server error occurs while processing the request
     */
    public TgClob toClob(IceaxeObjectFactory objectFactory) throws IOException, InterruptedException, TsurugiTransactionException {
        return objectFactory.createClob(this);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws IOException {
        ownerTransaction.removeChild(this);

        try (var c = lowLargeObjectCache) {
            return; // close only
        }
    }

    @Override
    public void close(long timeoutNanos) throws IOException {
        close();
    }
}
