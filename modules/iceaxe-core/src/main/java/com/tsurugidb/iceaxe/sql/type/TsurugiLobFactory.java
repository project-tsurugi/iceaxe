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
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi large object factory.
 *
 * @since 1.16.0
 */
public class TsurugiLobFactory {

    private final TsurugiSession session;
    private IceaxeObjectFactory objectFactory = null;
    private TgLobPersistenceType defaultPersistenceType = null;

    private final IceaxeTimeout blobUploadTimeout;
    private final IceaxeTimeout clobUploadTimeout;

    /**
     * Creates a new instance.
     *
     * @param session Tsurugi session
     */
    public TsurugiLobFactory(TsurugiSession session) {
        this.session = session;

        var sessionOption = session.getSessionOption();
        this.blobUploadTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.BLOB_UPLOAD);
        this.clobUploadTimeout = new IceaxeTimeout(sessionOption, TgTimeoutKey.CLOB_UPLOAD);
    }

    /**
     * Set object factory.
     *
     * @param objectFactory object factory
     */
    public void setIceaxeObjectFactory(IceaxeObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }

    /**
     * Get object factory.
     *
     * @return object factory
     */
    public IceaxeObjectFactory getIceaxeObjectFactory() {
        var factory = this.objectFactory;
        if (factory != null) {
            return factory;
        }
        return IceaxeObjectFactory.getDefaultInstance();
    }

    /**
     * Set default persistence type.
     *
     * @param persistenceType persistence type
     */
    public void setDefaultPersistenceType(TgLobPersistenceType persistenceType) {
        this.defaultPersistenceType = Objects.requireNonNull(persistenceType);
    }

    /**
     * Get default persistence type.
     *
     * @return persistence type
     */
    public TgLobPersistenceType getDefaultPersistenceType() {
        return this.defaultPersistenceType;
    }

    /**
     * Get persistence type.
     *
     * @param persistenceType persistence type
     * @return persistence type
     */
    protected TgLobPersistenceType getPersistenceType(TgLobPersistenceType persistenceType) {
        if (persistenceType != null) {
            return persistenceType;
        }
        return this.defaultPersistenceType;
    }

    /**
     * Set blob upload timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setBlobUploadTimeout(long time, TimeUnit unit) {
        setBlobUploadTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * Set blob upload timeout.
     *
     * @param timeout time
     */
    public void setBlobUploadTimeout(TgTimeValue timeout) {
        blobUploadTimeout.set(timeout);
    }

    /**
     * Set clob upload timeout.
     *
     * @param time timeout time
     * @param unit timeout unit
     */
    public void setClobUploadTimeout(long time, TimeUnit unit) {
        setClobUploadTimeout(TgTimeValue.of(time, unit));
    }

    /**
     * Set clob upload timeout.
     *
     * @param timeout time
     */
    public void setClobUploadTimeout(TgTimeValue timeout) {
        clobUploadTimeout.set(timeout);
    }

    /**
     * Get blob upload timeout.
     *
     * @param value timeout value (if null, use default timeout)
     * @return blob upload timeout
     */
    protected IceaxeTimeout getBlobUploadTimeout(@Nullable TgTimeValue value) {
        if (value == null) {
            return this.blobUploadTimeout;
        }

        var timeout = new IceaxeTimeout(getSessionOption(), TgTimeoutKey.BLOB_UPLOAD);
        timeout.set(value);
        return timeout;
    }

    /**
     * Get clob upload timeout.
     *
     * @param value timeout value (if null, use default timeout)
     * @return clob upload timeout
     */
    protected IceaxeTimeout getClobUploadTimeout(@Nullable TgTimeValue value) {
        if (value == null) {
            return this.clobUploadTimeout;
        }

        var timeout = new IceaxeTimeout(getSessionOption(), TgTimeoutKey.CLOB_UPLOAD);
        timeout.set(value);
        return timeout;
    }

    /**
     * Get session option.
     *
     * @return session option
     */
    protected TgSessionOption getSessionOption() {
        return session.getSessionOption();
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param path path
     * @return BLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgBlob createBlob(Path path) throws IOException, InterruptedException {
        return createBlob(path, null);
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param path            path
     * @param persistenceType persistence type
     * @return BLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgBlob createBlob(Path path, TgLobPersistenceType persistenceType) throws IOException, InterruptedException {
        if (path == null) {
            return null;
        }

        var objectFactory = getIceaxeObjectFactory();
        var type = getPersistenceType(persistenceType);
        return objectFactory.createBlob(path, type);
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param is input stream
     * @return BLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgBlob createBlob(InputStream is) throws IOException, InterruptedException {
        return createBlob(is, null);
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param is              input stream
     * @param persistenceType persistence type
     * @return BLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     * @apiNote If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgBlob is closed.
     */
    public TgBlob createBlob(InputStream is, TgLobPersistenceType persistenceType) throws IOException, InterruptedException {
        if (is == null) {
            return null;
        }

        try (is) {
            var objectFactory = getIceaxeObjectFactory();
            var type = getPersistenceType(persistenceType);
            return objectFactory.createBlob(is, type);
        }
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param value byte array
     * @return BLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgBlob createBlob(byte[] value) throws IOException, InterruptedException {
        return createBlob(value, null);
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param value           byte array
     * @param persistenceType persistence type
     * @return BLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     * @apiNote If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgBlob is closed.
     */
    public TgBlob createBlob(byte[] value, TgLobPersistenceType persistenceType) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var objectFactory = getIceaxeObjectFactory();
        var type = getPersistenceType(persistenceType);
        return objectFactory.createBlob(value, type);
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param value BLOB reference
     * @return BLOB instance
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgBlob createBlob(TgBlobReference value) throws IOException, InterruptedException, TsurugiTransactionException {
        return createBlob(value, null);
    }

    /**
     * Creates a new BLOB instance.
     *
     * @param value           BLOB reference
     * @param persistenceType persistence type
     * @return BLOB instance
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     * @apiNote If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgBlob is closed.
     */
    public TgBlob createBlob(TgBlobReference value, TgLobPersistenceType persistenceType) throws IOException, InterruptedException, TsurugiTransactionException {
        if (value == null) {
            return null;
        }

        var objectFactory = getIceaxeObjectFactory();
        var type = getPersistenceType(persistenceType);
        return objectFactory.createBlob(value, type);
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param path path
     * @return CLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgClob createClob(Path path) throws IOException, InterruptedException {
        return createClob(path, null);
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param path            path
     * @param persistenceType persistence type
     * @return CLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgClob createClob(Path path, TgLobPersistenceType persistenceType) throws IOException, InterruptedException {
        if (path == null) {
            return null;
        }

        var objectFactory = getIceaxeObjectFactory();
        var type = getPersistenceType(persistenceType);
        return objectFactory.createClob(path, type);
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param reader reader
     * @return CLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgClob createClob(Reader reader) throws IOException, InterruptedException {
        return createClob(reader, null);
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param reader          reader
     * @param persistenceType persistence type
     * @return CLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     * @apiNote If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgClob is closed.
     */
    public TgClob createClob(Reader reader, TgLobPersistenceType persistenceType) throws IOException, InterruptedException {
        if (reader == null) {
            return null;
        }

        try (reader) {
            var objectFactory = getIceaxeObjectFactory();
            var type = getPersistenceType(persistenceType);
            return objectFactory.createClob(reader, type);
        }
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param value string value
     * @return CLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgClob createClob(String value) throws IOException, InterruptedException {
        return createClob(value, null);
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param value           string value
     * @param persistenceType persistence type
     * @return CLOB instance
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     * @apiNote If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgClob is closed.
     */
    public TgClob createClob(String value, TgLobPersistenceType persistenceType) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var objectFactory = getIceaxeObjectFactory();
        var type = getPersistenceType(persistenceType);
        return objectFactory.createClob(value, type);
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param value CLOB reference
     * @return CLOB instance
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgClob createClob(TgClobReference value) throws IOException, InterruptedException, TsurugiTransactionException {
        return createClob(value, null);
    }

    /**
     * Creates a new CLOB instance.
     *
     * @param value           CLOB reference
     * @param persistenceType persistence type
     * @return CLOB instance
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     * @apiNote If the persistence type is FILE, a temporary file is created. The temporary file is deleted when TgClob is closed.
     */
    public TgClob createClob(TgClobReference value, TgLobPersistenceType persistenceType) throws IOException, InterruptedException, TsurugiTransactionException {
        if (value == null) {
            return null;
        }

        var objectFactory = getIceaxeObjectFactory();
        var type = getPersistenceType(persistenceType);
        return objectFactory.createClob(value, type);
    }

    /**
     * Upload BLOB.
     *
     * @param path path
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(Path path) throws IOException, InterruptedException {
        return uploadBlob(path, null);
    }

    /**
     * Upload BLOB.
     *
     * @param path path
     * @param time timeout time
     * @param unit timeout unit
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(Path path, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadBlob(path, timeout);
    }

    /**
     * Upload BLOB.
     *
     * @param path    path
     * @param timeout timeout
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(Path path, TgTimeValue timeout) throws IOException, InterruptedException {
        if (path == null) {
            return null;
        }

        var helper = session.getLargeObjectHelper();
        var t = getBlobUploadTimeout(timeout);
        return helper.uploadBlob(session, path, t);
    }

    /**
     * Upload BLOB.
     *
     * @param is input stream
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(InputStream is) throws IOException, InterruptedException {
        return uploadBlob(is, null);
    }

    /**
     * Upload BLOB.
     *
     * @param is   input stream
     * @param time timeout time
     * @param unit timeout unit
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(InputStream is, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadBlob(is, timeout);
    }

    /**
     * Upload BLOB.
     *
     * @param is      input stream
     * @param timeout timeout
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(InputStream is, TgTimeValue timeout) throws IOException, InterruptedException {
        if (is == null) {
            return null;
        }

        var helper = session.getLargeObjectHelper();
        var t = getBlobUploadTimeout(timeout);
        return helper.uploadBlob(session, is, t);
    }

    /**
     * Upload BLOB.
     *
     * @param value byte array
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(byte[] value) throws IOException, InterruptedException {
        return uploadBlob(value, null);
    }

    /**
     * Upload BLOB.
     *
     * @param value byte array
     * @param time  timeout time
     * @param unit  timeout unit
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(byte[] value, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadBlob(value, timeout);
    }

    /**
     * Upload BLOB.
     *
     * @param value   byte array
     * @param timeout timeout
     * @return uploaded BLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteBlob uploadBlob(byte[] value, TgTimeValue timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var helper = session.getLargeObjectHelper();
        var t = getBlobUploadTimeout(timeout);
        return helper.uploadBlob(session, value, t);
    }

    /**
     * Upload BLOB.
     *
     * @param value BLOB reference
     * @return uploaded BLOB
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgRemoteBlob uploadBlob(TgBlob value) throws IOException, InterruptedException {
        return uploadBlob(value, null);
    }

    /**
     * Upload BLOB.
     *
     * @param value BLOB reference
     * @param time  timeout time
     * @param unit  timeout unit
     * @return uploaded BLOB
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgRemoteBlob uploadBlob(TgBlob value, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadBlob(value, timeout);
    }

    /**
     * Upload BLOB.
     *
     * @param value   BLOB reference
     * @param timeout timeout
     * @return uploaded BLOB
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgRemoteBlob uploadBlob(TgBlob value, TgTimeValue timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        return value.upload(session, timeout);
    }

    /**
     * Upload CLOB.
     *
     * @param path path
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(Path path) throws IOException, InterruptedException {
        return uploadClob(path, null);
    }

    /**
     * Upload CLOB.
     *
     * @param path path
     * @param time timeout time
     * @param unit timeout unit
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(Path path, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadClob(path, timeout);
    }

    /**
     * Upload CLOB.
     *
     * @param path    path
     * @param timeout timeout
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(Path path, TgTimeValue timeout) throws IOException, InterruptedException {
        if (path == null) {
            return null;
        }

        var helper = session.getLargeObjectHelper();
        var t = getClobUploadTimeout(timeout);
        return helper.uploadClob(session, path, t);
    }

    /**
     * Upload CLOB.
     *
     * @param reader reader
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(Reader reader) throws IOException, InterruptedException {
        return uploadClob(reader, null);
    }

    /**
     * Upload CLOB.
     *
     * @param reader reader
     * @param time   timeout time
     * @param unit   timeout unit
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(Reader reader, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadClob(reader, timeout);
    }

    /**
     * Upload CLOB.
     *
     * @param reader  reader
     * @param timeout timeout
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(Reader reader, TgTimeValue timeout) throws IOException, InterruptedException {
        if (reader == null) {
            return null;
        }

        var helper = session.getLargeObjectHelper();
        var t = getClobUploadTimeout(timeout);
        return helper.uploadClob(session, reader, t);
    }

    /**
     * Upload CLOB.
     *
     * @param value string value
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(String value) throws IOException, InterruptedException {
        return uploadClob(value, null);
    }

    /**
     * Upload CLOB.
     *
     * @param value string value
     * @param time  timeout time
     * @param unit  timeout unit
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(String value, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadClob(value, timeout);
    }

    /**
     * Upload CLOB.
     *
     * @param value   string value
     * @param timeout timeout
     * @return uploaded CLOB
     * @throws IOException          when I/O error occurs
     * @throws InterruptedException when interrupted while waiting for I/O operation
     */
    public TgRemoteClob uploadClob(String value, TgTimeValue timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        var helper = session.getLargeObjectHelper();
        var t = getClobUploadTimeout(timeout);
        return helper.uploadClob(session, value, t);
    }

    /**
     * Upload CLOB.
     *
     * @param value CLOB reference
     * @return uploaded CLOB
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgRemoteClob uploadClob(TgClob value) throws IOException, InterruptedException {
        return uploadClob(value, null);
    }

    /**
     * Upload CLOB.
     *
     * @param value CLOB reference
     * @param time  timeout time
     * @param unit  timeout unit
     * @return uploaded CLOB
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgRemoteClob uploadClob(TgClob value, long time, TimeUnit unit) throws IOException, InterruptedException {
        var timeout = TgTimeValue.of(time, unit);
        return uploadClob(value, timeout);
    }

    /**
     * Upload CLOB.
     *
     * @param value   CLOB reference
     * @param timeout timeout
     * @return uploaded CLOB
     * @throws IOException                 when I/O error occurs
     * @throws InterruptedException        when interrupted while waiting for I/O operation
     * @throws TsurugiTransactionException when transaction error occurs
     */
    public TgRemoteClob uploadClob(TgClob value, TgTimeValue timeout) throws IOException, InterruptedException {
        if (value == null) {
            return null;
        }

        return value.upload(session, timeout);
    }
}
