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
package com.tsurugidb.iceaxe.session;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.metadata.TsurugiTableListHelper;
import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.explain.TsurugiExplainHelper;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.type.TgBlobReference;
import com.tsurugidb.iceaxe.sql.type.TgClobReference;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.status.TsurugiTransactionStatusHelper;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Session Option.
 *
 * @see TsurugiConnector#createSession(TgSessionOption)
 */
@ThreadSafe
public class TgSessionOption {

    /**
     * create session option.
     *
     * @return session option
     */
    public static TgSessionOption of() {
        return new TgSessionOption();
    }

    /**
     * timeout key.
     */
    public enum TgTimeoutKey {
        /** default */
        DEFAULT,

        /** {@link TsurugiSession} connect */
        SESSION_CONNECT,
        /** {@link TsurugiSession} close */
        SESSION_CLOSE,

        /** {@link TsurugiTransaction} begin */
        TRANSACTION_BEGIN,
        /** {@link TsurugiTransaction} commit */
        TRANSACTION_COMMIT,
        /** {@link TsurugiTransaction} rollback */
        TRANSACTION_ROLLBACK,
        /** {@link TsurugiTransaction} close */
        TRANSACTION_CLOSE,
        /** {@link TsurugiTransactionStatusHelper} connect */
        TX_STATUS_CONNECT,
        /** {@link TsurugiTransactionStatusHelper} close */
        @Deprecated(since = "1.4.0")
        TX_STATUS_CLOSE,

        /** {@link TsurugiSqlPrepared} connect */
        PS_CONNECT,
        /** {@link TsurugiSqlPrepared} close */
        PS_CLOSE,

        /** {@link TsurugiQueryResult} connect */
        RS_CONNECT,
        /** {@link TsurugiQueryResult} close */
        RS_CLOSE,
        /** {@link TsurugiStatementResult} connect */
        RESULT_CONNECT,
        /** {@link TsurugiStatementResult} close */
        RESULT_CLOSE,

        /** {@link TsurugiExplainHelper} connect */
        EXPLAIN_CONNECT,
        /** {@link TsurugiExplainHelper} close */
        EXPLAIN_CLOSE,

        /** {@link TsurugiTableListHelper} connect */
        TABLE_LIST_CONNECT,
        /** {@link TsurugiTableListHelper} close */
        @Deprecated(since = "1.4.0")
        TABLE_LIST_CLOSE,
        /** {@link TsurugiTableMetadataHelper} connect */
        TABLE_METADATA_CONNECT,
        /** {@link TsurugiTableMetadataHelper} close */
        @Deprecated(since = "1.4.0")
        TABLE_METADATA_CLOSE,

        /**
         * {@link TgBlobReference} get.
         *
         * @since X.X.X
         */
        BLOB_GET,
        /**
         * {@link TgBlobReference} cache get.
         *
         * @since X.X.X
         */
        BLOB_CACHE_GET,
        /**
         * {@link TgClobReference} get.
         *
         * @since X.X.X
         */
        CLOB_GET,
        /**
         * {@link TgClobReference} cache get.
         *
         * @since X.X.X
         */
        CLOB_CACHE_GET,

        //
        ;
    }

    private String sessionLabel;
    private String applicationName;
    private Optional<Boolean> keepAlive = Optional.empty();
    private final Map<TgTimeoutKey, TgTimeValue> timeoutMap = Collections.synchronizedMap(new EnumMap<>(TgTimeoutKey.class));
    private TgCommitType commitType = TgCommitType.DEFAULT;
    private TgSessionShutdownType closeShutdownType = TgSessionShutdownType.NOTHING;

    /**
     * Tsurugi Session Option.
     */
    public TgSessionOption() {
        timeoutMap.put(TgTimeoutKey.DEFAULT, TgTimeValue.of(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
    }

    /**
     * set session label.
     *
     * @param label session label
     * @return this
     */
    public TgSessionOption setLabel(@Nullable String label) {
        this.sessionLabel = label;
        return this;
    }

    /**
     * get session label.
     *
     * @return session label
     */
    public @Nullable String getLabel() {
        return this.sessionLabel;
    }

    /**
     * set application name.
     *
     * @param name application name
     * @return this
     * @since 1.4.0
     */
    public TgSessionOption setApplicationName(@Nullable String name) {
        this.applicationName = name;
        return this;
    }

    /**
     * get application name.
     *
     * @return application name
     * @since 1.4.0
     */
    public @Nullable String getApplicationName() {
        return this.applicationName;
    }

    /**
     * set session keep-alive.
     *
     * @param enabled {@code true} to enable session keep-alive, or {@code false} to disable it
     * @return this
     * @since 1.6.0
     */
    public TgSessionOption setKeepAlive(@Nullable Boolean enabled) {
        this.keepAlive = Optional.ofNullable(enabled);
        return this;
    }

    /**
     * get session keep-alive.
     *
     * @return session keep-alive
     * @since 1.6.0
     */
    public Optional<Boolean> findKeepAlive() {
        return this.keepAlive;
    }

    /**
     * set timeout.
     *
     * @param key  timeout key
     * @param time timeout time
     * @param unit timeout unit
     *
     * @return this
     */
    public TgSessionOption setTimeout(@Nonnull TgTimeoutKey key, long time, @Nonnull TimeUnit unit) {
        timeoutMap.put(key, new TgTimeValue(time, unit));
        return this;
    }

    /**
     * get timeout time.
     *
     * @param key timeout key
     * @return time
     */
    public TgTimeValue getTimeout(TgTimeoutKey key) {
        TgTimeValue value = timeoutMap.get(key);
        if (value != null) {
            return value;
        }
        return timeoutMap.get(TgTimeoutKey.DEFAULT);
    }

    /**
     * set commit type.
     *
     * @param commitType commit type
     * @return this
     */
    public TgSessionOption setCommitType(@Nonnull TgCommitType commitType) {
        this.commitType = Objects.requireNonNull(commitType);
        return this;
    }

    /**
     * get commit type.
     *
     * @return commit type
     */
    public TgCommitType getCommitType() {
        return this.commitType;
    }

    /**
     * set shutdown type on close.
     *
     * @param shutdownType shutdown type
     * @return this
     * @since 1.4.0
     */
    public TgSessionOption setCloseShutdownType(@Nonnull TgSessionShutdownType shutdownType) {
        this.closeShutdownType = Objects.requireNonNull(shutdownType);
        return this;
    }

    /**
     * get shutdown type on close.
     *
     * @return shutdown type
     * @since 1.4.0
     */
    public TgSessionShutdownType getCloseShutdownType() {
        return this.closeShutdownType;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() //
                + "{label=" + sessionLabel //
                + ", applicationName=" + applicationName //
                + ", timeout=" + timeoutMap //
                + ", commitType=" + commitType //
                + ", closeShutdownType=" + closeShutdownType //
                + "}";
    }
}
