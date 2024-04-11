package com.tsurugidb.iceaxe.session;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.metadata.TsurugiTableListHelper;
import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.explain.TsurugiExplainHelper;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
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
        TABLE_LIST_CLOSE,
        /** {@link TsurugiTableMetadataHelper} connect */
        TABLE_METADATA_CONNECT,
        /** {@link TsurugiTableMetadataHelper} close */
        TABLE_METADATA_CLOSE,

        ;

        /**
         * {@link TsurugiStatementResult} connect.
         *
         * @see #RESULT_CONNECT
         */
        @Deprecated(forRemoval = true, since = "1.3.0")
        public static final TgTimeoutKey RESULT_CHECK = RESULT_CONNECT;
        /**
         * {@link TsurugiTableMetadataHelper} connect.
         *
         * @see #TABLE_METADATA_CONNECT
         */
        @Deprecated(forRemoval = true, since = "1.3.0")
        public static final TgTimeoutKey METADATA_CONNECT = TABLE_METADATA_CONNECT;
        /**
         * {@link TsurugiTableMetadataHelper} close.
         *
         * @see #TABLE_METADATA_CLOSE
         */
        @Deprecated(forRemoval = true, since = "1.3.0")
        public static final TgTimeoutKey METADATA_CLOSE = TABLE_METADATA_CLOSE;
    }

    private String sessionLabel;
    private final Map<TgTimeoutKey, TgTimeValue> timeoutMap = Collections.synchronizedMap(new EnumMap<>(TgTimeoutKey.class));
    private TgCommitType commitType = TgCommitType.DEFAULT;

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
    public TgSessionOption setLabel(String label) {
        this.sessionLabel = label;
        return this;
    }

    /**
     * get session label.
     *
     * @return session label
     */
    public String getLabel() {
        return this.sessionLabel;
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
        this.commitType = commitType;
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{label=" + sessionLabel + ", timeout=" + timeoutMap + ", commitType=" + commitType + "}";
    }
}
