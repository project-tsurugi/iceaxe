package com.tsurugidb.iceaxe.session;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.explain.TsurugiExplainHelper;
import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.util.TgTimeValue;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * Tsurugi Session Information
 */
@ThreadSafe
public class TgSessionInfo {

    /**
     * create Session Information
     *
     * @return Session Information
     */
    public static TgSessionInfo of() {
        return new TgSessionInfo();
    }

    /**
     * create Session Information
     *
     * @param user     user id
     * @param password password
     * @return Session Information
     */
    @Deprecated(forRemoval = true) // TODO remove TgSessionInfo.of(user, password)
    public static TgSessionInfo of(@Nonnull String user, @Nullable String password) {
        var credential = new UsernamePasswordCredential(user, password);
        return of(credential);
    }

    /**
     * create Session Information
     *
     * @param credential Credential
     * @return Session Information
     */
    public static TgSessionInfo of(@Nonnull Credential credential) {
        return new TgSessionInfo().credential(credential);
    }

    /**
     * timeout key
     */
    public enum TgTimeoutKey {
        /** default */
        DEFAULT,

        /** {@link TsurugiSession} connect */
        SESSION_CONNECT,
        /** {@link TsurugiSession} close */
        SESSION_CLOSE,

        /** {@link TsurugiTableMetadataHelper} connect */
        METADATA_CONNECT,
        /** {@link TsurugiTableMetadataHelper} close */
        METADATA_CLOSE,

        /** {@link TsurugiExplainHelper} connect */
        EXPLAIN_CONNECT,
        /** {@link TsurugiExplainHelper} close */
        EXPLAIN_CLOSE,

        /** {@link TsurugiPreparedStatementWithLowPs} connect */
        PS_CONNECT,
        /** {@link TsurugiPreparedStatementWithLowPs} close */
        PS_CLOSE,

        /** {@link TsurugiTransaction} begin */
        TRANSACTION_BEGIN,
        /** {@link TsurugiTransaction} commit */
        TRANSACTION_COMMIT,
        /** {@link TsurugiTransaction} rollback */
        TRANSACTION_ROLLBACK,
        /** {@link TsurugiTransaction} close */
        TRANSACTION_CLOSE,

        /** {@link TsurugiResultSet} connect */
        RS_CONNECT,
        /** {@link TsurugiResultSet} close */
        RS_CLOSE,
        /** {@link TsurugiResultCount} check */
        RESULT_CHECK,
        /** {@link TsurugiResultCount} close */
        RESULT_CLOSE,
    }

    private Credential credential;
    private final Map<TgTimeoutKey, TgTimeValue> timeoutMap = Collections.synchronizedMap(new EnumMap<>(TgTimeoutKey.class));
    private TgCommitType commitType = TgCommitType.DEFAULT;

    /**
     * Tsurugi Session Information
     */
    public TgSessionInfo() {
        timeoutMap.put(TgTimeoutKey.DEFAULT, TgTimeValue.of(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
    }

    /**
     * set credential
     *
     * @param credential Credential
     * @return this
     */
    public TgSessionInfo credential(@Nonnull Credential credential) {
        this.credential = credential;
        return this;
    }

    /**
     * get credential
     *
     * @return Credential
     */
    public Credential credential() {
        return this.credential;
    }

    /**
     * set timeout
     *
     * @param key  timeout key
     * @param time timeout time
     * @param unit timeout unit
     *
     * @return this
     */
    public TgSessionInfo timeout(@Nonnull TgTimeoutKey key, long time, @Nonnull TimeUnit unit) {
        timeoutMap.put(key, new TgTimeValue(time, unit));
        return this;
    }

    /**
     * get timeout time
     *
     * @param key timeout key
     * @return time
     */
    public TgTimeValue timeout(TgTimeoutKey key) {
        TgTimeValue value = timeoutMap.get(key);
        if (value != null) {
            return value;
        }
        return timeoutMap.get(TgTimeoutKey.DEFAULT);
    }

    /**
     * set commit type
     *
     * @param commitType commit type
     * @return this
     */
    public TgSessionInfo commitType(@Nonnull TgCommitType commitType) {
        this.commitType = commitType;
        return this;
    }

    /**
     * get commit type
     *
     * @return commit type
     */
    public TgCommitType commitType() {
        return this.commitType;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{credential=" + credential + ", timeout=" + timeoutMap + ", commitType=" + commitType + "}";
    }
}
