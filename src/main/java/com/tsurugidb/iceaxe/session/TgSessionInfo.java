package com.tsurugidb.iceaxe.session;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.result.TsurugiResult;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementWithLowPs;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.util.TgTimeValue;

/**
 * Tsurugi Session Information
 */
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
    public static TgSessionInfo of(String user, String password) {
        var info = new TgSessionInfo().user(user).password(password);
        return info;
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

        /** {@link TsurugiResult} connect */
        RESULT_CONNECT,
        /** {@link TsurugiResultSet} connect */
        RS_CONNECT,
    }

    private String user;
    private String password;
    private final Map<TgTimeoutKey, TgTimeValue> timeoutMap = new EnumMap<>(TgTimeoutKey.class);

    /**
     * Tsurugi Session Information
     */
    public TgSessionInfo() {
        timeoutMap.put(TgTimeoutKey.DEFAULT, TgTimeValue.of(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
    }

    /**
     * set user
     * 
     * @param user user id
     * @return this
     */
    public TgSessionInfo user(String user) {
        this.user = user;
        return this;
    }

    /**
     * set password
     * 
     * @param password password
     * @return this
     */
    public TgSessionInfo password(String password) {
        this.password = password;
        return this;
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
    public TgSessionInfo timeout(TgTimeoutKey key, long time, TimeUnit unit) {
        timeoutMap.put(key, new TgTimeValue(time, unit));
        return this;
    }

    /**
     * get user
     * 
     * @return user id
     */
    public String user() {
        return user;
    }

    /**
     * get password
     * 
     * @return password
     */
    public String password() {
        return password;
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{user=" + user + ", password=" + (password != null ? "???" : null) + ", timeout=" + timeoutMap + "}";
    }
}
