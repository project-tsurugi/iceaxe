package com.tsurugi.iceaxe.session;

import java.util.concurrent.TimeUnit;

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

    private String user;
    private String password;
    private long timeoutTime = Long.MAX_VALUE;
    private TimeUnit timeoutUnit = TimeUnit.NANOSECONDS;

    /**
     * Tsurugi Session Information
     */
    public TgSessionInfo() {
        // do nothing
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
     * @param time timeout time
     * @param unit timeout unit
     * 
     * @return this
     */
    public TgSessionInfo timeout(long time, TimeUnit unit) {
        this.timeoutUnit = unit;
        this.timeoutTime = time;
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
     * @return time
     */
    public long timeoutTime() {
        return timeoutTime;
    }

    /**
     * get timeout unit
     * 
     * @return unit
     */
    public TimeUnit timeoutUnit() {
        return timeoutUnit;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{user=" + user + ", password=" + (password != null ? "???" : null) + ", timeout=" + timeoutTime + timeoutUnit + "}";
    }
}
