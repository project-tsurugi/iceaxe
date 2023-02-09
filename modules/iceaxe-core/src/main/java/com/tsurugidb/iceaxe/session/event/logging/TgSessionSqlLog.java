package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;

import com.tsurugidb.iceaxe.session.event.logging.TgSessionTxLog.TgSqlLogKey;

/**
 * Tsurugi SQL statement log
 */
public class TgSessionSqlLog {

    private int sqlId;
    private TgSqlLogKey key;
    private int readCount = 0;

    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    private ZonedDateTime closeTime;

    /**
     * set sqlId
     *
     * @param sqlId sqlId
     */
    public void setSqlId(int sqlId) {
        this.sqlId = sqlId;
    }

    /**
     * get sqlId
     *
     * @return sqlId
     */
    public int getSqlId() {
        return this.sqlId;
    }

    /**
     * set sqlLog key
     *
     * @param key sqlLog key
     */
    public void setSqlLogKey(TgSqlLogKey key) {
        this.key = key;
    }

    /**
     * get sqlLog key
     *
     * @return sqlLog key
     */
    public TgSqlLogKey getSqlLogKey() {
        return this.key;
    }

    /**
     * increment read count
     */
    public void incrementReadCount() {
        this.readCount++;
    }

    /**
     * get read count
     *
     * @return read count
     */
    public int getReadCount() {
        return this.readCount;
    }

    /**
     * set start time
     *
     * @param time start time
     */
    public void setStartTime(ZonedDateTime time) {
        this.startTime = time;
    }

    /**
     * get start time
     *
     * @return start time
     */
    public ZonedDateTime getStartTime() {
        return this.startTime;
    }

    /**
     * set end time
     *
     * @param time end time
     */
    public void setEndTime(ZonedDateTime time) {
        this.endTime = time;
    }

    /**
     * get end time
     *
     * @return end time
     */
    public ZonedDateTime getEndTime() {
        return this.endTime;
    }

    /**
     * set close time
     *
     * @param time close time
     */
    public void setCloseTime(ZonedDateTime time) {
        this.closeTime = time;
    }

    /**
     * get close time
     *
     * @return close time
     */
    public ZonedDateTime getCloseTime() {
        return this.closeTime;
    }
}
