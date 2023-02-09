package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;

/**
 * Tsurugi transaction manager log
 */
public class TgSessionTmLog {

    private int executeId;
    private TgSessionTxLog txLog;

    private ZonedDateTime startTime;
    private ZonedDateTime endTime;

    /**
     * set transaction manager executeId
     *
     * @param executeId executeId
     */
    public void setExecuteId(int executeId) {
        this.executeId = executeId;
    }

    /**
     * get transaction manager executeId
     *
     * @return executeId
     */
    public int getExecuteId() {
        return this.executeId;
    }

    /**
     * set current transaction log
     *
     * @param txLog transaction log
     */
    public void setCurrentTxLog(TgSessionTxLog txLog) {
        this.txLog = txLog;
    }

    /**
     * get current transaction log
     *
     * @return transaction log
     */
    public TgSessionTxLog getCurrentTxLog() {
        return this.txLog;
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
}
