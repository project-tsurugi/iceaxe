package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;

/**
 * Tsurugi transaction manager execute log
 */
public class TgSessionTmLog {

    private int iceaxeTmExecuteId;
    private TgSessionTxLog txLog;

    private ZonedDateTime startTime;
    private ZonedDateTime endTime;

    /**
     * set iceaxe tm executeId
     *
     * @param iceaxeTmExecuteId iceaxe tm executeId
     */
    public void setIceaxeTmExecuteId(int iceaxeTmExecuteId) {
        this.iceaxeTmExecuteId = iceaxeTmExecuteId;
    }

    /**
     * get iceaxe tm executeId
     *
     * @return iceaxe tm executeId
     */
    public int getIceaxeTmExecuteId() {
        return this.iceaxeTmExecuteId;
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
