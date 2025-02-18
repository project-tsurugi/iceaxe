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
package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;

/**
 * Tsurugi transaction manager execute log.
 */
public class TgSessionTmLog {

    private int iceaxeTmExecuteId;
    private TgSessionTxLog txLog;

    private ZonedDateTime startTime;
    private ZonedDateTime endTime;

    /**
     * set iceaxe tm executeId.
     *
     * @param iceaxeTmExecuteId iceaxe tm executeId
     */
    public void setIceaxeTmExecuteId(int iceaxeTmExecuteId) {
        this.iceaxeTmExecuteId = iceaxeTmExecuteId;
    }

    /**
     * get iceaxe tm executeId.
     *
     * @return iceaxe tm executeId
     */
    public int getIceaxeTmExecuteId() {
        return this.iceaxeTmExecuteId;
    }

    /**
     * set current transaction log.
     *
     * @param txLog transaction log
     */
    public void setCurrentTxLog(TgSessionTxLog txLog) {
        this.txLog = txLog;
    }

    /**
     * get current transaction log.
     *
     * @return transaction log
     */
    public TgSessionTxLog getCurrentTxLog() {
        return this.txLog;
    }

    /**
     * set start time.
     *
     * @param time start time
     */
    public void setStartTime(ZonedDateTime time) {
        this.startTime = time;
    }

    /**
     * get start time.
     *
     * @return start time
     */
    public ZonedDateTime getStartTime() {
        return this.startTime;
    }

    /**
     * set end time.
     *
     * @param time end time
     */
    public void setEndTime(ZonedDateTime time) {
        this.endTime = time;
    }

    /**
     * get end time.
     *
     * @return end time
     */
    public ZonedDateTime getEndTime() {
        return this.endTime;
    }
}
