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

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;

/**
 * Tsurugi SQL execute log.
 */
public class TgSessionSqlLog {

    private int iceaxeSqlExecuteId;
    private TsurugiSql sqlDefinition;
    private Object sqlParameter;
    private TsurugiSqlResult result;

    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    private ZonedDateTime closeTime;

    /**
     * set iceaxe SQL executeId.
     *
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    public void setIceaxeSqlExecuteId(int iceaxeSqlExecuteId) {
        this.iceaxeSqlExecuteId = iceaxeSqlExecuteId;
    }

    /**
     * get SQL executeId.
     *
     * @return SQL executeId
     */
    public int getIceaxeSqlExecuteId() {
        return this.iceaxeSqlExecuteId;
    }

    /**
     * set SQL definition.
     *
     * @param ps        SQL definition
     * @param parameter SQL parameter
     */
    public void setSqlStatement(TsurugiSql ps, Object parameter) {
        this.sqlDefinition = ps;
        this.sqlParameter = parameter;
    }

    /**
     * get SQL definition.
     *
     * @return SQL definition
     */
    public TsurugiSql getSqlDefinition() {
        return this.sqlDefinition;
    }

    /**
     * get SQL parameter.
     *
     * @return SQL parameter
     */
    public Object getSqlParameter() {
        return this.sqlParameter;
    }

    /**
     * get iceaxe sqlId.
     *
     * @return iceaxe sqlId
     */
    public int getIceaxeSqlDefinitionId() {
        return this.sqlDefinition.getIceaxeSqlId();
    }

    /**
     * set SQL result.
     *
     * @param result SQL result
     */
    public void setSqlResult(TsurugiSqlResult result) {
        this.result = result;
    }

    /**
     * get SQL result.
     *
     * @return SQL result
     */
    public TsurugiSqlResult getSqlResult() {
        return this.result;
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

    /**
     * set close time.
     *
     * @param time close time
     */
    public void setCloseTime(ZonedDateTime time) {
        this.closeTime = time;
    }

    /**
     * get close time.
     *
     * @return close time
     */
    public ZonedDateTime getCloseTime() {
        return this.closeTime;
    }
}
