package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;

import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;

/**
 * Tsurugi SQL execute log
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
     * set iceaxe SQL executeId
     *
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     */
    public void setIceaxeSqExecutelId(int iceaxeSqlExecuteId) {
        this.iceaxeSqlExecuteId = iceaxeSqlExecuteId;
    }

    /**
     * get SQL executeId
     *
     * @return SQL executeId
     */
    public int getIceaxeSqlExecuteId() {
        return this.iceaxeSqlExecuteId;
    }

    /**
     * set SQL definition
     *
     * @param ps        SQL definition
     * @param parameter SQL parameter
     */
    public void setSqlStatement(TsurugiSql ps, Object parameter) {
        this.sqlDefinition = ps;
        this.sqlParameter = parameter;
    }

    /**
     * get SQL definition
     *
     * @return SQL definition
     */
    public TsurugiSql getSqlStatement() {
        return this.sqlDefinition;
    }

    /**
     * get SQL parameter
     *
     * @return SQL parameter
     */
    public Object getSqlParameter() {
        return this.sqlParameter;
    }

    /**
     * get SQL statementId
     *
     * @return SQL statementId
     */
    public int getIceaxeSqlStatementId() {
        return this.sqlDefinition.getIceaxeSqlId();
    }

    /**
     * set result
     *
     * @param result SQL result
     */
    public void setSqlResult(TsurugiSqlResult result) {
        this.result = result;
    }

    /**
     * get SQL result
     *
     * @return SQL result
     */
    public TsurugiSqlResult getSqlResult() {
        return this.result;
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
