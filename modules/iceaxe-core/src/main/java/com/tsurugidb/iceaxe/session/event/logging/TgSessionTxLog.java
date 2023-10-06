package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * Tsurugi transaction log.
 */
public class TgSessionTxLog {
    private static final Logger LOG = LoggerFactory.getLogger(TgSessionTxLog.class);

    private TsurugiTransaction transaction;
    private String transactionId;
    private TgSessionTmLog tmLog;
    private final Map<Integer, TgSessionSqlLog> sqlLogMap = new ConcurrentHashMap<>();

    private ZonedDateTime startTime;
    private ZonedDateTime lowGetStartTime;
    private ZonedDateTime lowGetEndTime;
    private final Map<Integer, TgSessionTxExecuteLog> timeMap = new ConcurrentHashMap<>();
    private ZonedDateTime commitStartTime;
    private ZonedDateTime commitEndTime;
    private ZonedDateTime rollbackStartTime;
    private ZonedDateTime rollbackEndTime;
    private ZonedDateTime closeTime;

    /**
     * set transaction.
     *
     * @param transaction transaction
     */
    public void setTransaction(TsurugiTransaction transaction) {
        this.transaction = transaction;
    }

    /**
     * get transaction.
     *
     * @return transaction
     */
    public TsurugiTransaction getTransaction() {
        return this.transaction;
    }

    /**
     * set transactionId.
     *
     * @param transactionId transactionId
     */
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * set transactionId.
     *
     * @return transactionId
     */
    public String getTransactionId() {
        return this.transactionId;
    }

    /**
     * set transaction manager log.
     *
     * @param tmLog transaction manager log
     */
    public void setTmLog(TgSessionTmLog tmLog) {
        this.tmLog = tmLog;
    }

    /**
     * get transaction manager log.
     *
     * @return transaction manager log
     */
    public TgSessionTmLog getTmLog() {
        return this.tmLog;
    }

    /**
     * get new SQL execute log.
     *
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @return SQL execute log
     */
    public TgSessionSqlLog getNewSqlLog(int iceaxeSqlExecuteId) {
        var startTime = ZonedDateTime.now();

        var log = createSqlLog();
        log.setStartTime(startTime);
        log.setIceaxeSqlExecuteId(iceaxeSqlExecuteId);

        sqlLogMap.put(iceaxeSqlExecuteId, log);
        return log;
    }

    /**
     * Creates a new SQL execute log instance.
     *
     * @return SQL execute log
     */
    protected TgSessionSqlLog createSqlLog() {
        return new TgSessionSqlLog();
    }

    /**
     * get SQL execute log.
     *
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @return SQL execute log
     */
    public TgSessionSqlLog getSqlLog(int iceaxeSqlExecuteId) {
        var log = sqlLogMap.get(iceaxeSqlExecuteId);
        if (log == null) {
            LOG.debug("sqlLog not found in sqlIdMap. iceaxeSqlExecuteId={}", iceaxeSqlExecuteId);
        }
        return log;
    }

    /**
     * get and remove SQL execute log.
     *
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @return SQL execute log
     */
    public TgSessionSqlLog removeSqlLog(int iceaxeSqlExecuteId) {
        return sqlLogMap.remove(iceaxeSqlExecuteId);
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
     * set low transaction get start time.
     *
     * @param time low transaction get start time
     */
    public void setLowGetStartTime(ZonedDateTime time) {
        this.lowGetStartTime = time;
    }

    /**
     * get low transaction get start time.
     *
     * @return low transaction get start time
     */
    public ZonedDateTime getLowGetStartTime() {
        return this.lowGetStartTime;
    }

    /**
     * set low transaction get end time.
     *
     * @param time low transaction get end time
     */
    public void setLowGetEndTime(ZonedDateTime time) {
        this.lowGetEndTime = time;
    }

    /**
     * get low transaction get end time.
     *
     * @return low transaction get end time
     */
    public ZonedDateTime getLowGetEndTime() {
        return this.lowGetEndTime;
    }

    /**
     * Tsurugi transaction SQL execute log.
     */
    public static class TgSessionTxExecuteLog {
        private int iceaxeTxExecuteId;
        private ZonedDateTime startTime;
        private ZonedDateTime endTime;

        /**
         * set iceaxe tx executeId.
         *
         * @param iceaxeTxExecuteId iceaxe tx executeId
         */
        public void setIceaxeTxExecuteId(int iceaxeTxExecuteId) {
            this.iceaxeTxExecuteId = iceaxeTxExecuteId;
        }

        /**
         * get iceaxe tx executeId.
         *
         * @return iceaxe tx executeId
         */
        public int getIceaxeTxExecuteId() {
            return this.iceaxeTxExecuteId;
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

    /**
     * get new transaction SQL execute log.
     *
     * @param iceaxeTxExecuteId iceaxe tx executeId
     * @return transaction SQL execute log
     */
    public TgSessionTxExecuteLog getNewTxExecuteLog(int iceaxeTxExecuteId) {
        var exLog = createTxExecuteLog();
        exLog.setIceaxeTxExecuteId(iceaxeTxExecuteId);

        timeMap.put(iceaxeTxExecuteId, exLog);
        return exLog;
    }

    /**
     * Creates a new transaction SQL execute log instance.
     *
     * @return transaction SQL execute log
     */
    protected TgSessionTxExecuteLog createTxExecuteLog() {
        return new TgSessionTxExecuteLog();
    }

    /**
     * get and remove transaction SQL execute log.
     *
     * @param iceaxeTxExecuteId iceaxe tx executeId
     * @return transaction SQL execute log
     */
    public TgSessionTxExecuteLog removeTxExecuteLog(int iceaxeTxExecuteId) {
        return timeMap.remove(iceaxeTxExecuteId);
    }

    /**
     * set commit start time.
     *
     * @param time commit start time
     */
    public void setCommitStartTime(ZonedDateTime time) {
        this.commitStartTime = time;
    }

    /**
     * set commit start time.
     *
     * @return commit start time
     */
    public ZonedDateTime getCommitStartTime() {
        return this.commitStartTime;
    }

    /**
     * set commit end time.
     *
     * @param time commit end time
     */
    public void setCommitEndTime(ZonedDateTime time) {
        this.commitEndTime = time;
    }

    /**
     * get commit end time.
     *
     * @return commit end time
     */
    public ZonedDateTime getCommitEndTime() {
        return this.commitEndTime;
    }

    /**
     * set rollback start time.
     *
     * @param time rollback start time
     */
    public void setRollbackStartTime(ZonedDateTime time) {
        this.rollbackStartTime = time;
    }

    /**
     * get rollback start time.
     *
     * @return rollback start time
     */
    public ZonedDateTime getRollbackStartTime() {
        return this.rollbackStartTime;
    }

    /**
     * set rollback end time.
     *
     * @param time rollback end time
     */
    public void setRollbackEndTime(ZonedDateTime time) {
        this.rollbackEndTime = time;
    }

    /**
     * get rollback end time.
     *
     * @return rollback end time
     */
    public ZonedDateTime getRollbackEndTime() {
        return this.rollbackEndTime;
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
