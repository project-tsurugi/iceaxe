package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TsurugiResult;
import com.tsurugidb.iceaxe.statement.TsurugiSql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * Tsurugi transaction log
 */
public class TgSessionTxLog {
    private static final Logger LOG = LoggerFactory.getLogger(TgSessionTxLog.class);

    /**
     * Tsurugi sqlLog key
     */
    public static final class TgSqlLogKey { // record

        private final TsurugiSql ps;
        private final Object parameter;

        /**
         * Creates a new instance.
         *
         * @param ps        SQL statement
         * @param parameter SQL parameter
         */
        public TgSqlLogKey(TsurugiSql ps, @Nullable Object parameter) {
            this.ps = ps;
            this.parameter = parameter;
        }

        /**
         * get SQL statement
         *
         * @return SQL statement
         */
        public TsurugiSql ps() {
            return this.ps;
        }

        /**
         * get SQL parameter
         *
         * @return SQL parameter
         */
        public Object parameter() {
            return this.parameter;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(ps) ^ System.identityHashCode(parameter);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof TgSqlLogKey) {
                var that = (TgSqlLogKey) obj;
                return that.ps == this.ps && that.parameter == this.parameter;
            }
            return false;
        }
    }

    private TsurugiTransaction transaction;
    private String transactionId;
    private TgSessionTmLog tmLog;
    private final AtomicInteger sqlCount = new AtomicInteger(0);
    private final Map<TgSqlLogKey, TgSessionSqlLog> sqlIdMap = new ConcurrentHashMap<>();
    private final Map<TsurugiResult, TgSessionSqlLog> resultMap = new ConcurrentHashMap<>();

    private ZonedDateTime startTime;
    private ZonedDateTime commitStartTime;
    private ZonedDateTime commitEndTime;
    private ZonedDateTime rollbackStartTime;
    private ZonedDateTime rollbackEndTime;
    private ZonedDateTime closeTime;

    /**
     * set transaction
     *
     * @param transaction transaction
     */
    public void setTransaction(TsurugiTransaction transaction) {
        this.transaction = transaction;
    }

    /**
     * get transaction
     *
     * @return transaction
     */
    public TsurugiTransaction getTransaction() {
        return this.transaction;
    }

    /**
     * set transactionId
     *
     * @param transactionId transactionId
     */
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * set transactionId
     *
     * @return transactionId
     */
    public String getTransactionId() {
        return this.transactionId;
    }

    /**
     * set transaction manager log
     *
     * @param tmLog transaction manager log
     */
    public void setTmLog(TgSessionTmLog tmLog) {
        this.tmLog = tmLog;
    }

    /**
     * get transaction manager log
     *
     * @return transaction manager log
     */
    public TgSessionTmLog getTmLog() {
        return this.tmLog;
    }

    /**
     * get new SQL statement log
     *
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @return SQL statement log
     */
    public TgSessionSqlLog getNewSqlLog(TsurugiSql ps, Object parameter) {
        var log = createSqlLog();
        log.setStartTime(ZonedDateTime.now());

        int sqlId = sqlCount.incrementAndGet();
        log.setSqlId(sqlId);

        var key = new TgSqlLogKey(ps, parameter);
        log.setSqlLogKey(key);

        sqlIdMap.put(key, log);
        return log;
    }

    /**
     * Creates a new SQL statement log instance
     *
     * @return SQL statement log
     */
    protected TgSessionSqlLog createSqlLog() {
        return new TgSessionSqlLog();
    }

    /**
     * get SQL statement log
     *
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @return SQL statement log
     */
    public TgSessionSqlLog getSqlLog(TsurugiSql ps, Object parameter) {
        var log = sqlIdMap.get(new TgSqlLogKey(ps, parameter));
        if (log == null) {
            LOG.debug("sqlLog not found in sqlIdMap. {}, {}", ps, parameter);
        }
        return log;
    }

    /**
     * put SQL statement log
     *
     * @param result SQL result
     * @param sqlLog SQL statement log
     */
    public void putSqlLog(TsurugiResult result, TgSessionSqlLog sqlLog) {
        resultMap.put(result, sqlLog);
    }

    /**
     * get SQL statement log
     *
     * @param result SQL result
     * @return SQL statement log
     */
    public TgSessionSqlLog getSqlLog(TsurugiResult result) {
        return resultMap.get(result);
    }

    /**
     * get and remove SQL statement log
     *
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param result    SQL result
     * @return SQL statement log
     */
    public TgSessionSqlLog removeSqlLog(TsurugiSql ps, Object parameter, TsurugiResult result) {
        var sqlLog = sqlIdMap.remove(new TgSqlLogKey(ps, parameter));
        resultMap.remove(result);
        return sqlLog;
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
     * set commit start time
     *
     * @param time commit start time
     */
    public void setCommitStartTime(ZonedDateTime time) {
        this.commitStartTime = time;
    }

    /**
     * set commit start time
     *
     * @return commit start time
     */
    public ZonedDateTime getCommitStartTime() {
        return this.commitStartTime;
    }

    /**
     * set commit end time
     *
     * @param time commit end time
     */
    public void setCommitEndTime(ZonedDateTime time) {
        this.commitEndTime = time;
    }

    /**
     * get commit end time
     *
     * @return commit end time
     */
    public ZonedDateTime getCommitEndTime() {
        return this.commitEndTime;
    }

    /**
     * set rollback start time
     *
     * @param time rollback start time
     */
    public void setRollbackStartTime(ZonedDateTime time) {
        this.rollbackStartTime = time;
    }

    /**
     * get rollback start time
     *
     * @return rollback start time
     */
    public ZonedDateTime getRollbackStartTime() {
        return this.rollbackStartTime;
    }

    /**
     * set rollback end time
     *
     * @param time rollback end time
     */
    public void setRollbackEndTime(ZonedDateTime time) {
        this.rollbackEndTime = time;
    }

    /**
     * get rollback end time
     *
     * @return rollback end time
     */
    public ZonedDateTime getRollbackEndTime() {
        return this.rollbackEndTime;
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
