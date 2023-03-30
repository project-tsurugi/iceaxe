package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.TsurugiSessionEventListener;
import com.tsurugidb.iceaxe.session.event.logging.TgSessionTxLog.TgSessionTxExecuteLog;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlPreparedQueryResultEventListener;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlPreparedStatementResultEventListener;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlQueryEventListener;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlQueryResultEventListener;
import com.tsurugidb.iceaxe.sql.event.TsurugiSqlStatementResultEventListener;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxExecuteMethod;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi transaction logger.
 */
public class TsurugiSessionTxLogger implements TsurugiSessionEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiSessionTxLogger.class);

    private final Map<Integer, TgSessionTmLog> tmLogMap = new ConcurrentHashMap<>();
    private final Map<Integer, TgSessionTxLog> txLogMap = new ConcurrentHashMap<>();

    private final TsurugiTmEventListener tmLogger = new TsurugiTmEventListener() {
        @Override
        public void executeStart(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption txOption) {
            doLogTmExecuteStart(iceaxeTmExecuteId);
        }

        @Override
        public void transactionException(TsurugiTransaction transaction, Throwable occurred) {
            doLogTmExecuteException(transaction, occurred);
        }

        @Override
        public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextTxOption) {
            doLogTmExecuteRetry(transaction, cause, nextTxOption);
        }

        @Override
        public void transactionRetryOver(TsurugiTransaction transaction, Exception cause) {
            doLogTmExecuteRetry(transaction, cause, null);
        }

        @Override
        public void executeEndSuccess(TsurugiTransaction transaction, boolean committed, Object returnValue) {
            var tmExecuteId = transaction.getIceaxeTmExecuteId();
            doLogTmExecuteEnd(tmExecuteId, transaction, committed, returnValue, null);
        }

        @Override
        public void executeEndFail(TsurugiTransactionManager tm, int iceaxeTmExecuteId, TgTxOption txOption, @Nullable TsurugiTransaction transaction, Throwable occurred) {
            doLogTmExecuteEnd(iceaxeTmExecuteId, transaction, false, null, occurred);
        }
    };

    private final TsurugiTransactionEventListener txLogger = new TsurugiTransactionEventListener() {
        @Override
        public void lowTransactionGetStart(TsurugiTransaction transaction) {
            doLowTransactionGetStart(transaction);
        }

        @Override
        public void lowTransactionGetEnd(TsurugiTransaction transaction, String transactionId, Throwable occurred) {
            doLowTransactionGetEnd(transaction, transactionId, occurred);
        }

        @Override
        public void executeStart(TsurugiTransaction transaction, TgTxExecuteMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter) {
            doLogTransactionSqlStart(transaction, method, iceaxeTxExecuteId, ps, parameter);
        }

        @Override
        public void executeEnd(TsurugiTransaction transaction, TgTxExecuteMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter, TsurugiSqlResult result,
                @Nullable Throwable occurred) {
            doLogTransactionSqlEnd(transaction, method, iceaxeTxExecuteId, ps, parameter, result, occurred);
        }

        @Override
        public void commitStart(TsurugiTransaction transaction, TgCommitType commitType) {
            doLogTransactionCommitStart(transaction, commitType);
        }

        @Override
        public void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, @Nullable Throwable occurred) {
            doLogTransactionCommitEnd(transaction, commitType, occurred);
        }

        @Override
        public void rollbackStart(TsurugiTransaction transaction) {
            doLogTransactionRollbackStart(transaction);
        }

        @Override
        public void rollbackEnd(TsurugiTransaction transaction, @Nullable Throwable occurred) {
            doLogTransactionRollbackEnd(transaction, occurred);
        }

        @Override
        public void closeTransaction(TsurugiTransaction transaction, @Nullable Throwable occurred) {
            doLogTransactionClose(transaction, occurred);
        }
    };

    private final TsurugiSqlQueryResultEventListener<Object> queryLogger = new TsurugiSqlQueryResultEventListener<>() {
        @Override
        public void executeQueryStart(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, int iceaxeSqlExecuteId) {
            doLogSqlStart(transaction, iceaxeSqlExecuteId, ps, null);
        }

        @Override
        public void executeQueryStartException(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, int iceaxeSqlExecuteId, Throwable occurred) {
            doLogSqlStartException(transaction, iceaxeSqlExecuteId, occurred);
        }

        @Override
        public void executeQueryStarted2(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, TsurugiQueryResult<Object> result) {
            doLogSqlStarted(transaction, result);
        }

        @Override
        public void executeQueryRead(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, TsurugiQueryResult<Object> result, Object record) {
            doLogSqlRead(transaction, result, record);
        }

        @Override
        public void executeQueryException(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, TsurugiQueryResult<Object> result, Throwable occurred) {
            doLogSqlReadException(transaction, result, occurred);
        }

        @Override
        public void executeQueryEnd(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, TsurugiQueryResult<Object> result) {
            doLogSqlEnd(transaction, result, null);
        }

        @Override
        public void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, TsurugiQueryResult<Object> result, Throwable occurred) {
            doLogSqlClose(transaction, result, occurred);
        }
    };

    private final TsurugiSqlPreparedQueryResultEventListener<Object, Object> preparedQueryLogger = new TsurugiSqlPreparedQueryResultEventListener<>() {
        @Override
        public void executeQueryStart(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, int iceaxeSqlExecuteId) {
            doLogSqlStart(transaction, iceaxeSqlExecuteId, ps, parameter);
        }

        @Override
        public void executeQueryStartException(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, int iceaxeSqlExecuteId, Throwable occurred) {
            doLogSqlStartException(transaction, iceaxeSqlExecuteId, occurred);
        }

        @Override
        public void executeQueryStarted2(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, TsurugiQueryResult<Object> result) {
            doLogSqlStarted(transaction, result);
        }

        @Override
        public void executeQueryRead(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, TsurugiQueryResult<Object> result, Object record) {
            doLogSqlRead(transaction, result, record);
        }

        @Override
        public void executeQueryException(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, TsurugiQueryResult<Object> result, Throwable occurred) {
            doLogSqlReadException(transaction, result, occurred);
        }

        @Override
        public void executeQueryEnd(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, TsurugiQueryResult<Object> result) {
            doLogSqlEnd(transaction, result, null);
        }

        @Override
        public void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, TsurugiQueryResult<Object> result, Throwable occurred) {
            doLogSqlClose(transaction, result, occurred);
        }
    };

    private final TsurugiSqlStatementResultEventListener statementLogger = new TsurugiSqlStatementResultEventListener() {
        @Override
        public void executeStatementStart(TsurugiTransaction transaction, TsurugiSqlStatement ps, int iceaxeSqlExecuteId) {
            doLogSqlStart(transaction, iceaxeSqlExecuteId, ps, null);
        }

        @Override
        public void executeStatementStartException(TsurugiTransaction transaction, TsurugiSqlStatement ps, int iceaxeSqlExecuteId, Throwable occurred) {
            doLogSqlStartException(transaction, iceaxeSqlExecuteId, occurred);
        }

        @Override
        public void executeStatementStarted2(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result) {
            doLogSqlStarted(transaction, result);
        }

        @Override
        public void executeStatementEnd(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result, Throwable occurred) {
            doLogSqlEnd(transaction, result, occurred);
        }

        @Override
        public void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result, Throwable occurred) {
            doLogSqlClose(transaction, result, occurred);
        }
    };

    private final TsurugiSqlPreparedStatementResultEventListener<Object> preparedStatementLogger = new TsurugiSqlPreparedStatementResultEventListener<>() {
        @Override
        public void executeStatementStart(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Object parameter, int iceaxeSqlExecuteId) {
            doLogSqlStart(transaction, iceaxeSqlExecuteId, ps, parameter);
        }

        @Override
        public void executeStatementStartException(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Object parameter, int iceaxeSqlExecuteId, Throwable occurred) {
            doLogSqlStartException(transaction, iceaxeSqlExecuteId, occurred);
        }

        @Override
        public void executeStatementStarted2(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Object parameter, TsurugiStatementResult result) {
            doLogSqlStarted(transaction, result);
        }

        @Override
        public void executeStatementEnd(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Object parameter, TsurugiStatementResult result, Throwable occurred) {
            doLogSqlEnd(transaction, result, occurred);
        }

        @Override
        public void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Object parameter, TsurugiStatementResult result, Throwable occurred) {
            doLogSqlClose(transaction, result, occurred);
        }
    };

    @Override
    public final <R> void createQuery(TsurugiSqlQuery<R> ps) {
        @SuppressWarnings("unchecked")
        var logger = (TsurugiSqlQueryEventListener<R>) this.queryLogger;
        ps.addEventListener(logger);

        logCreateSqlStatement(ps);
    }

    @Override
    public final <P, R> void createQuery(TsurugiSqlPreparedQuery<P, R> ps) {
        @SuppressWarnings("unchecked")
        var logger = (TsurugiSqlPreparedQueryResultEventListener<P, R>) this.preparedQueryLogger;
        ps.addEventListener(logger);

        logCreateSqlStatement(ps);
    }

    @Override
    public final void createStatement(TsurugiSqlStatement ps) {
        var logger = this.statementLogger;
        ps.addEventListener(logger);

        logCreateSqlStatement(ps);
    }

    @Override
    public final <P> void createStatement(TsurugiSqlPreparedStatement<P> ps) {
        @SuppressWarnings("unchecked")
        var logger = (TsurugiSqlPreparedStatementResultEventListener<P>) this.preparedStatementLogger;
        ps.addEventListener(logger);

        logCreateSqlStatement(ps);
    }

    @Override
    public final void createTransactionManager(TsurugiTransactionManager tm) {
        tm.addEventListener(tmLogger);
    }

    @Override
    public final void createTransaction(TsurugiTransaction transaction) {
        var startTime = ZonedDateTime.now();

        transaction.addEventListener(txLogger);

        doLogTransactionStart(transaction, startTime);
    }

    @Override
    public final void closeSession(TsurugiSession session, @Nullable Throwable occurred) {
        logSessionClose(session, occurred);
    }

    // Session

    /**
     * called when create SQL statement
     *
     * @param sqlStatement SQL statement
     */
    protected void logCreateSqlStatement(TsurugiSql sqlStatement) {
        // do override
    }

    /**
     * called when close session
     *
     * @param session  session
     * @param occurred exception
     */
    protected void logSessionClose(TsurugiSession session, @Nullable Throwable occurred) {
        // do override
    }

    // transactionManager.execute()

    protected void doLogTmExecuteStart(int iceaxeTmExecuteId) {
        var satrtTime = ZonedDateTime.now();

        var tmLog = createTmLog();
        tmLog.setIceaxeTmExecuteId(iceaxeTmExecuteId);
        tmLog.setStartTime(satrtTime);
        tmLogMap.put(iceaxeTmExecuteId, tmLog);

        logTmExecuteStart(tmLog);
    }

    protected TgSessionTmLog createTmLog() {
        return new TgSessionTmLog();
    }

    protected TgSessionTmLog getTmLog(int iceaxeTmExecuteId) {
        return tmLogMap.get(iceaxeTmExecuteId);
    }

    /**
     * called when start transactionManager.execute
     *
     * @param tmLog transaction manager log
     */
    protected void logTmExecuteStart(TgSessionTmLog tmLog) {
        // do override
    }

    protected void doLogTmExecuteException(TsurugiTransaction transaction, Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        logTmExecuteException(txLog, occurred);
    }

    /**
     * called when occurs transactionManager.execute exception
     *
     * @param txLog    transaction log
     * @param occurred exception
     */
    protected void logTmExecuteException(TgSessionTxLog txLog, Throwable occurred) {
        // do override
    }

    protected void doLogTmExecuteRetry(TsurugiTransaction transaction, Exception cause, @Nullable TgTxOption nextOption) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        logTmExecuteRetry(txLog, cause, nextOption);
    }

    /**
     * called when retry transaction
     *
     * @param txLog        transaction log
     * @param cause        retry exception
     * @param nextTxOption next transaction option
     */
    protected void logTmExecuteRetry(TgSessionTxLog txLog, Exception cause, @Nullable TgTxOption nextTxOption) {
        // do override
    }

    protected void doLogTmExecuteEnd(int iceaxeTmExecuteId, @Nullable TsurugiTransaction transaction, boolean committed, @Nullable Object returnValue, @Nullable Throwable occurred) {
        var tmLog = tmLogMap.remove(iceaxeTmExecuteId);
        if (tmLog == null) {
            return;
        }

        tmLog.setEndTime(ZonedDateTime.now());

        logTmExecuteEnd(tmLog, occurred);
    }

    /**
     * called when end transactionManager.execute
     *
     * @param tmLog    transaction manager log
     * @param occurred exception
     */
    protected void logTmExecuteEnd(TgSessionTmLog tmLog, @Nullable Throwable occurred) {
        // do override
    }

    // Transaction

    protected void doLogTransactionStart(TsurugiTransaction transaction, ZonedDateTime startTime) {
        var txLog = createTxLog();
        txLog.setTransaction(transaction);
        txLog.setStartTime(startTime);
        txLogMap.put(transaction.getIceaxeTxId(), txLog);

        var tmLog = getTmLog(transaction.getIceaxeTmExecuteId());
        if (tmLog != null) {
            tmLog.setCurrentTxLog(txLog);
            txLog.setTmLog(tmLog);
        }

        logTransactionStart(txLog);
    }

    protected TgSessionTxLog createTxLog() {
        return new TgSessionTxLog();
    }

    protected TgSessionTxLog getTxLog(TsurugiTransaction transaction) {
        var txLog = txLogMap.get(transaction.getIceaxeTxId());
        if (txLog == null) {
            LOG.debug("transaction not found in txLogMap. {}", transaction);
        }
        return txLog;
    }

    /**
     * called when transaction start
     *
     * @param txLog transaction log
     */
    protected void logTransactionStart(TgSessionTxLog txLog) {
        // do override
    }

    protected void doLowTransactionGetStart(TsurugiTransaction transaction) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setLowGetStartTime(ZonedDateTime.now());

        logLowTransactionGetStart(txLog);
    }

    /**
     * called when get transactionId
     *
     * @param txLog         transaction log
     * @param transactionId transactionId
     */
    protected void logLowTransactionGetStart(TgSessionTxLog txLog) {
        // do override
    }

    protected void doLowTransactionGetEnd(TsurugiTransaction transaction, String transactionId, Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setLowGetEndTime(ZonedDateTime.now());
        txLog.setTransactionId(transactionId);

        logLowTransactionGetEnd(txLog, transactionId, occurred);
    }

    /**
     * called when get transactionId
     *
     * @param txLog         transaction log
     * @param transactionId transactionId
     * @param occurred      exception
     */
    protected void logLowTransactionGetEnd(TgSessionTxLog txLog, @Nullable String transactionId, @Nullable Throwable occurred) {
        // do override
    }

    protected void doLogTransactionSqlStart(TsurugiTransaction transaction, TgTxExecuteMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        var exLog = txLog.getNewTxExecuteLog(iceaxeTxExecuteId);
        exLog.setStartTime(ZonedDateTime.now());

        logTransactionSqlStart(method, txLog, exLog, ps, parameter);
    }

    /**
     * called when start execute sql
     *
     * @param method    execute method
     * @param txLog     transaction log
     * @param exLog     transaction SQL execute log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     */
    protected void logTransactionSqlStart(TgTxExecuteMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter) {
        // do override
    }

    protected void doLogTransactionSqlEnd(TsurugiTransaction transaction, TgTxExecuteMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter, @Nullable TsurugiSqlResult result,
            @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var exLog = txLog.removeTxExecuteLog(iceaxeTxExecuteId);
        if (exLog == null) {
            return;
        }

        exLog.setEndTime(ZonedDateTime.now());

        logTransactionSqlEnd(method, txLog, exLog, ps, parameter, result, occurred);
    }

    /**
     * called when end execute sql
     *
     * @param method    execute method
     * @param txLog     transaction log
     * @param exLog     transaction SQL execute log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param result    SQL result
     * @param occurred  exception
     */
    protected void logTransactionSqlEnd(TgTxExecuteMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter, @Nullable TsurugiSqlResult result,
            @Nullable Throwable occurred) {
        // do override
    }

    protected void doLogTransactionCommitStart(TsurugiTransaction transaction, TgCommitType commitType) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setCommitStartTime(ZonedDateTime.now());

        logTransactionCommitStart(txLog, commitType);
    }

    /**
     * called when start commit
     *
     * @param txLog      transaction log
     * @param commitType commit type
     */
    protected void logTransactionCommitStart(TgSessionTxLog txLog, TgCommitType commitType) {
        // do override
    }

    protected void doLogTransactionCommitEnd(TsurugiTransaction transaction, TgCommitType commitType, @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setCommitEndTime(ZonedDateTime.now());

        logTransactionCommitEnd(txLog, commitType, occurred);
    }

    /**
     * called when end commit
     *
     * @param txLog      transaction log
     * @param commitType commit type
     * @param occurred   exception
     */
    protected void logTransactionCommitEnd(TgSessionTxLog txLog, TgCommitType commitType, @Nullable Throwable occurred) {
        // do override
    }

    protected void doLogTransactionRollbackStart(TsurugiTransaction transaction) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setRollbackStartTime(ZonedDateTime.now());

        logTransactionRollbackStart(txLog);
    }

    /**
     * called when start rollback
     *
     * @param txLog transaction log
     */
    protected void logTransactionRollbackStart(TgSessionTxLog txLog) {
        // do override
    }

    protected void doLogTransactionRollbackEnd(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setRollbackEndTime(ZonedDateTime.now());

        logTransactionRollbackEnd(txLog, occurred);
    }

    /**
     * called when end rollback
     *
     * @param txLog    transaction log
     * @param occurred exception
     */
    protected void logTransactionRollbackEnd(TgSessionTxLog txLog, @Nullable Throwable occurred) {
        // do override
    }

    protected void doLogTransactionClose(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        var txLog = txLogMap.remove(transaction.getIceaxeTxId());
        if (txLog == null) {
            LOG.debug("tranaction already closed {}", transaction);
            return;
        }

        txLog.setCloseTime(ZonedDateTime.now());

        logTransactionClose(txLog, occurred);

        var tmLog = txLog.getTmLog();
        if (tmLog != null) {
            tmLog.setCurrentTxLog(null);
        }
    }

    /**
     * called when close transaction
     *
     * @param txLog    transaction log
     * @param occurred exception
     */
    protected void logTransactionClose(TgSessionTxLog txLog, @Nullable Throwable occurred) {
        // do override
    }

    // SQL start/result

    protected void doLogSqlStart(TsurugiTransaction transaction, int iceaxeSqlExecuteId, TsurugiSql ps, Object parameter) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getNewSqlLog(iceaxeSqlExecuteId);

        sqlLog.setSqlStatement(ps, parameter);

        logSqlStart(txLog, sqlLog);
    }

    /**
     * called when start sql
     *
     * @param txLog  transaction log
     * @param sqlLog SQL log
     */
    protected void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog) {
        // do override
    }

    protected void doLogSqlStartException(TsurugiTransaction transaction, int iceaxeSqlExecuteId, Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(iceaxeSqlExecuteId);
        if (sqlLog == null) {
            return;
        }

        logSqlStartException(txLog, sqlLog, occurred);
    }

    /**
     * called when occurs exception start sql
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param occurred exception
     */
    protected void logSqlStartException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, Throwable occurred) {
        // do override
    }

    protected void doLogSqlStarted(TsurugiTransaction transaction, TsurugiSqlResult result) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(result.getIceaxeSqlExecuteId());
        if (sqlLog == null) {
            return;
        }

        sqlLog.setSqlResult(result);

        logSqlStarted(txLog, sqlLog);
    }

    /**
     * called when started sql
     *
     * @param txLog  transaction log
     * @param sqlLog SQL log
     */
    protected void logSqlStarted(TgSessionTxLog txLog, TgSessionSqlLog sqlLog) {
        // do override
    }

    protected <R> void doLogSqlRead(TsurugiTransaction transaction, TsurugiQueryResult<R> result, R record) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(result.getIceaxeSqlExecuteId());
        if (sqlLog == null) {
            return;
        }

        logSqlRead(txLog, sqlLog, result, record);
    }

    /**
     * called when read record
     *
     * @param <R>    result type
     * @param txLog  transaction log
     * @param sqlLog SQL log
     * @param ps     SQL statement
     * @param result SQL result
     * @param record record
     */
    protected <R> void logSqlRead(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiQueryResult<R> result, R record) {
        // do override
    }

    protected <R> void doLogSqlReadException(TsurugiTransaction transaction, TsurugiQueryResult<R> result, Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(result.getIceaxeSqlExecuteId());
        if (sqlLog == null) {
            return;
        }

        logSqlReadException(txLog, sqlLog, result, occurred);
    }

    /**
     * called when occurs read exception
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param result   SQL result
     * @param occurred exception
     */
    protected <R> void logSqlReadException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiQueryResult<R> result, Throwable occurred) {
        // do override
    }

    protected void doLogSqlEnd(TsurugiTransaction transaction, TsurugiSqlResult result, @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(result.getIceaxeSqlExecuteId());
        if (sqlLog == null) {
            return;
        }

        sqlLog.setEndTime(ZonedDateTime.now());

        logSqlEnd(txLog, sqlLog, occurred);
    }

    /**
     * called when end sql
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param occurred exception
     */
    protected void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, @Nullable Throwable occurred) {
        // do override
    }

    protected void doLogSqlClose(TsurugiTransaction transaction, TsurugiSqlResult result, @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.removeSqlLog(result.getIceaxeSqlExecuteId());
        if (sqlLog == null) {
            return;
        }

        sqlLog.setCloseTime(ZonedDateTime.now());

        logSqlClose(txLog, sqlLog, occurred);
    }

    /**
     * called when close SQL result
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param occurred exception
     */
    protected void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, @Nullable Throwable occurred) {
        // do override
    }
}
