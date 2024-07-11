package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
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
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
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
        public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
            doLogTmExecuteRetry(transaction, cause, nextTmOption);
        }

        @Override
        public void transactionRetryOver(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
            doLogTmExecuteRetry(transaction, cause, nextTmOption);
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
        public void executeStart(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter) {
            doLogTransactionSqlStart(transaction, method, iceaxeTxExecuteId, ps, parameter);
        }

        @Override
        public void executeEnd(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter, TsurugiSqlResult result, @Nullable Throwable occurred) {
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
        public void closeTransaction(TsurugiTransaction transaction, long timeoutNanos, @Nullable Throwable occurred) {
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
        public void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlQuery<Object> ps, TsurugiQueryResult<Object> result, long timeoutNanos, Throwable occurred) {
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
        public void executeQueryClose(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<Object, Object> ps, Object parameter, TsurugiQueryResult<Object> result, long timeoutNanos,
                Throwable occurred) {
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
        public void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlStatement ps, TsurugiStatementResult result, long timeoutNanos, Throwable occurred) {
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
        public void executeStatementClose(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Object parameter, TsurugiStatementResult result, long timeoutNanos,
                Throwable occurred) {
            doLogSqlClose(transaction, result, occurred);
        }

        @Override
        public void executeBatchStart(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Collection<Object> parameterList, int iceaxeSqlExecuteId) {
            doLogSqlStart(transaction, iceaxeSqlExecuteId, ps, parameterList);
        }

        @Override
        public void executeBatchStartException(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Collection<Object> parameterList, int iceaxeSqlExecuteId, Throwable occurred) {
            doLogSqlStartException(transaction, iceaxeSqlExecuteId, occurred);
        }

        @Override
        public void executeBatchStarted2(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Collection<Object> parameterList, TsurugiStatementResult result) {
            doLogSqlStarted(transaction, result);
        }

        @Override
        public void executeBatchEnd(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Collection<Object> parameterList, TsurugiStatementResult result, Throwable occurred) {
            doLogSqlEnd(transaction, result, occurred);
        }

        @Override
        public void executeBatchClose(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<Object> ps, Collection<Object> parameterList, TsurugiStatementResult result, long timeoutNanos,
                Throwable occurred) {
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
    public final void shutdownSession(TsurugiSession session, TgSessionShutdownType shutdownType, long timeoutNanos, @Nullable Throwable occurred) {
        logSessionShutdown(session, shutdownType, occurred);
    }

    @Override
    public final void closeSession(TsurugiSession session, long timeoutNanos, @Nullable Throwable occurred) {
        logSessionClose(session, occurred);
    }

    // Session

    /**
     * called when create SQL definition.
     *
     * @param ps SQL definition
     */
    protected void logCreateSqlStatement(TsurugiSql ps) {
        // do override
    }

    /**
     * called when shutdown session.
     *
     * @param session      session
     * @param shutdownType shutdown type
     * @param occurred     exception
     * @since 1.4.0
     */
    protected void logSessionShutdown(TsurugiSession session, TgSessionShutdownType shutdownType, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * called when close session.
     *
     * @param session  session
     * @param occurred exception
     */
    protected void logSessionClose(TsurugiSession session, @Nullable Throwable occurred) {
        // do override
    }

    // transactionManager.execute()

    /**
     * do transactionManager execute start.
     *
     * @param iceaxeTmExecuteId iceaxe tm executeId
     */
    protected void doLogTmExecuteStart(int iceaxeTmExecuteId) {
        var startTime = ZonedDateTime.now();

        var tmLog = createTmLog();
        tmLog.setIceaxeTmExecuteId(iceaxeTmExecuteId);
        tmLog.setStartTime(startTime);
        tmLogMap.put(iceaxeTmExecuteId, tmLog);

        logTmExecuteStart(tmLog);
    }

    /**
     * Creates a new transaction manager execute log.
     *
     * @return transaction manager execute log
     */
    protected TgSessionTmLog createTmLog() {
        return new TgSessionTmLog();
    }

    /**
     * get transaction manager execute log.
     *
     * @param iceaxeTmExecuteId iceaxe tm executeId
     * @return transaction manager execute log
     */
    protected TgSessionTmLog getTmLog(int iceaxeTmExecuteId) {
        return tmLogMap.get(iceaxeTmExecuteId);
    }

    /**
     * called when transactionManager execute start.
     *
     * @param tmLog transaction manager log
     */
    protected void logTmExecuteStart(TgSessionTmLog tmLog) {
        // do override
    }

    /**
     * do transactionManager execute error.
     *
     * @param transaction transaction
     * @param occurred    exception
     */
    protected void doLogTmExecuteException(TsurugiTransaction transaction, Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        logTmExecuteException(txLog, occurred);
    }

    /**
     * called when occurs transactionManager execute error.
     *
     * @param txLog    transaction log
     * @param occurred exception
     */
    protected void logTmExecuteException(TgSessionTxLog txLog, Throwable occurred) {
        // do override
    }

    /**
     * do transactionManager execute retry.
     *
     * @param transaction  transaction
     * @param cause        retry exception
     * @param nextTmOption next transaction option
     */
    protected void doLogTmExecuteRetry(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        logTmExecuteRetry(txLog, cause, nextTmOption);
    }

    /**
     * called when transactionManager execute retry.
     *
     * @param txLog        transaction log
     * @param cause        retry exception
     * @param nextTmOption next transaction option
     */
    protected void logTmExecuteRetry(TgSessionTxLog txLog, Exception cause, TgTmTxOption nextTmOption) {
        // do override
    }

    /**
     * do transactionManager execute end.
     *
     * @param iceaxeTmExecuteId iceaxe tm executeId
     * @param transaction       transaction
     * @param committed         {@code true} if committed
     * @param returnValue       return value
     * @param occurred          exception
     */
    protected void doLogTmExecuteEnd(int iceaxeTmExecuteId, @Nullable TsurugiTransaction transaction, boolean committed, @Nullable Object returnValue, @Nullable Throwable occurred) {
        var tmLog = tmLogMap.remove(iceaxeTmExecuteId);
        if (tmLog == null) {
            return;
        }

        tmLog.setEndTime(ZonedDateTime.now());

        logTmExecuteEnd(tmLog, occurred);
    }

    /**
     * called when transactionManager execute end.
     *
     * @param tmLog    transaction manager log
     * @param occurred exception
     */
    protected void logTmExecuteEnd(TgSessionTmLog tmLog, @Nullable Throwable occurred) {
        // do override
    }

    // Transaction

    /**
     * do transaction start.
     *
     * @param transaction transaction
     * @param startTime   start time
     */
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

    /**
     * Creates a new transaction log instance.
     *
     * @return transaction log
     */
    protected TgSessionTxLog createTxLog() {
        return new TgSessionTxLog();
    }

    /**
     * get transaction log.
     *
     * @param transaction transaction
     * @return transaction log
     */
    protected TgSessionTxLog getTxLog(TsurugiTransaction transaction) {
        var txLog = txLogMap.get(transaction.getIceaxeTxId());
        if (txLog == null) {
            LOG.debug("transaction not found in txLogMap. {}", transaction);
        }
        return txLog;
    }

    /**
     * called when transaction start.
     *
     * @param txLog transaction log
     */
    protected void logTransactionStart(TgSessionTxLog txLog) {
        // do override
    }

    /**
     * do get transactionId.
     *
     * @param transaction transaction
     */
    protected void doLowTransactionGetStart(TsurugiTransaction transaction) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setLowGetStartTime(ZonedDateTime.now());

        logLowTransactionGetStart(txLog);
    }

    /**
     * called when get transactionId.
     *
     * @param txLog transaction log
     */
    protected void logLowTransactionGetStart(TgSessionTxLog txLog) {
        // do override
    }

    /**
     * do get transactionId.
     *
     * @param transaction   transaction
     * @param transactionId transaction id
     * @param occurred      exception
     */
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
     * called when get transactionId.
     *
     * @param txLog         transaction log
     * @param transactionId transactionId
     * @param occurred      exception
     */
    protected void logLowTransactionGetEnd(TgSessionTxLog txLog, @Nullable String transactionId, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * do execute SQL start.
     *
     * @param transaction       transaction
     * @param method            execute method
     * @param iceaxeTxExecuteId iceaxe tm executeId
     * @param ps                SQL definition
     * @param parameter         SQL parameter
     */
    protected void doLogTransactionSqlStart(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        var exLog = txLog.getNewTxExecuteLog(iceaxeTxExecuteId);
        exLog.setStartTime(ZonedDateTime.now());

        logTransactionSqlStart(method, txLog, exLog, ps, parameter);
    }

    /**
     * called when execute SQL start.
     *
     * @param method    execute method
     * @param txLog     transaction log
     * @param exLog     transaction SQL execute log
     * @param ps        SQL definition
     * @param parameter SQL parameter
     */
    protected void logTransactionSqlStart(TgTxMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter) {
        // do override
    }

    /**
     * do execute SQL end.
     *
     * @param transaction       transaction
     * @param method            execute method
     * @param iceaxeTxExecuteId iceaxe tm executeId
     * @param ps                SQL definition
     * @param parameter         SQL parameter
     * @param result            SQL result
     * @param occurred          exception
     */
    protected void doLogTransactionSqlEnd(TsurugiTransaction transaction, TgTxMethod method, int iceaxeTxExecuteId, TsurugiSql ps, Object parameter, @Nullable TsurugiSqlResult result,
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
     * called when execute SQL end.
     *
     * @param method    execute method
     * @param txLog     transaction log
     * @param exLog     transaction SQL execute log
     * @param ps        SQL definition
     * @param parameter SQL parameter
     * @param result    SQL result
     * @param occurred  exception
     */
    protected void logTransactionSqlEnd(TgTxMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter, @Nullable TsurugiSqlResult result,
            @Nullable Throwable occurred) {
        // do override
    }

    /**
     * do commit start.
     *
     * @param transaction transaction
     * @param commitType  commit type
     */
    protected void doLogTransactionCommitStart(TsurugiTransaction transaction, TgCommitType commitType) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setCommitStartTime(ZonedDateTime.now());

        logTransactionCommitStart(txLog, commitType);
    }

    /**
     * called when commit start.
     *
     * @param txLog      transaction log
     * @param commitType commit type
     */
    protected void logTransactionCommitStart(TgSessionTxLog txLog, TgCommitType commitType) {
        // do override
    }

    /**
     * do commit end.
     *
     * @param transaction transaction
     * @param commitType  commit type
     * @param occurred    exception
     */
    protected void doLogTransactionCommitEnd(TsurugiTransaction transaction, TgCommitType commitType, @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setCommitEndTime(ZonedDateTime.now());

        logTransactionCommitEnd(txLog, commitType, occurred);
    }

    /**
     * called when commit end.
     *
     * @param txLog      transaction log
     * @param commitType commit type
     * @param occurred   exception
     */
    protected void logTransactionCommitEnd(TgSessionTxLog txLog, TgCommitType commitType, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * do rollback start.
     *
     * @param transaction transaction
     */
    protected void doLogTransactionRollbackStart(TsurugiTransaction transaction) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setRollbackStartTime(ZonedDateTime.now());

        logTransactionRollbackStart(txLog);
    }

    /**
     * called when rollback start.
     *
     * @param txLog transaction log
     */
    protected void logTransactionRollbackStart(TgSessionTxLog txLog) {
        // do override
    }

    /**
     * do rollback end.
     *
     * @param transaction transaction
     * @param occurred    exception
     */
    protected void doLogTransactionRollbackEnd(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setRollbackEndTime(ZonedDateTime.now());

        logTransactionRollbackEnd(txLog, occurred);
    }

    /**
     * called when rollback end.
     *
     * @param txLog    transaction log
     * @param occurred exception
     */
    protected void logTransactionRollbackEnd(TgSessionTxLog txLog, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * do transaction close.
     *
     * @param transaction transaction
     * @param occurred    exception
     */
    protected void doLogTransactionClose(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        var txLog = txLogMap.remove(transaction.getIceaxeTxId());
        if (txLog == null) {
            LOG.debug("transaction already closed {}", transaction);
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
     * called when transaction close.
     *
     * @param txLog    transaction log
     * @param occurred exception
     */
    protected void logTransactionClose(TgSessionTxLog txLog, @Nullable Throwable occurred) {
        // do override
    }

    // SQL start/result

    /**
     * do SQL start.
     *
     * @param transaction        transaction
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param ps                 SQL definition
     * @param parameter          SQL parameter
     */
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
     * called when SQL execute start.
     *
     * @param txLog  transaction log
     * @param sqlLog SQL log
     */
    protected void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog) {
        // do override
    }

    /**
     * do SQL execute start error.
     *
     * @param transaction        transaction
     * @param iceaxeSqlExecuteId iceaxe SQL executeId
     * @param occurred           exception
     */
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
     * called when occurs exception SQL execute start.
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param occurred exception
     */
    protected void logSqlStartException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, Throwable occurred) {
        // do override
    }

    /**
     * do SQL execute started.
     *
     * @param transaction transaction
     * @param result      SQL result
     */
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
     * called when SQL execute started.
     *
     * @param txLog  transaction log
     * @param sqlLog SQL log
     */
    protected void logSqlStarted(TgSessionTxLog txLog, TgSessionSqlLog sqlLog) {
        // do override
    }

    /**
     * do SQL read.
     *
     * @param <R>         record type
     * @param transaction transaction
     * @param result      SQL result
     * @param record      record
     */
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
     * called when SQL read.
     *
     * @param <R>    result type
     * @param txLog  transaction log
     * @param sqlLog SQL log
     * @param result SQL result
     * @param record record
     */
    protected <R> void logSqlRead(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiQueryResult<R> result, R record) {
        // do override
    }

    /**
     * do SQL read error.
     *
     * @param <R>         record type
     * @param transaction transaction
     * @param result      SQL result
     * @param occurred    exception
     */
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
     * called when occurs exception SQL read.
     *
     * @param <R>      record type
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param result   SQL result
     * @param occurred exception
     */
    protected <R> void logSqlReadException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiQueryResult<R> result, Throwable occurred) {
        // do override
    }

    /**
     * do SQL execute end.
     *
     * @param transaction transaction
     * @param result      SQL result
     * @param occurred    exception
     */
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
     * called when SQL execute end.
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param occurred exception
     */
    protected void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, @Nullable Throwable occurred) {
        // do override
    }

    /**
     * do SQL result close.
     *
     * @param transaction transaction
     * @param result      SQL result
     * @param occurred    exception
     */
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
     * called when SQL result close.
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param occurred exception
     */
    protected void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, @Nullable Throwable occurred) {
        // do override
    }
}
