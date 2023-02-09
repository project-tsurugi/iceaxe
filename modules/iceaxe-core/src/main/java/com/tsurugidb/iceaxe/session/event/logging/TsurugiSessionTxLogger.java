package com.tsurugidb.iceaxe.session.event.logging;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TsurugiResult;
import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.event.TsurugiSessionEventListener;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.statement.TsurugiSql;
import com.tsurugidb.iceaxe.statement.TsurugiSqlDirect;
import com.tsurugidb.iceaxe.statement.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlPreparedQueryResultEventListener;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlPreparedStatementResultEventListener;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlQueryEventListener;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlQueryResultEventListener;
import com.tsurugidb.iceaxe.statement.event.TsurugiSqlStatementResultEventListener;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
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
    private final Map<TsurugiTransaction, TgSessionTxLog> txLogMap = new ConcurrentHashMap<>();

    private final TsurugiTmEventListener tmLogger = new TsurugiTmEventListener() {
        @Override
        public void executeStart(TsurugiTransactionManager tm, int executeId, TgTxOption option) {
            doLogTmExecuteStart(executeId);
        }

        @Override
        public void transactionException(TsurugiTransaction transaction, Throwable occurred) {
            doLogTmExecuteException(transaction, occurred);
        }

        @Override
        public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextOption) {
            doLogTmExecuteRetry(transaction, cause, nextOption);
        }

        @Override
        public void transactionRetryOver(TsurugiTransaction transaction, Exception cause) {
            doLogTmExecuteRetry(transaction, cause, null);
        }

        @Override
        public void executeEndSuccess(TsurugiTransaction transaction, boolean committed, Object returnValue) {
            var executeId = transaction.getExecuteId();
            doLogTmExecuteEnd(executeId, transaction, committed, returnValue, null);
        }

        @Override
        public void executeEndFail(TsurugiTransactionManager tm, int executeId, TgTxOption option, @Nullable TsurugiTransaction transaction, Throwable occurred) {
            doLogTmExecuteEnd(executeId, transaction, false, null, occurred);
        }
    };

    private final TsurugiTransactionEventListener txLogger = new TsurugiTransactionEventListener() {
        @Override
        public void gotTransactionId(TsurugiTransaction transaction, String transactionId) {
            doLogTransactionId(transaction, transactionId);
        }

        @Override
        public void executeStart(TsurugiTransaction transaction, TsurugiSql ps, Object parameter) {
            doLogTransactionSqlStart(transaction, ps, parameter);
        }

        @Override
        public void executeEnd(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, @Nullable Throwable occurred) {
            doLogTransactionSqlEnd(transaction, ps, parameter, occurred);
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
        public void executeQueryStart(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<Object> ps) {
            doLogSqlStart(transaction, ps);
        }

        @Override
        public void executeQueryRead(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<Object> ps, TsurugiResultSet<Object> rs, Object record) {
            doLogSqlRead(transaction, ps, rs, record);
        }

        @Override
        public void executeQueryException(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<Object> ps, TsurugiResultSet<Object> rs, Throwable occurred) {
            doLogSqlReadException(transaction, ps, rs, occurred);
        }

        @Override
        public void executeQueryEnd(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<Object> ps, TsurugiResultSet<Object> rs) {
            doLogSqlEnd(transaction, ps, rs);
        }

        @Override
        public void executeQueryClose(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<Object> ps, TsurugiResultSet<Object> rs, Throwable occurred) {
            doLogSqlClose(transaction, ps, rs, occurred);
        }
    };

    private final TsurugiSqlPreparedQueryResultEventListener<Object, Object> preparedQueryLogger = new TsurugiSqlPreparedQueryResultEventListener<>() {
        @Override
        public void executeQueryStart(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<Object, Object> ps, Object parameter) {
            doLogSqlStart(transaction, ps, parameter);
        }

        @Override
        public void executeQueryRead(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<Object, Object> ps, Object parameter, TsurugiResultSet<Object> rs, Object record) {
            doLogSqlRead(transaction, ps, parameter, rs, record);
        }

        @Override
        public void executeQueryException(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<Object, Object> ps, Object parameter, TsurugiResultSet<Object> rs, Throwable occurred) {
            doLogSqlReadException(transaction, ps, parameter, rs, occurred);
        }

        @Override
        public void executeQueryEnd(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<Object, Object> ps, Object parameter, TsurugiResultSet<Object> rs) {
            doLogSqlEnd(transaction, ps, parameter, rs);
        }

        @Override
        public void executeQueryClose(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<Object, Object> ps, Object parameter, TsurugiResultSet<Object> rs, Throwable occurred) {
            doLogSqlClose(transaction, ps, parameter, rs, occurred);
        }
    };

    private final TsurugiSqlStatementResultEventListener statementLogger = new TsurugiSqlStatementResultEventListener() {
        @Override
        public void executeStatementStart(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps) {
            doLogSqlStart(transaction, ps);
        }

        @Override
        public void executeStatementEnd(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, Throwable occurred) {
            doLogSqlEnd(transaction, ps, rc, occurred);
        }

        @Override
        public void executeStatementClose(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, Throwable occurred) {
            doLogSqlClose(transaction, ps, rc, occurred);
        }
    };

    private final TsurugiSqlPreparedStatementResultEventListener<Object> preparedStatementLogger = new TsurugiSqlPreparedStatementResultEventListener<>() {
        @Override
        public void executeStatementStart(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<Object> ps, Object parameter) {
            doLogSqlStart(transaction, ps, parameter);
        }

        @Override
        public void executeStatementEnd(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<Object> ps, Object parameter, TsurugiResultCount rc, Throwable occurred) {
            logSqlEnd(transaction, ps, parameter, rc, occurred);
        }

        @Override
        public void executeStatementClose(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<Object> ps, Object parameter, TsurugiResultCount rc, Throwable occurred) {
            doLogSqlClose(transaction, ps, parameter, rc, occurred);
        }
    };

    @Override
    public <R> void createQuery(TsurugiPreparedStatementQuery0<R> ps) {
        @SuppressWarnings("unchecked")
        var logger = (TsurugiSqlQueryEventListener<R>) this.queryLogger;
        ps.addEventListener(logger);
    }

    @Override
    public <P, R> void createQuery(TsurugiPreparedStatementQuery1<P, R> ps) {
        @SuppressWarnings("unchecked")
        var logger = (TsurugiSqlPreparedQueryResultEventListener<P, R>) this.preparedQueryLogger;
        ps.addEventListener(logger);
    }

    @Override
    public void createStatement(TsurugiPreparedStatementUpdate0 ps) {
        var logger = this.statementLogger;
        ps.addEventListener(logger);
    }

    @Override
    public <P> void createStatement(TsurugiPreparedStatementUpdate1<P> ps) {
        @SuppressWarnings("unchecked")
        var logger = (TsurugiSqlPreparedStatementResultEventListener<P>) this.preparedStatementLogger;
        ps.addEventListener(logger);
    }

    @Override
    public void createTransactionManager(TsurugiTransactionManager tm) {
        tm.addEventListener(tmLogger);
    }

    @Override
    public void createTransaction(TsurugiTransaction transaction) {
        var startTime = ZonedDateTime.now();

        transaction.addEventListener(txLogger);

        doLogTransactionStart(transaction, startTime);
    }

    protected void doLogTmExecuteStart(int executeId) {
        var satrtTime = ZonedDateTime.now();

        var tmLog = createTmLog();
        tmLog.setExecuteId(executeId);
        tmLog.setStartTime(satrtTime);
        tmLogMap.put(executeId, tmLog);

        logTmExecuteStart(tmLog);
    }

    protected TgSessionTmLog createTmLog() {
        return new TgSessionTmLog();
    }

    protected TgSessionTmLog getTmLog(int executeId) {
        return tmLogMap.get(executeId);
    }

    /**
     * called when start transactionManager.execute
     *
     * @param tmLog transaction manager log
     */
    protected void logTmExecuteStart(TgSessionTmLog tmLog) {
        // do override
    }

    protected void doLogTransactionStart(TsurugiTransaction transaction, ZonedDateTime startTime) {
        var txLog = createTxLog();
        txLog.setTransaction(transaction);
        txLog.setStartTime(startTime);
        txLogMap.put(transaction, txLog);

        var tmLog = getTmLog(transaction.getExecuteId());
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
        var txLog = txLogMap.get(transaction);
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

    protected void doLogTransactionId(TsurugiTransaction transaction, String transactionId) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        txLog.setTransactionId(transactionId);

        logTransactionId(txLog, transactionId);
    }

    /**
     * called when get transactionId
     *
     * @param txLog         transaction log
     * @param transactionId transactionId
     */
    protected void logTransactionId(TgSessionTxLog txLog, String transactionId) {
        // do override
    }

    protected void doLogTransactionSqlStart(TsurugiTransaction transaction, TsurugiSql ps, Object parameter) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }

        logTransactionSqlStart(txLog, ps, parameter);
    }

    /**
     * called when start execute sql
     *
     * @param txLog     transaction log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     */
    protected void logTransactionSqlStart(TgSessionTxLog txLog, TsurugiSql ps, Object parameter) {
        // do override
    }

    protected <R> void doLogSqlStart(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps) {
        Object parameter = null;
        doLogSqlStartMain(transaction, ps, parameter, (t, s) -> logSqlStart(t, s, ps));
    }

    protected <P, R> void doLogSqlStart(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter) {
        doLogSqlStartMain(transaction, ps, parameter, (t, s) -> logSqlStart(t, s, ps, parameter));
    }

    protected void doLogSqlStart(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps) {
        Object parameter = null;
        doLogSqlStartMain(transaction, ps, parameter, (t, s) -> logSqlStart(t, s, ps));
    }

    protected <P> void doLogSqlStart(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter) {
        doLogSqlStartMain(transaction, ps, parameter, (t, s) -> logSqlStart(t, s, ps, parameter));
    }

    protected void doLogSqlStartMain(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, BiConsumer<TgSessionTxLog, TgSessionSqlLog> action) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getNewSqlLog(ps, parameter);

        action.accept(txLog, sqlLog);
    }

    /**
     * called when start sql
     *
     * @param <R>    result type
     * @param txLog  transaction log
     * @param sqlLog SQL log
     * @param ps     SQL statement
     */
    protected <R> void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery0<R> ps) {
        // do override
        logSqlStartDirect(txLog, sqlLog, ps);
    }

    /**
     * called when start sql
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     */
    protected <P, R> void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery1<P, R> ps, P parameter) {
        // do override
        logSqlStartPrepared(txLog, sqlLog, ps, parameter);
    }

    /**
     * called when start sql
     *
     * @param txLog  transaction log
     * @param sqlLog SQL log
     * @param ps     SQL statement
     */
    protected void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementUpdate0 ps) {
        // do override
        logSqlStartDirect(txLog, sqlLog, ps);
    }

    /**
     * called when start sql
     *
     * @param <P>       parameter type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     */
    protected <P> void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementUpdate1<P> ps, P parameter) {
        // do override
        logSqlStartPrepared(txLog, sqlLog, ps, parameter);
    }

    /**
     * called when start sql
     *
     * @param txLog  transaction log
     * @param sqlLog SQL log
     * @param ps     SQL statement
     */
    protected void logSqlStartDirect(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSqlDirect ps) {
        // do override
        Object parameter = null;
        logSqlStartCommon(txLog, sqlLog, ps, parameter);
    }

    /**
     * called when start sql
     *
     * @param <P>       parameter type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     */
    protected <P> void logSqlStartPrepared(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSqlPrepared<P> ps, P parameter) {
        // do override
        logSqlStartCommon(txLog, sqlLog, ps, parameter);
    }

    /**
     * called when start sql
     *
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     */
    protected void logSqlStartCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter) {
        // do override
    }

    protected <R> void doLogSqlRead(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, R record) {
        Object parameter = null;
        doLogSqlReadMain(transaction, ps, parameter, rs, (t, s) -> logSqlRead(t, s, ps, rs, record));
    }

    protected <P, R> void doLogSqlRead(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs, R record) {
        doLogSqlReadMain(transaction, ps, parameter, rs, (t, s) -> logSqlRead(t, s, ps, parameter, rs, record));
    }

    protected <R> void doLogSqlReadMain(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, TsurugiResultSet<R> rs, BiConsumer<TgSessionTxLog, TgSessionSqlLog> action) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(rs);
        if (sqlLog == null) {
            sqlLog = txLog.getSqlLog(ps, parameter);
            if (sqlLog == null) {
                return;
            }
            txLog.putSqlLog(rs, sqlLog);
        }

        sqlLog.incrementReadCount();

        action.accept(txLog, sqlLog);
    }

    /**
     * called when read record
     *
     * @param <R>    result type
     * @param txLog  transaction log
     * @param sqlLog SQL log
     * @param ps     SQL statement
     * @param rs     ResultSet
     * @param record record
     */
    protected <R> void logSqlRead(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, R record) {
        // do override
        Object parameter = null;
        logSqlReadCommon(txLog, sqlLog, ps, parameter, rs, record);
    }

    /**
     * called when read record
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rs        ResultSet
     * @param record    record
     */
    protected <P, R> void logSqlRead(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs, R record) {
        // do override
        logSqlReadCommon(txLog, sqlLog, ps, parameter, rs, record);
    }

    /**
     * called when read record
     *
     * @param <R>       result type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rs        ResultSet
     * @param record    record
     */
    protected <R> void logSqlReadCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResultSet<R> rs, R record) {
        // do override
    }

    protected <R> void doLogSqlReadException(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, Throwable occurred) {
        Object parameter = null;
        doLogSqlReadExceptionMain(transaction, ps, parameter, rs, (t, s) -> logSqlReadException(t, s, ps, rs, occurred));
    }

    protected <P, R> void doLogSqlReadException(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs, Throwable occurred) {
        doLogSqlReadExceptionMain(transaction, ps, parameter, rs, (t, s) -> logSqlReadException(t, s, ps, parameter, rs, occurred));
    }

    protected <R> void doLogSqlReadExceptionMain(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, TsurugiResultSet<R> rs, BiConsumer<TgSessionTxLog, TgSessionSqlLog> action) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(ps, parameter);
        if (sqlLog == null) {
            return;
        }

        action.accept(txLog, sqlLog);
    }

    /**
     * called when occurs read exception
     *
     * @param <R>      result type
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param ps       SQL statement
     * @param rs       ResultSet
     * @param occurred exception
     */
    protected <R> void logSqlReadException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, Throwable occurred) {
        // do override
        Object parameter = null;
        logSqlReadExceptionCommon(txLog, sqlLog, ps, parameter, rs, occurred);
    }

    /**
     * called when occurs read exception
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rs        ResultSet
     * @param occurred  exception
     */
    protected <P, R> void logSqlReadException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs, Throwable occurred) {
        // do override
        logSqlReadExceptionCommon(txLog, sqlLog, ps, parameter, rs, occurred);
    }

    /**
     * called when occurs read exception
     *
     * @param <R>
     * @param <R>       result type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rs        ResultSet
     * @param occurred  exception
     */
    protected <R> void logSqlReadExceptionCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResultSet<R> rs, Throwable occurred) {
        // do override
    }

    protected <R> void doLogSqlEnd(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs) {
        Object parameter = null;
        doLogSqlEndMain(transaction, ps, parameter, (t, s) -> logSqlEnd(t, s, ps, rs));
    }

    protected <P, R> void doLogSqlEnd(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs) {
        doLogSqlEndMain(transaction, ps, parameter, (t, s) -> logSqlEnd(t, s, ps, parameter, rs));
    }

    protected void doLogSqlEnd(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, @Nullable Throwable occurred) {
        Object parameter = null;
        doLogSqlEndMain(transaction, ps, parameter, (t, s) -> logSqlEnd(t, s, ps, rc, occurred));
    }

    protected <P> void logSqlEnd(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc, @Nullable Throwable occurred) {
        doLogSqlEndMain(transaction, ps, parameter, (t, s) -> logSqlEnd(t, s, ps, parameter, rc, occurred));
    }

    protected <R> void doLogSqlEndMain(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, BiConsumer<TgSessionTxLog, TgSessionSqlLog> action) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.getSqlLog(ps, parameter);
        if (sqlLog == null) {
            return;
        }

        sqlLog.setEndTime(ZonedDateTime.now());

        action.accept(txLog, sqlLog);
    }

    /**
     * called when end sql
     *
     * @param <R>    result type
     * @param txLog  transaction log
     * @param sqlLog SQL log
     * @param ps     SQL statement
     * @param rs     ResultSet
     */
    protected <R> void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs) {
        // do override
        logSqlEndDirect(txLog, sqlLog, ps, rs, null);
    }

    /**
     * called when end sql
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rs        ResultSet
     */
    protected <P, R> void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs) {
        // do override
        logSqlEndPrepared(txLog, sqlLog, ps, parameter, rs, null);
    }

    /**
     * called when end sql
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param ps       SQL statement
     * @param rc       ResultCount
     * @param occurred exception
     */
    protected void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, @Nullable Throwable occurred) {
        // do override
        logSqlEndDirect(txLog, sqlLog, ps, rc, occurred);
    }

    /**
     * called when end sql
     *
     * @param <P>       parameter type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rc        ResultCount
     * @param occurred  exception
     */
    protected <P> void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc, @Nullable Throwable occurred) {
        // do override
        logSqlEndPrepared(txLog, sqlLog, ps, parameter, rc, occurred);
    }

    /**
     * called when end sql
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param ps       SQL statement
     * @param result   SQL result
     * @param occurred exception
     */
    protected void logSqlEndDirect(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSqlDirect ps, TsurugiResult result, @Nullable Throwable occurred) {
        // do override
        Object parameter = null;
        logSqlEndCommon(txLog, sqlLog, ps, parameter, result, occurred);
    }

    /**
     * called when end sql
     *
     * @param <P>       parameter type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param result    SQL result
     * @param occurred  exception
     */
    protected <P> void logSqlEndPrepared(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSqlPrepared<P> ps, P parameter, TsurugiResult result, @Nullable Throwable occurred) {
        // do override
        logSqlEndCommon(txLog, sqlLog, ps, parameter, result, occurred);
    }

    /**
     * called when end sql
     *
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param result    SQL result
     * @param occurred  exception
     */
    protected void logSqlEndCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResult result, @Nullable Throwable occurred) {
        // do override
    }

    protected <R> void doLogSqlClose(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, Throwable occurred) {
        Object parameter = null;
        doLogSqlCloseMain(transaction, ps, parameter, rs, (t, s) -> logSqlClose(t, s, ps, rs, occurred));
    }

    protected <P, R> void doLogSqlClose(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs, Throwable occurred) {
        doLogSqlCloseMain(transaction, ps, parameter, rs, (t, s) -> logSqlClose(t, s, ps, parameter, rs, occurred));
    }

    protected void doLogSqlClose(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, Throwable occurred) {
        Object parameter = null;
        doLogSqlCloseMain(transaction, ps, parameter, rc, (t, s) -> logSqlClose(t, s, ps, rc, occurred));
    }

    protected <P> void doLogSqlClose(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc, Throwable occurred) {
        doLogSqlCloseMain(transaction, ps, parameter, rc, (t, s) -> logSqlClose(t, s, ps, parameter, rc, occurred));
    }

    protected void doLogSqlCloseMain(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, TsurugiResult result, BiConsumer<TgSessionTxLog, TgSessionSqlLog> action) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
        var sqlLog = txLog.removeSqlLog(ps, parameter, result);
        if (sqlLog == null) {
            return;
        }

        sqlLog.setCloseTime(ZonedDateTime.now());

        action.accept(txLog, sqlLog);
    }

    /**
     * called when close ResultSet
     *
     * @param <R>      result type
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param ps       SQL statement
     * @param rs       ResultSet
     * @param occurred exception
     */
    protected <R> void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery0<R> ps, TsurugiResultSet<R> rs, Throwable occurred) {
        // do override
        logSqlCloseDirect(txLog, sqlLog, ps, rs, occurred);
    }

    /**
     * called when close ResultSet
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rs        ResultSet
     * @param occurred  exception
     */
    protected <P, R> void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementQuery1<P, R> ps, P parameter, TsurugiResultSet<R> rs, Throwable occurred) {
        // do override
        logSqlClosePrepared(txLog, sqlLog, ps, parameter, rs, occurred);
    }

    /**
     * called when close ResultCount
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param ps       SQL statement
     * @param rc       ResultCount
     * @param occurred exception
     */
    protected void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementUpdate0 ps, TsurugiResultCount rc, Throwable occurred) {
        // do override
        logSqlCloseDirect(txLog, sqlLog, ps, rc, occurred);
    }

    /**
     * called when close ResultClose
     *
     * @param <P>       parameter type
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param rc        ResultCount
     * @param occurred  exception
     */
    protected <P> void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiPreparedStatementUpdate1<P> ps, P parameter, TsurugiResultCount rc, Throwable occurred) {
        // do override
        logSqlClosePrepared(txLog, sqlLog, ps, parameter, rc, occurred);
    }

    /**
     * called when close SQL result
     *
     * @param txLog    transaction log
     * @param sqlLog   SQL log
     * @param ps       SQL statement
     * @param result   SQL result
     * @param occurred exception
     */
    protected void logSqlCloseDirect(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, TsurugiResult result, Throwable occurred) {
        // do override
        Object parameter = null;
        logSqlCloseCommon(txLog, sqlLog, ps, parameter, result, occurred);
    }

    /**
     * called when close SQL result
     *
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param result    SQL result
     * @param occurred  exception
     */
    protected void logSqlClosePrepared(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResult result, Throwable occurred) {
        // do override
        logSqlCloseCommon(txLog, sqlLog, ps, parameter, result, occurred);
    }

    /**
     * called when close SQL result
     *
     * @param txLog     transaction log
     * @param sqlLog    SQL log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param result    SQL result
     * @param occurred  exception
     */
    protected void logSqlCloseCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResult result, Throwable occurred) {
        // do override
    }

    protected void doLogTransactionSqlEnd(TsurugiTransaction transaction, TsurugiSql ps, Object parameter, @Nullable Throwable occurred) {
        var txLog = getTxLog(transaction);
        if (txLog == null) {
            return;
        }
//      var sqlLog = txLog.getSqlLog(ps, parameter); // sqlLogは doLogSqlCloseMainで txLogから削除されている

        logTransactionSqlEnd(txLog, ps, parameter, occurred);
    }

    /**
     * called when end execute sql
     *
     * @param txLog     transaction log
     * @param ps        SQL statement
     * @param parameter SQL parameter
     * @param occurred  exception
     */
    protected void logTransactionSqlEnd(TgSessionTxLog txLog, TsurugiSql ps, Object parameter, @Nullable Throwable occurred) {
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
     * @param txLog      transaction log
     * @param cause      retry exception
     * @param nextOption next transaction option
     */
    protected void logTmExecuteRetry(TgSessionTxLog txLog, Exception cause, @Nullable TgTxOption nextOption) {
        // do override
    }

    protected void doLogTransactionClose(TsurugiTransaction transaction, @Nullable Throwable occurred) {
        var txLog = txLogMap.remove(transaction);
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

    protected void doLogTmExecuteEnd(int executeId, @Nullable TsurugiTransaction transaction, boolean committed, @Nullable Object returnValue, @Nullable Throwable occurred) {
        var tmLog = tmLogMap.remove(executeId);
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
}
