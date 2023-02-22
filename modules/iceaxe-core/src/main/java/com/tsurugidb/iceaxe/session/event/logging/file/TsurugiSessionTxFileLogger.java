package com.tsurugidb.iceaxe.session.event.logging.file;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.result.TsurugiResult;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.logging.TgSessionSqlLog;
import com.tsurugidb.iceaxe.session.event.logging.TgSessionTxLog;
import com.tsurugidb.iceaxe.session.event.logging.TgSessionTxLog.TgSessionTxExecuteLog;
import com.tsurugidb.iceaxe.session.event.logging.TsurugiSessionTxLogger;
import com.tsurugidb.iceaxe.statement.TsurugiSql;
import com.tsurugidb.iceaxe.statement.TsurugiSqlDirect;
import com.tsurugidb.iceaxe.statement.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxExecuteMethod;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.function.IoSupplier;

/**
 * Tsurugi transaction logger to file.
 */
public class TsurugiSessionTxFileLogger extends TsurugiSessionTxLogger {

    private static final DateTimeFormatter FILENAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSSSSS");

    protected final TsurugiSessionTxFileLogConfig config;
    private final Map<Integer, TsurugiSessionTxFileLogWriter> writerMap = new ConcurrentHashMap<>();

    /**
     * Creates a new instance.
     *
     * @param outputDir       output directory
     * @param writeExplain    write explain flag
     * @param writeReadRecord write read record
     */
    public TsurugiSessionTxFileLogger(TsurugiSessionTxFileLogConfig config) {
        this.config = config;
    }

    @Override
    protected void logTransactionStart(TgSessionTxLog txLog) {
        var startTime = txLog.getStartTime();
        var threadName = Thread.currentThread().getName();
        var writer = createWriter(txLog);
        writerMap.put(txLog.getTransaction().getIceaxeTransactionId(), writer);

        var transaction = txLog.getTransaction();
        writer.println("transaction start %s %s\n%s", startTime, threadName, transaction);
    }

    protected TsurugiSessionTxFileLogWriter createWriter(TgSessionTxLog txLog) {
        var file = config.outputDir().resolve(getLogFileName(txLog));
        return new TsurugiSessionTxFileLogWriter(config, file);
    }

    protected String getLogFileName(TgSessionTxLog txLog) {
        String txName = getLogDirNameTx(txLog);
        switch (config.subDirType()) {
        case NOTHING:
            return txName + ".log";
        case TM:
            return getLogDirNameTm(txLog) + "/" + txName + ".log";
        case TX:
        default:
            return txName + "/" + txName + ".log";
        case TM_TX:
            return getLogDirNameTm(txLog) + "/" + txName + "/" + txName + ".log";
        }
    }

    protected String getLogDirNameTm(TgSessionTxLog txLog) {
        int tmExecuteId = 0;
        ZonedDateTime startTime = null;
        var tmLog = txLog.getTmLog();
        if (tmLog != null) {
            tmExecuteId = tmLog.getIceaxeTmExecuteId();
            startTime = tmLog.getStartTime();
        }

        if (tmExecuteId == 0) {
            return "tm0";
        }

        var threadName = Thread.currentThread().getName();
        return "tm" + startTime.format(FILENAME_FORMATTER) + "." + tmExecuteId + "." + threadName;
    }

    protected String getLogDirNameTx(TgSessionTxLog txLog) {
        var startTime = txLog.getStartTime();
        var transaction = txLog.getTransaction();
        int txId = transaction.getIceaxeTransactionId();
        var label = transaction.getTransactionOption().label();
        if (label == null) {
            label = Thread.currentThread().getName();
        }
        label = label.replaceAll("\\s+", "_");

        return "tx" + startTime.format(FILENAME_FORMATTER) + "." + txId + "." + label;
    }

    protected TsurugiSessionTxFileLogWriter getWriter(TgSessionTxLog txLog) {
        return writerMap.get(txLog.getTransaction().getIceaxeTransactionId());
    }

    @Override
    protected void logTransactionId(TgSessionTxLog txLog, String transactionId) {
        var writer = getWriter(txLog);

        writer.println("transactionId=" + transactionId);
    }

    @Override
    protected void logTransactionSqlStart(TgTxExecuteMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter) {
        var writer = getWriter(txLog);

        int txExecuteId = exLog.getIceaxeTxExecuteId();
        var methodName = method.getMethodName();
        writer.println("iceaxeTxExecuteId=%d: %s(sql) start", txExecuteId, methodName);
    }

    @Override
    protected void logTransactionSqlEnd(TgTxExecuteMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter, @Nullable TsurugiResult result,
            @Nullable Throwable occurred) {
        var writer = getWriter(txLog);

        int txExecuteId = exLog.getIceaxeTxExecuteId();
        var methodName = method.getMethodName();
        var time = elapsed(exLog.getStartTime(), exLog.getEndTime());
        if (result == null) {
            if (occurred == null) {
                writer.println("iceaxeTxExecuteId=%d: %s(sql) end. %d[ms]", txExecuteId, methodName, time);
            } else {
                writer.println("iceaxeTxExecuteId=%d: %s(sql) error. %d[ms]", txExecuteId, methodName, time);
                writer.println(occurred);
            }
        } else {
            int sqlId = result.getIceaxeSqlExecuteId();
            if (occurred == null) {
                writer.println("iceaxeTxExecuteId=%d: %s(sql-%d) end. %d[ms]", txExecuteId, methodName, sqlId, time);
            } else {
                writer.println("iceaxeTxExecuteId=%d: %s(sql-%d) error. %d[ms]", txExecuteId, methodName, sqlId, time);
                writer.println(occurred);
            }
        }
    }

    @Override
    protected void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        var ps = sqlLog.getSqlStatement();
        writer.println("sql-%d: start\n%s", sqlId, ps.getSql());

        if (!ps.isPrepared()) {
            logSqlExplain(sqlId, writer, ((TsurugiSqlDirect) ps)::explain);
        } else {
            var parameter = sqlLog.getSqlParameter();
            writer.println("sql-%d: args=%s", sqlId, parameter);

            @SuppressWarnings("unchecked")
            var prepared = (TsurugiSqlPrepared<Object>) ps;
            logSqlExplain(sqlId, writer, () -> prepared.explain(parameter));
        }
    }

    protected void logSqlExplain(int sqlId, TsurugiSessionTxFileLogWriter writer, IoSupplier<TgStatementMetadata> explainSupplier) {
        int writeExplain = config.writeExplain();
        if (writeExplain == TsurugiSessionTxFileLogConfig.EXPLAIN_NOTHING) {
            return;
        }

        try {
            var explain = explainSupplier.get();

            if ((writeExplain & TsurugiSessionTxFileLogConfig.EXPLAIN_LOG) != 0) {
                var graph = explain.getLowPlanGraph();
                writer.println(graph.toString());
            }
            if ((writeExplain & TsurugiSessionTxFileLogConfig.EXPLAIN_FILE) != 0) {
                String contents = explain.getMetadataContents();
                writer.writeExplain(sqlId, contents);
            }
        } catch (Exception e) {
            writer.println("[WARN]" + TsurugiSessionTxFileLogger.class.getSimpleName() + ": explain error");
            writer.println(e);
        }
    }

    @Override
    protected void logSqlStartException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        writer.println("sql-%d: start error", sqlId);
        writer.println(occurred);
    }

    @Override
    protected <R> void logSqlRead(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiResultSet<R> rs, R record) {
        if (!config.writeReadRecord()) {
            return;
        }
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int index = rs.getReadCount() - 1;
        writer.println("sql-%d: read[%d]=%s", sqlId, index, record);
    }

    @Override
    protected <R> void logSqlReadException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiResultSet<R> rs, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        writer.println("sql-%d: read error", sqlId);
        writer.println(occurred);
    }

    @Override
    protected void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, @Nullable Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        var time = elapsed(sqlLog.getStartTime(), sqlLog.getEndTime());
        var result = sqlLog.getSqlResult();
        if (result instanceof TsurugiResultSet) {
            var rs = (TsurugiResultSet<?>) result;
            writer.println("sql-%d: readCount=%d, hasNextRow=%s", sqlId, rs.getReadCount(), rs.getHasNextRow().map(b -> b.toString()).orElse("unread"));
        }
        if (occurred == null) {
            writer.println("sql-%d: end. %d[ms]", sqlId, time);
        } else {
            writer.println("sql-%d: error. %d[ms]", sqlId, time);
            writer.println(occurred);
        }
    }

    @Override
    protected void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        var time = elapsed(sqlLog.getEndTime(), sqlLog.getCloseTime());
        writer.println("sql-%d: close. %d[ms]", sqlId, time);
        writer.println(occurred);
    }

    @Override
    protected void logTransactionCommitStart(TgSessionTxLog txLog, TgCommitType commitType) {
        var writer = getWriter(txLog);

        writer.println("commit start. commitType=" + commitType);
    }

    @Override
    protected void logTransactionCommitEnd(TgSessionTxLog txLog, TgCommitType commitType, Throwable occurred) {
        var writer = getWriter(txLog);

        var time = elapsed(txLog.getCommitStartTime(), txLog.getCommitEndTime());
        if (occurred == null) {
            writer.println("commit end. %d[ms]", time);
        } else {
            writer.println("commit error. %d[ms]", time);
            writer.println(occurred);
        }
    }

    @Override
    protected void logTransactionRollbackStart(TgSessionTxLog txLog) {
        var writer = getWriter(txLog);

        writer.println("rollback start");
    }

    @Override
    protected void logTransactionRollbackEnd(TgSessionTxLog txLog, Throwable occurred) {
        var writer = getWriter(txLog);

        var time = elapsed(txLog.getRollbackStartTime(), txLog.getRollbackEndTime());
        if (occurred == null) {
            writer.println("rollback end. %d[ms]", time);
        } else {
            writer.println("rollback error. %d[ms]", time);
            writer.println(occurred);
        }
    }

    @Override
    protected void logTmExecuteException(TgSessionTxLog txLog, Throwable occurred) {
        var writer = getWriter(txLog);

        writer.println("tm.execute error");
        writer.println(occurred);
    }

    @Override
    protected void logTmExecuteRetry(TgSessionTxLog txLog, Exception cause, TgTxOption nextOption) {
        var writer = getWriter(txLog);

        var transaction = txLog.getTransaction();
        var tmExecuteId = transaction.getIceaxeTmExecuteId();
        var attempt = transaction.getAttempt();
        writer.println("tm.execute(iceaxeTmExecuteId=%d, attempt=%d) retry. nextTx=%s", tmExecuteId, attempt, nextOption);
    }

    @Override
    protected void logTransactionClose(TgSessionTxLog txLog, Throwable occurred) {
        var writer = getWriter(txLog);

        var time = elapsed(txLog.getStartTime(), txLog.getCloseTime());
        writer.println("transaction close. transaction.elapsed=%d[ms]", time);
        writer.println(occurred);

        writer.close();
        writerMap.remove(txLog.getTransaction().getIceaxeTransactionId());
    }

    protected long elapsed(ZonedDateTime start, ZonedDateTime end) {
        if (start != null && end != null) {
            return start.until(end, ChronoUnit.MILLIS);
        }
        return -1;
    }

    @Override
    public void logSessionClose(TsurugiSession session, Throwable occurred) {
        for (var writer : writerMap.values()) {
            writer.close();
        }
        writerMap.clear();
    }
}
