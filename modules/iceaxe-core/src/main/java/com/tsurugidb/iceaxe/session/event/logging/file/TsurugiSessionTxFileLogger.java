package com.tsurugidb.iceaxe.session.event.logging.file;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.logging.TgSessionSqlLog;
import com.tsurugidb.iceaxe.session.event.logging.TgSessionTxLog;
import com.tsurugidb.iceaxe.session.event.logging.TgSessionTxLog.TgSessionTxExecuteLog;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlDirect;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.sql.result.TusurigQueryResult;
import com.tsurugidb.iceaxe.session.event.logging.TsurugiSessionTxLogger;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxExecuteMethod;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.function.IoSupplier;

/**
 * Tsurugi transaction logger to file.
 */
public class TsurugiSessionTxFileLogger extends TsurugiSessionTxLogger {

    private static final DateTimeFormatter FILENAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSSSSS");

    /** iceaxeTxId */
    private static final String TX_HEADER = "[TX-%d]";
    /** iceaxeSqlExecuteId */
    private static final String SQL_HEADER = "[sql-%d][ss-%d]";
    /** iceaxeTmExecuteId */
    private static final String TM_HEADER = "[TM-%d]";

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
    protected void logCreateSqlStatement(TsurugiSql sqlStatement) {
        if (!config.writeSqlFile()) {
            return;
        }

        int ssId = sqlStatement.getIceaxeSqlId();
        String fileName = String.format("ss-%d.sql", ssId);
        var outputDir = config.outputDir().resolve("sql_statement");
        try {
            Files.createDirectories(outputDir);
            Files.writeString(outputDir.resolve(fileName), sqlStatement.getSql(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    protected void logTransactionStart(TgSessionTxLog txLog) {
        var transaction = txLog.getTransaction();
        int txId = transaction.getIceaxeTxId();
        var startTime = txLog.getStartTime();
        var threadName = Thread.currentThread().getName();
        var writer = createWriter(txLog);
        writerMap.put(txId, writer);

        writer.println(TX_HEADER + " transaction start %s %s\n%s", txId, startTime, threadName, transaction);
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
        int txId = transaction.getIceaxeTxId();
//      var label = transaction.getTransactionOption().label();
//      if (label == null) {
//          label = Thread.currentThread().getName();
//      }
//      label = label.replaceAll("\\s+", "_");
        var label = Thread.currentThread().getName();

        return "tx" + startTime.format(FILENAME_FORMATTER) + "." + txId + "." + label;
    }

    protected TsurugiSessionTxFileLogWriter getWriter(TgSessionTxLog txLog) {
        return writerMap.get(txLog.getTransaction().getIceaxeTxId());
    }

    @Override
    protected void logTransactionId(TgSessionTxLog txLog, String transactionId) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        writer.println(TX_HEADER + " transactionId=%s", txId, transactionId);
    }

    @Override
    protected void logTransactionSqlStart(TgTxExecuteMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        int txExecuteId = exLog.getIceaxeTxExecuteId();
        var methodName = method.getMethodName();
        writer.println(TX_HEADER + "[iceaxeTxExecuteId=%d] %s(sql) start", txId, txExecuteId, methodName);
    }

    @Override
    protected void logTransactionSqlEnd(TgTxExecuteMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter, @Nullable TsurugiSqlResult result,
            @Nullable Throwable occurred) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        int txExecuteId = exLog.getIceaxeTxExecuteId();
        var methodName = method.getMethodName();
        var time = elapsed(exLog.getStartTime(), exLog.getEndTime());
        if (result == null) {
            if (occurred == null) {
                writer.println(TX_HEADER + "[iceaxeTxExecuteId=%d] %s(sql) end. %d[ms]", txId, txExecuteId, methodName, time);
            } else {
                writer.println(TX_HEADER + "[iceaxeTxExecuteId=%d] %s(sql) error. %d[ms]", txId, txExecuteId, methodName, time);
                writer.println(occurred);
            }
        } else {
            int sqlId = result.getIceaxeSqlExecuteId();
            if (occurred == null) {
                writer.println(TX_HEADER + "[iceaxeTxExecuteId=%d] %s(sql-%d) end. %d[ms]", txId, txExecuteId, methodName, sqlId, time);
            } else {
                writer.println(TX_HEADER + "[iceaxeTxExecuteId=%d] %s(sql-%d) error. %d[ms]", txId, txExecuteId, methodName, sqlId, time);
                writer.println(occurred);
            }
        }
    }

    @Override
    protected void logSqlStart(TgSessionTxLog txLog, TgSessionSqlLog sqlLog) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlStatementId();
        var ps = sqlLog.getSqlStatement();
        {
            int maxLength = config.sqlMaxLength();
            if (maxLength == 0) {
                writer.println(SQL_HEADER + " sql start", sqlId, ssId);
            } else {
                String sql = snip(ps.getSql(), maxLength);
                writer.println(SQL_HEADER + " sql start. sql=%s", sqlId, ssId, sql);
            }
        }

        if (!ps.isPrepared()) {
            logSqlExplain(sqlId, ssId, writer, ((TsurugiSqlDirect) ps)::explain);
        } else {
            var parameter = sqlLog.getSqlParameter();

            int maxLength = config.argMaxLength();
            if (maxLength != 0) {
                String args = snip(parameter, maxLength);
                writer.println(SQL_HEADER + " args=%s", sqlId, ssId, args);
            }

            @SuppressWarnings("unchecked")
            var prepared = (TsurugiSqlPrepared<Object>) ps;
            logSqlExplain(sqlId, ssId, writer, () -> prepared.explain(parameter));
        }
    }

    private String snip(Object object, int maxLength) {
        if (object == null) {
            return null;
        }
        String s = object.toString();
        if (maxLength < 0) {
            return s;
        }
        if (s.length() <= maxLength) {
            return s;
        }
        return s.substring(0, maxLength) + "...";
    }

    protected void logSqlExplain(int sqlId, int ssId, TsurugiSessionTxFileLogWriter writer, IoSupplier<TgStatementMetadata> explainSupplier) {
        int writeExplain = config.writeExplain();
        if (writeExplain == TsurugiSessionTxFileLogConfig.EXPLAIN_NOTHING) {
            return;
        }

        try {
            var explain = explainSupplier.get();

            if ((writeExplain & TsurugiSessionTxFileLogConfig.EXPLAIN_LOG) != 0) {
                var graph = explain.getLowPlanGraph();
                writer.println(SQL_HEADER + " %s", sqlId, ssId, graph);
            }
            if ((writeExplain & TsurugiSessionTxFileLogConfig.EXPLAIN_FILE) != 0) {
                String contents = explain.getMetadataContents();
                writer.writeExplain(sqlId, contents);
            }
        } catch (Exception e) {
            writer.println(SQL_HEADER + "[WARN] " + TsurugiSessionTxFileLogger.class.getSimpleName() + ": explain error", sqlId, ssId);
            writer.println(e);
        }
    }

    @Override
    protected void logSqlStartException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlStatementId();
        writer.println(SQL_HEADER + " sql start error", sqlId, ssId);
        writer.println(occurred);
    }

    @Override
    protected <R> void logSqlRead(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TusurigQueryResult<R> rs, R record) {
        if (!(config.writeReadRecord() || config.readProgress() >= 1)) {
            return;
        }
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlStatementId();
        if (config.writeReadRecord()) {
            int index = rs.getReadCount() - 1;
            writer.println(SQL_HEADER + " read[%d]=%s", sqlId, ssId, index, record);
        }

        int progress = config.readProgress();
        if (progress >= 1) {
            int count = rs.getReadCount();
            if (count % progress == 0) {
                writer.println(SQL_HEADER + " read progress=%d", sqlId, ssId, count);
            }
        }
    }

    @Override
    protected <R> void logSqlReadException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TusurigQueryResult<R> rs, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlStatementId();
        writer.println(SQL_HEADER + " read error", sqlId, ssId);
        writer.println(occurred);
    }

    @Override
    protected void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, @Nullable Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlStatementId();
        var time = elapsed(sqlLog.getStartTime(), sqlLog.getEndTime());
        var result = sqlLog.getSqlResult();
        if (result instanceof TusurigQueryResult) {
            var rs = (TusurigQueryResult<?>) result;
            writer.println(SQL_HEADER + " readCount=%d, hasNextRow=%s", sqlId, ssId, rs.getReadCount(), getNextRowText(rs));
        }
        if (occurred == null) {
            writer.println(SQL_HEADER + " sql end. %d[ms]", sqlId, ssId, time);
        } else {
            writer.println(SQL_HEADER + " sql error. %d[ms]", sqlId, ssId, time);
            writer.println(occurred);
        }
    }

    private String getNextRowText(TusurigQueryResult<?> rs) {
        return rs.getHasNextRow().map(b -> b.toString()).orElse("unread");
    }

    @Override
    protected void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlStatementId();
        var time = elapsed(sqlLog.getEndTime(), sqlLog.getCloseTime());
        writer.println(SQL_HEADER + " sql close. close.elapsed=%d[ms]", sqlId, ssId, time);
        writer.println(occurred);
    }

    @Override
    protected void logTransactionCommitStart(TgSessionTxLog txLog, TgCommitType commitType) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        writer.println(TX_HEADER + " commit start. commitType=%s", txId, commitType);
    }

    @Override
    protected void logTransactionCommitEnd(TgSessionTxLog txLog, TgCommitType commitType, Throwable occurred) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        var time = elapsed(txLog.getCommitStartTime(), txLog.getCommitEndTime());
        if (occurred == null) {
            writer.println(TX_HEADER + " commit end. %d[ms]", txId, time);
        } else {
            writer.println(TX_HEADER + " commit error. %d[ms]", txId, time);
            writer.println(occurred);
        }
    }

    @Override
    protected void logTransactionRollbackStart(TgSessionTxLog txLog) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        writer.println(TX_HEADER + " rollback start", txId);
    }

    @Override
    protected void logTransactionRollbackEnd(TgSessionTxLog txLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        var time = elapsed(txLog.getRollbackStartTime(), txLog.getRollbackEndTime());
        if (occurred == null) {
            writer.println(TX_HEADER + " rollback end. %d[ms]", txId, time);
        } else {
            writer.println(TX_HEADER + " rollback error. %d[ms]", txId, time);
            writer.println(occurred);
        }
    }

    @Override
    protected void logTmExecuteException(TgSessionTxLog txLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int tmExecuteId = txLog.getTransaction().getIceaxeTmExecuteId();
        writer.println(TM_HEADER + " tm.execute error", tmExecuteId);
        writer.println(occurred);
    }

    @Override
    protected void logTmExecuteRetry(TgSessionTxLog txLog, Exception cause, TgTxOption nextOption) {
        var writer = getWriter(txLog);

        var transaction = txLog.getTransaction();
        var tmExecuteId = transaction.getIceaxeTmExecuteId();
        var attempt = transaction.getAttempt();
        writer.println(TM_HEADER + " tm.execute(iceaxeTmExecuteId=%d, attempt=%d) retry. nextTx=%s", tmExecuteId, tmExecuteId, attempt, nextOption);
    }

    @Override
    protected void logTransactionClose(TgSessionTxLog txLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        var time = elapsed(txLog.getStartTime(), txLog.getCloseTime());
        writer.println(TX_HEADER + " transaction close. transaction.elapsed=%d[ms]", txId, time);
        writer.println(occurred);

        writer.close();
        writerMap.remove(txLog.getTransaction().getIceaxeTxId());
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
