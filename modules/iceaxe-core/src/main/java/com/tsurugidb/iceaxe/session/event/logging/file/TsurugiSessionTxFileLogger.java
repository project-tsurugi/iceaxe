package com.tsurugidb.iceaxe.session.event.logging.file;

import java.nio.file.Path;
import java.time.ZonedDateTime;
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
import com.tsurugidb.iceaxe.session.event.logging.TsurugiSessionTxLogger;
import com.tsurugidb.iceaxe.statement.TsurugiSql;
import com.tsurugidb.iceaxe.statement.TsurugiSqlDirect;
import com.tsurugidb.iceaxe.statement.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.function.IoSupplier;

/**
 * Tsurugi transaction logger to file.
 */
public class TsurugiSessionTxFileLogger extends TsurugiSessionTxLogger {

    /** not output explain */
    public static final int EXPLAIN_NOTHING = 0;
    /** output explain to log */
    public static final int EXPLAIN_LOG = 1;
    /** output explain to file */
    public static final int EXPLAIN_FILE = 2;
    /** output explain to log & file */
    public static final int EXPLAIN_BOTH = EXPLAIN_LOG | EXPLAIN_FILE;

    private final Path outputDir;
    private int writeExplain;
    private final Map<TgSessionTxLog, TsurugiSessionTxFileLogWriter> writerMap = new ConcurrentHashMap<>();

    /**
     * Creates a new instance.
     *
     * @param outputDir output directory
     */
    public TsurugiSessionTxFileLogger(Path outputDir) {
        this(outputDir, EXPLAIN_BOTH);
    }

    /**
     * Creates a new instance.
     *
     * @param outputDir    output directory
     * @param writeExplain write explain flag
     */
    public TsurugiSessionTxFileLogger(Path outputDir, int writeExplain) {
        this.outputDir = outputDir;
        this.writeExplain = writeExplain;
    }

    @Override
    protected void logTransactionStart(TgSessionTxLog txLog) {
        var startTime = txLog.getStartTime();
        var threadName = Thread.currentThread().getName();
        var writer = new TsurugiSessionTxFileLogWriter(startTime, threadName, outputDir);
        writerMap.put(txLog, writer);

        var transaction = txLog.getTransaction();
        var executeId = transaction.getExecuteId();
        var attempt = transaction.getAttempt();
        var txOption = transaction.getTransactionOption();
        writer.println("transaction start %s %s\nexecuteId=%d, attempt=%d, tx=%s", startTime, threadName, executeId, attempt, txOption);
    }

    protected TsurugiSessionTxFileLogWriter getWriter(TgSessionTxLog txLog) {
        return writerMap.get(txLog);
    }

    @Override
    protected void logTransactionId(TgSessionTxLog txLog, String transactionId) {
        var writer = getWriter(txLog);

        writer.println("transactionId=" + transactionId);
    }

    @Override
    protected void logTransactionSqlStart(TgSessionTxLog txLog, TsurugiSql ps, Object parameter) {
        var writer = getWriter(txLog);

        writer.println("execute(sql) start");
    }

    @Override
    protected void logSqlStartDirect(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSqlDirect ps) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getSqlId();
        writer.println("sql-%05d start\n%s", sqlId, ps.getSql());

        logSqlExplain(sqlId, writer, ps::explain);
    }

    @Override
    protected <P> void logSqlStartPrepared(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSqlPrepared<P> ps, P parameter) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getSqlId();
        writer.println("sql-%05d start\n%s\nargs=%s", sqlId, ps.getSql(), parameter);

        logSqlExplain(sqlId, writer, () -> ps.explain(parameter));
    }

    protected void logSqlExplain(int sqlId, TsurugiSessionTxFileLogWriter writer, IoSupplier<TgStatementMetadata> explainSupplier) {
        if (this.writeExplain == EXPLAIN_NOTHING) {
            return;
        }

        try {
            var explain = explainSupplier.get();

            if ((this.writeExplain & EXPLAIN_LOG) != 0) {
                var graph = explain.getLowPlanGraph();
                writer.println(graph.toString());
            }
            if ((this.writeExplain & EXPLAIN_FILE) != 0) {
                String contents = explain.getMetadataContents();
                writer.writeExplain(sqlId, contents);
            }
        } catch (Exception e) {
            writer.println("[WARN]" + TsurugiSessionTxFileLogger.class.getSimpleName() + ": explain error");
            writer.println(e);
        }
    }

    @Override
    protected <R> void logSqlReadExceptionCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResultSet<R> rs, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getSqlId();
        writer.println("sql-%05d read error", sqlId);
        writer.println(occurred);
    }

    @Override
    protected void logSqlEndCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResult result, @Nullable Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getSqlId();
        var time = elapsed(sqlLog.getStartTime(), sqlLog.getEndTime());
        if (result instanceof TsurugiResultSet) {
            writer.println("sql-%05d readCount=%d", sqlId, sqlLog.getReadCount());
        }
        if (occurred == null) {
            writer.println("sql-%05d end. %d[ms]", sqlId, time);
        } else {
            writer.println("sql-%05d error. %d[ms]", sqlId, time);
            writer.println(occurred);
        }
    }

    @Override
    protected void logSqlCloseCommon(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiSql ps, Object parameter, TsurugiResult result, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getSqlId();
        var time = elapsed(sqlLog.getEndTime(), sqlLog.getCloseTime());
        writer.println("sql-%05d close. %d[ms]", sqlId, time);
        writer.println(occurred);
    }

    @Override
    protected void logTransactionSqlEnd(TgSessionTxLog txLog, TsurugiSql ps, Object parameter, Throwable occurred) {
        var writer = getWriter(txLog);

        if (occurred == null) {
            writer.println("execute(sql) end");
        } else {
            writer.println("execute(sql) error");
            writer.println(occurred);
        }
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
        var executeId = transaction.getExecuteId();
        var attempt = transaction.getAttempt();
        writer.println("tm.execute(executeId=%d, attempt=%d) retry. nextOption=%s", executeId, attempt, nextOption);
    }

    @Override
    protected void logTransactionClose(TgSessionTxLog txLog, Throwable occurred) {
        var writer = getWriter(txLog);

        var time = elapsed(txLog.getStartTime(), txLog.getCloseTime());
        writer.println("transaction close. transaction.elapsed=%d[ms]", time);
        writer.println(occurred);

        writer.close();
        writerMap.remove(txLog);
    }

    protected long elapsed(ZonedDateTime start, ZonedDateTime end) {
        if (start != null && end != null) {
            return start.until(end, ChronoUnit.MILLIS);
        }
        return -1;
    }

    @Override
    public void closeSession(TsurugiSession session, Throwable occurred) {
        for (var writer : writerMap.values()) {
            writer.close();
        }
        writerMap.clear();
    }
}
