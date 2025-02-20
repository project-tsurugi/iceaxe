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
package com.tsurugidb.iceaxe.session.event.logging.file;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import com.tsurugidb.iceaxe.session.event.logging.TsurugiSessionTxLogger;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlDirect;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiSqlResult;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction.TgTxMethod;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.util.IceaxeFileUtil;
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

    /** transaction file log config */
    protected final TsurugiSessionTxFileLogConfig config;
    private final Map<Integer, TsurugiSessionTxFileLogWriter> writerMap = new ConcurrentHashMap<>();

    /**
     * Creates a new instance.
     *
     * @param config transaction file log config
     */
    public TsurugiSessionTxFileLogger(TsurugiSessionTxFileLogConfig config) {
        this.config = config;
    }

    @Override
    protected void logCreateSqlStatement(TsurugiSql ps) {
        if (!config.writeSqlFile()) {
            return;
        }

        int ssId = ps.getIceaxeSqlId();
        String fileName = String.format("ss-%d.sql", ssId);
        var outputDir = config.outputDir().resolve("sql_statement");
        try {
            Files.createDirectories(outputDir);
            IceaxeFileUtil.writeString(outputDir.resolve(fileName), ps.getSql());
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

    /**
     * create writer.
     *
     * @param txLog transaction log
     * @return writer
     * @throws UncheckedIOException if an I/O error occurs opening or creating the file
     */
    protected TsurugiSessionTxFileLogWriter createWriter(TgSessionTxLog txLog) throws UncheckedIOException {
        var file = config.outputDir().resolve(getLogFileName(txLog));
        try {
            var outputDir = createOutputDir(file);
            var pw = createPrintWriter(config, file);
            return new TsurugiSessionTxFileLogWriter(config, outputDir, pw);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    /**
     * get log file name.
     *
     * @param txLog transaction log
     * @return file name
     */
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

    /**
     * get log directory name for transaction manager.
     *
     * @param txLog transaction log
     * @return directory name
     */
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

    /**
     * get log directory name for transaction.
     *
     * @param txLog transaction log
     * @return directory name
     */
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

    /**
     * create output directory.
     *
     * @param file file path
     * @return output directory path
     * @throws IOException if an I/O error occurs
     * @since 1.3.0
     */
    protected Path createOutputDir(Path file) throws IOException {
        var outputDir = file.getParent();
        if (outputDir != null) {
            Files.createDirectories(outputDir);
        } else {
            outputDir = Path.of(".");
        }
        return outputDir;
    }

    /**
     * create print writer.
     *
     * @param config transaction file log config
     * @param file   file path
     * @return print writer
     * @throws IOException if an I/O error occurs opening or creating the file
     * @since 1.3.0
     */
    protected PrintWriter createPrintWriter(TsurugiSessionTxFileLogConfig config, Path file) throws IOException {
        return new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8), config.autoFlush());
    }

    /**
     * get writer.
     *
     * @param txLog transaction log
     * @return writer
     */
    protected TsurugiSessionTxFileLogWriter getWriter(TgSessionTxLog txLog) {
        return writerMap.get(txLog.getTransaction().getIceaxeTxId());
    }

    @Override
    protected void logLowTransactionGetStart(TgSessionTxLog txLog) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        writer.println(TX_HEADER + " lowTransaction get start", txId);
    }

    @Override
    protected void logLowTransactionGetEnd(TgSessionTxLog txLog, String transactionId, Throwable occurred) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        var time = elapsed(txLog.getLowGetStartTime(), txLog.getLowGetEndTime());
        if (transactionId != null) {
            writer.println(TX_HEADER + " lowTransaction get end. %d[ms], transactionId=%s", txId, time, transactionId);
        }
        if (occurred != null) {
            writer.println(TX_HEADER + " lowTransaction get error. %d[ms]", txId, time);
            writer.println(occurred);
        }
    }

    @Override
    protected void logTransactionSqlStart(TgTxMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter) {
        var writer = getWriter(txLog);

        int txId = txLog.getTransaction().getIceaxeTxId();
        int txExecuteId = exLog.getIceaxeTxExecuteId();
        var methodName = method.getMethodName();
        writer.println(TX_HEADER + "[iceaxeTxExecuteId=%d] %s(sql) start", txId, txExecuteId, methodName);
    }

    @Override
    protected void logTransactionSqlEnd(TgTxMethod method, TgSessionTxLog txLog, TgSessionTxExecuteLog exLog, TsurugiSql ps, Object parameter, @Nullable TsurugiSqlResult result,
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
        int ssId = sqlLog.getIceaxeSqlDefinitionId();
        var ps = sqlLog.getSqlDefinition();
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

    /**
     * output SQL explain.
     *
     * @param sqlId           iceaxe SQL executeId
     * @param ssId            iceaxe sqlId
     * @param writer          writer
     * @param explainSupplier supplier of explain
     */
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
        int ssId = sqlLog.getIceaxeSqlDefinitionId();
        writer.println(SQL_HEADER + " sql start error", sqlId, ssId);
        writer.println(occurred);
    }

    @Override
    protected <R> void logSqlRead(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiQueryResult<R> result, R record) {
        if (!(config.writeReadRecord() || config.readProgress() >= 1)) {
            return;
        }
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlDefinitionId();
        if (config.writeReadRecord()) {
            int index = result.getReadCount() - 1;
            writer.println(SQL_HEADER + " read[%d]=%s", sqlId, ssId, index, record);
        }

        int progress = config.readProgress();
        if (progress >= 1) {
            int count = result.getReadCount();
            if (count % progress == 0) {
                writer.println(SQL_HEADER + " read progress=%d", sqlId, ssId, count);
            }
        }
    }

    @Override
    protected <R> void logSqlReadException(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, TsurugiQueryResult<R> result, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlDefinitionId();
        writer.println(SQL_HEADER + " read error", sqlId, ssId);
        writer.println(occurred);
    }

    @Override
    protected void logSqlEnd(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, @Nullable Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlDefinitionId();
        var time = elapsed(sqlLog.getStartTime(), sqlLog.getEndTime());
        var result = sqlLog.getSqlResult();
        if (result instanceof TsurugiQueryResult) {
            var rs = (TsurugiQueryResult<?>) result;
            writer.println(SQL_HEADER + " readCount=%d, hasNextRow=%s", sqlId, ssId, rs.getReadCount(), getNextRowText(rs));
        }
        if (occurred == null) {
            writer.println(SQL_HEADER + " sql end. %d[ms]", sqlId, ssId, time);
        } else {
            writer.println(SQL_HEADER + " sql error. %d[ms]", sqlId, ssId, time);
            writer.println(occurred);
        }
    }

    private String getNextRowText(TsurugiQueryResult<?> result) {
        return result.getHasNextRow().map(b -> b.toString()).orElse("unread");
    }

    @Override
    protected void logSqlClose(TgSessionTxLog txLog, TgSessionSqlLog sqlLog, Throwable occurred) {
        var writer = getWriter(txLog);

        int sqlId = sqlLog.getIceaxeSqlExecuteId();
        int ssId = sqlLog.getIceaxeSqlDefinitionId();
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
    protected void logTmExecuteRetry(TgSessionTxLog txLog, Exception cause, TgTmTxOption nextTmOption) {
        var writer = getWriter(txLog);

        var transaction = txLog.getTransaction();
        var tmExecuteId = transaction.getIceaxeTmExecuteId();
        var attempt = transaction.getAttempt();
        var nextTxOption = nextTmOption.getTransactionOption();
        var retryInstruction = nextTmOption.getRetryInstruction();
        writer.println(TM_HEADER + " tm.execute(iceaxeTmExecuteId=%d, attempt=%d) retry. nextTx=%s, instruction=%s", tmExecuteId, tmExecuteId, attempt, nextTxOption, retryInstruction);
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

    /**
     * get elapsed.
     *
     * @param start start time
     * @param end   end time
     * @return elapsed[ms]
     */
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
