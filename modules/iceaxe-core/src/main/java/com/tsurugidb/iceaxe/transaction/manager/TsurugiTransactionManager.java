package com.tsurugidb.iceaxe.transaction.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionTask;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.InterruptedRuntimeException;
import com.tsurugidb.iceaxe.util.function.TsurugiTransactionConsumer;

/**
 * Tsurugi Transaction Manager
 * <p>
 * Thread Safe (excluding setTimeout)
 * </p>
 */
@ThreadSafe
public class TsurugiTransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTransactionManager.class);

    private static final AtomicInteger EXECUTE_COUNT = new AtomicInteger(0);

    private final TsurugiSession ownerSession;
    private final TgTmSetting defaultSetting;
    private List<TsurugiTmEventListener> eventListenerList = null;

    // internal
    public TsurugiTransactionManager(TsurugiSession session, TgTmSetting defaultSetting) {
        this.ownerSession = session;
        this.defaultSetting = defaultSetting;
    }

    /**
     * get session
     *
     * @return session
     */
    @Nonnull
    public TsurugiSession getSession() {
        return this.ownerSession;
    }

    protected final TgTmSetting defaultSetting() {
        if (this.defaultSetting == null) {
            throw new IllegalStateException("defaultSetting is not specified");
        }
        return this.defaultSetting;
    }

    /**
     * add event listener
     *
     * @param listener event listener
     * @return this
     */
    public TsurugiTransactionManager addEventListener(TsurugiTmEventListener listener) {
        if (this.eventListenerList == null) {
            this.eventListenerList = new ArrayList<>();
        }
        eventListenerList.add(listener);
        return this;
    }

    private void event(TgTmSetting setting, Throwable occurred, Consumer<TsurugiTmEventListener> action) {
        try {
            if (this.eventListenerList != null) {
                for (var listener : eventListenerList) {
                    action.accept(listener);
                }
            }
            var settingListener = setting.getEventListener();
            if (settingListener != null) {
                for (var listener : settingListener) {
                    action.accept(listener);
                }
            }
        } catch (Throwable e) {
            if (occurred != null) {
                e.addSuppressed(occurred);
            }
            throw e;
        }
    }

    /**
     * execute transaction
     *
     * @param action action
     * @throws IOException
     * @throws InterruptedException
     * @see TsurugiSql
     */
    public void execute(TsurugiTransactionAction action) throws IOException, InterruptedException {
        execute(defaultSetting(), action);
    }

    /**
     * execute transaction
     *
     * @param setting transaction manager settings
     * @param action  action
     * @throws IOException
     * @throws InterruptedException
     * @see TsurugiSql
     */
    public void execute(TgTmSetting setting, TsurugiTransactionAction action) throws IOException, InterruptedException {
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }
        execute(setting, transaction -> {
            action.run(transaction);
            return null;
        });
    }

    /**
     * execute transaction
     *
     * @param <R>    return type
     * @param action action
     * @return return value (null if transaction is rollbacked)
     * @throws IOException
     * @throws InterruptedException
     * @see TsurugiSql
     */
    public <R> R execute(TsurugiTransactionTask<R> action) throws IOException, InterruptedException {
        return execute(defaultSetting(), action);
    }

    /**
     * execute transaction
     *
     * @param <R>     return type
     * @param setting transaction manager settings
     * @param action  action
     * @return return value (null if transaction is rollbacked)
     * @throws IOException
     * @throws InterruptedException
     * @see TsurugiSql
     */
    public <R> R execute(TgTmSetting setting, TsurugiTransactionTask<R> action) throws IOException, InterruptedException {
        LOG.trace("tm.execute start");
        if (setting == null) {
            throw new IllegalArgumentException("setting is not specified");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }

        final int tmExecuteId = EXECUTE_COUNT.incrementAndGet();

        var txOption = setting.getFirstTransactionOption();
        {
            var finalTxOption = txOption;
            event(setting, null, listener -> listener.executeStart(this, tmExecuteId, finalTxOption));
        }
        for (int attempt = 0;; attempt++) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("tm.execute iceaxeTmExecuteId={}, attempt={}, tx={}", tmExecuteId, attempt, txOption);
            }

            final int finalAttempt = attempt;
            final var finalTxOption = txOption;
            event(setting, null, listener -> listener.transactionStart(this, tmExecuteId, finalAttempt, finalTxOption));

            TsurugiTransaction lastTransaction = null;
            try (var transaction = ownerSession.createTransaction(txOption, tx -> {
                tx.setOwner(this, tmExecuteId, finalAttempt);
                setting.initializeTransaction(tx);
            })) {
                lastTransaction = transaction;
                event(setting, null, listener -> listener.transactionStarted(transaction));

                try {
                    R r = action.run(transaction);
                    if (transaction.isRollbacked()) {
                        LOG.trace("tm.execute end (rollbacked)");
                        event(setting, null, listener -> listener.executeEndSuccess(transaction, false, r));
                        return r;
                    }
                    var sessionOption = ownerSession.getSessionOption();
                    var commitType = setting.getCommitType(sessionOption);
                    transaction.commit(commitType);
                    LOG.trace("tm.execute end (committed)");
                    event(setting, null, listener -> listener.executeEndSuccess(transaction, true, r));
                    return r;
                } catch (TsurugiTransactionException e) {
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    txOption = processTransactionException(setting, transaction, e, txOption, e);
                    continue;
                } catch (TsurugiTransactionRuntimeException e) {
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    var c = e.getCause();
                    txOption = processTransactionException(setting, transaction, e, txOption, c);
                    continue;
                } catch (Exception e) {
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    var c = findTransactionException(e);
                    if (c == null) {
                        LOG.trace("tm.execute error", e);
                        rollback(setting, transaction, e);
                        if (e instanceof InterruptedRuntimeException) {
                            throw ((InterruptedRuntimeException) e).getCause();
                        }
                        throw e;
                    }
                    txOption = processTransactionException(setting, transaction, e, txOption, c);
                    continue;
                } catch (Throwable e) {
                    LOG.trace("tm.execute error", e);
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    rollback(setting, transaction, e);
                    throw e;
                }
            } catch (Throwable e) {
                {
                    var finalTransaction = lastTransaction;
                    event(setting, e, listener -> listener.executeEndFail(this, tmExecuteId, finalTxOption, finalTransaction, e));
                }
                throw e;
            }
        }
    }

    private TsurugiTransactionException findTransactionException(Exception e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TsurugiTransactionException) {
                return (TsurugiTransactionException) t;
            }
        }
        return null;
    }

    private TgTxOption processTransactionException(TgTmSetting setting, TsurugiTransaction transaction, Exception cause, TgTxOption txOption, TsurugiTransactionException e) throws IOException {
        boolean calledRollback = false;
        try {
            int attempt = transaction.getAttempt();

            TgTmTxOption nextTmOption;
            try {
                nextTmOption = setting.getTransactionOption(attempt + 1, transaction, e);
            } catch (Throwable t) {
                t.addSuppressed(cause);
                throw t;
            }

            if (nextTmOption.isExecute()) {
                try {
                    // リトライ可能なabortの場合でもrollbackは呼ぶ
                    rollback(setting, transaction, null);
                } finally {
                    calledRollback = true;
                }

                var nextOption = nextTmOption.getTransactionOption();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("tm.execute retry{}. e={}, nextTx={}", attempt + 1, e.getMessage(), nextOption);
                }
                event(setting, cause, listener -> listener.transactionRetry(transaction, cause, nextOption));
                return nextOption;
            }

            LOG.trace("tm.execute error", e);
            if (nextTmOption.isRetryOver()) {
                event(setting, cause, listener -> listener.transactionRetryOver(transaction, cause));
                throw new TsurugiTransactionRetryOverIOException(transaction, cause);
            } else {
                event(setting, cause, listener -> listener.transactionNotRetryable(transaction, cause));
                throw new TsurugiTransactionIOException(cause.getMessage(), transaction, cause);
            }
        } catch (Throwable t) {
            if (!calledRollback) {
                rollback(setting, transaction, t);
            }
            throw t;
        }
    }

    private void rollback(TgTmSetting setting, TsurugiTransaction transaction, Throwable save) throws IOException {
        try {
            if (transaction.available()) {
                transaction.rollback();
                event(setting, null, listener -> listener.transactionRollbacked(transaction, save));
            }
        } catch (IOException | RuntimeException | Error e) {
            if (save != null) {
                save.addSuppressed(e);
            } else {
                throw e;
            }
        } catch (Throwable e) {
            if (save != null) {
                save.addSuppressed(e);
            } else {
                throw new IOException(e.getMessage(), e);
            }
        }
    }

    // execute statement

    /**
     * execute ddl.
     *
     * @param sql DDL
     * @throws IOException
     * @throws InterruptedException
     */
    public void executeDdl(String sql) throws IOException, InterruptedException {
        var setting = (this.defaultSetting != null) ? this.defaultSetting : TgTmSetting.of(TgTxOption.ofLTX().label("iceaxe ddl"));
        execute(setting, transaction -> {
            transaction.executeDdl(sql);
        });
    }

    /**
     * execute ddl.
     *
     * @param setting transaction manager settings
     * @param sql     DDL
     * @throws IOException
     * @throws InterruptedException
     */
    public void executeDdl(TgTmSetting setting, String sql) throws IOException, InterruptedException {
        execute(setting, transaction -> {
            transaction.executeDdl(sql);
        });
    }

    /**
     * execute query.
     *
     * @param sql    SQL
     * @param action The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public void executeAndForEach(String sql, TsurugiTransactionConsumer<TsurugiResultEntity> action) throws IOException, InterruptedException {
        executeAndForEach(defaultSetting(), sql, TgResultMapping.DEFAULT, action);
    }

    /**
     * execute query.
     *
     * @param setting transaction manager settings
     * @param sql     SQL
     * @param action  The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public void executeAndForEach(TgTmSetting setting, String sql, TsurugiTransactionConsumer<TsurugiResultEntity> action) throws IOException, InterruptedException {
        executeAndForEach(setting, sql, TgResultMapping.DEFAULT, action);
    }

    /**
     * execute query.
     *
     * @param <R>           result type
     * @param sql           SQL
     * @param resultMapping result mapping
     * @param action        The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> void executeAndForEach(String sql, TgResultMapping<R> resultMapping, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException {
        executeAndForEach(defaultSetting(), sql, resultMapping, action);
    }

    /**
     * execute query.
     *
     * @param <R>           result type
     * @param setting       transaction manager settings
     * @param sql           SQL
     * @param resultMapping result mapping
     * @param action        The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> void executeAndForEach(TgTmSetting setting, String sql, TgResultMapping<R> resultMapping, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createQuery(sql, resultMapping)) {
            executeAndForEach(setting, ps, action);
        }
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param action           The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> void executeAndForEach(String sql, TgParameterMapping<P> parameterMapping, P parameter, TsurugiTransactionConsumer<TsurugiResultEntity> action)
            throws IOException, InterruptedException {
        executeAndForEach(defaultSetting(), sql, parameterMapping, parameter, TgResultMapping.DEFAULT, action);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param setting          transaction manager settings
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param action           The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> void executeAndForEach(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter, TsurugiTransactionConsumer<TsurugiResultEntity> action)
            throws IOException, InterruptedException {
        executeAndForEach(setting, sql, parameterMapping, parameter, TgResultMapping.DEFAULT, action);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param resultMapping    result mapping
     * @param action           The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> void executeAndForEach(String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping, TsurugiTransactionConsumer<R> action)
            throws IOException, InterruptedException {
        executeAndForEach(defaultSetting(), sql, parameterMapping, parameter, resultMapping, action);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param setting          transaction manager settings
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param resultMapping    result mapping
     * @param action           The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> void executeAndForEach(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping, TsurugiTransactionConsumer<R> action)
            throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            executeAndForEach(setting, ps, parameter, action);
        }
    }

    /**
     * execute query.
     *
     * @param <R>    result type
     * @param ps     PreparedStatement
     * @param action The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> void executeAndForEach(TsurugiSqlQuery<R> ps, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException {
        executeAndForEach(defaultSetting(), ps, action);
    }

    /**
     * execute query.
     *
     * @param <R>     result type
     * @param setting transaction manager settings
     * @param ps      PreparedStatement
     * @param action  The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> void executeAndForEach(TgTmSetting setting, TsurugiSqlQuery<R> ps, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException {
        execute(setting, transaction -> {
            transaction.executeAndForEach(ps, action);
        });
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @param action    The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> void executeAndForEach(TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException {
        executeAndForEach(defaultSetting(), ps, parameter, action);
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param setting   transaction manager settings
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @param action    The action to be performed for each record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> void executeAndForEach(TgTmSetting setting, TsurugiSqlPreparedQuery<P, R> ps, P parameter, TsurugiTransactionConsumer<R> action) throws IOException, InterruptedException {
        execute(setting, transaction -> {
            transaction.executeAndForEach(ps, parameter, action);
        });
    }

    /**
     * execute query.
     *
     * @param sql SQL
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public List<TsurugiResultEntity> executeAndGetList(String sql) throws IOException, InterruptedException {
        return executeAndGetList(defaultSetting(), sql, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param setting transaction manager settings
     * @param sql     SQL
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public List<TsurugiResultEntity> executeAndGetList(TgTmSetting setting, String sql) throws IOException, InterruptedException {
        return executeAndGetList(setting, sql, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param <R>           result type
     * @param sql           SQL
     * @param resultMapping result mapping
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> List<R> executeAndGetList(String sql, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        return executeAndGetList(defaultSetting(), sql, resultMapping);
    }

    /**
     * execute query.
     *
     * @param <R>           result type
     * @param setting       transaction manager settings
     * @param sql           SQL
     * @param resultMapping result mapping
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> List<R> executeAndGetList(TgTmSetting setting, String sql, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createQuery(sql, resultMapping)) {
            return executeAndGetList(setting, ps);
        }
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> List<TsurugiResultEntity> executeAndGetList(String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException, InterruptedException {
        return executeAndGetList(defaultSetting(), sql, parameterMapping, parameter, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param setting          transaction manager settings
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> List<TsurugiResultEntity> executeAndGetList(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException, InterruptedException {
        return executeAndGetList(setting, sql, parameterMapping, parameter, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param resultMapping    result mapping
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> List<R> executeAndGetList(String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        return executeAndGetList(defaultSetting(), sql, parameterMapping, parameter, resultMapping);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param setting          transaction manager settings
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param resultMapping    result mapping
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> List<R> executeAndGetList(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping)
            throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            return executeAndGetList(setting, ps, parameter);
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  PreparedStatement
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> List<R> executeAndGetList(TsurugiSqlQuery<R> ps) throws IOException, InterruptedException {
        return executeAndGetList(defaultSetting(), ps);
    }

    /**
     * execute query.
     *
     * @param <R>     result type
     * @param setting transaction manager settings
     * @param ps      PreparedStatement
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> List<R> executeAndGetList(TgTmSetting setting, TsurugiSqlQuery<R> ps) throws IOException, InterruptedException {
        return execute(setting, transaction -> {
            return transaction.executeAndGetList(ps);
        });
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> List<R> executeAndGetList(TsurugiSqlPreparedQuery<P, R> ps, P parameter) throws IOException, InterruptedException {
        return executeAndGetList(defaultSetting(), ps, parameter);
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param setting   transaction manager settings
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return list of record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> List<R> executeAndGetList(TgTmSetting setting, TsurugiSqlPreparedQuery<P, R> ps, P parameter) throws IOException, InterruptedException {
        return execute(setting, transaction -> {
            return transaction.executeAndGetList(ps, parameter);
        });
    }

    /**
     * execute query.
     *
     * @param sql SQL
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public Optional<TsurugiResultEntity> executeAndFindRecord(String sql) throws IOException, InterruptedException {
        return executeAndFindRecord(defaultSetting(), sql, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param setting transaction manager settings
     * @param sql     SQL
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public Optional<TsurugiResultEntity> executeAndFindRecord(TgTmSetting setting, String sql) throws IOException, InterruptedException {
        return executeAndFindRecord(setting, sql, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param <R>           result type
     * @param sql           SQL
     * @param resultMapping result mapping
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> Optional<R> executeAndFindRecord(String sql, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        return executeAndFindRecord(defaultSetting(), sql, resultMapping);
    }

    /**
     * execute query.
     *
     * @param <R>           result type
     * @param setting       transaction manager settings
     * @param sql           SQL
     * @param resultMapping result mapping
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> Optional<R> executeAndFindRecord(TgTmSetting setting, String sql, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createQuery(sql, resultMapping)) {
            return executeAndFindRecord(setting, ps);
        }
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> Optional<TsurugiResultEntity> executeAndFindRecord(String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException, InterruptedException {
        return executeAndFindRecord(defaultSetting(), sql, parameterMapping, parameter, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param setting          transaction manager settings
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> Optional<TsurugiResultEntity> executeAndFindRecord(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException, InterruptedException {
        return executeAndFindRecord(setting, sql, parameterMapping, parameter, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param resultMapping    result mapping
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> Optional<R> executeAndFindRecord(String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping) throws IOException, InterruptedException {
        return executeAndFindRecord(defaultSetting(), sql, parameterMapping, parameter, resultMapping);
    }

    /**
     * execute query.
     *
     * @param <P>              parameter type
     * @param <R>              result type
     * @param setting          transaction manager settings
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @param resultMapping    result mapping
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> Optional<R> executeAndFindRecord(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping)
            throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            return executeAndFindRecord(setting, ps, parameter);
        }
    }

    /**
     * execute query.
     *
     * @param <R> result type
     * @param ps  PreparedStatement
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> Optional<R> executeAndFindRecord(TsurugiSqlQuery<R> ps) throws IOException, InterruptedException {
        return executeAndFindRecord(defaultSetting(), ps);
    }

    /**
     * execute query.
     *
     * @param <R>     result type
     * @param setting transaction manager settings
     * @param ps      PreparedStatement
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <R> Optional<R> executeAndFindRecord(TgTmSetting setting, TsurugiSqlQuery<R> ps) throws IOException, InterruptedException {
        return execute(setting, transaction -> {
            return transaction.executeAndFindRecord(ps);
        });
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param setting   transaction manager settings
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> Optional<R> executeAndFindRecord(TsurugiSqlPreparedQuery<P, R> ps, P parameter) throws IOException, InterruptedException {
        return executeAndFindRecord(defaultSetting(), ps, parameter);
    }

    /**
     * execute query.
     *
     * @param <P>       parameter type
     * @param <R>       result type
     * @param setting   transaction manager settings
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return record
     * @throws IOException
     * @throws InterruptedException
     */
    public <P, R> Optional<R> executeAndFindRecord(TgTmSetting setting, TsurugiSqlPreparedQuery<P, R> ps, P parameter) throws IOException, InterruptedException {
        return execute(setting, transaction -> {
            return transaction.executeAndFindRecord(ps, parameter);
        });
    }

    /**
     * execute statement.
     *
     * @param sql SQL
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public int executeAndGetCount(String sql) throws IOException, InterruptedException {
        return executeAndGetCount(defaultSetting(), sql);
    }

    /**
     * execute statement.
     *
     * @param setting transaction manager settings
     * @param sql     SQL
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public int executeAndGetCount(TgTmSetting setting, String sql) throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createStatement(sql)) {
            return executeAndGetCount(setting, ps);
        }
    }

    /**
     * execute statement.
     *
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> int executeAndGetCount(String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException, InterruptedException {
        return executeAndGetCount(defaultSetting(), sql, parameterMapping, parameter);
    }

    /**
     * execute statement.
     *
     * @param setting          transaction manager settings
     * @param sql              SQL
     * @param parameterMapping parameter mapping
     * @param parameter        SQL parameter
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> int executeAndGetCount(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException, InterruptedException {
        var session = getSession();
        try (var ps = session.createStatement(sql, parameterMapping)) {
            return executeAndGetCount(setting, ps, parameter);
        }
    }

    /**
     * execute statement.
     *
     * @param ps PreparedStatement
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public int executeAndGetCount(TsurugiSqlStatement ps) throws IOException, InterruptedException {
        return executeAndGetCount(defaultSetting(), ps);
    }

    /**
     * execute statement.
     *
     * @param setting transaction manager settings
     * @param ps      PreparedStatement
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public int executeAndGetCount(TgTmSetting setting, TsurugiSqlStatement ps) throws IOException, InterruptedException {
        return execute(setting, transaction -> {
            return transaction.executeAndGetCount(ps);
        });
    }

    /**
     * execute statement.
     *
     * @param <P>       parameter type
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> int executeAndGetCount(TsurugiSqlPreparedStatement<P> ps, P parameter) throws IOException, InterruptedException {
        return executeAndGetCount(defaultSetting(), ps, parameter);
    }

    /**
     * execute statement.
     *
     * @param <P>       parameter type
     * @param setting   transaction manager settings
     * @param ps        PreparedStatement
     * @param parameter SQL parameter
     * @return row count
     * @throws IOException
     * @throws InterruptedException
     */
    public <P> int executeAndGetCount(TgTmSetting setting, TsurugiSqlPreparedStatement<P> ps, P parameter) throws IOException, InterruptedException {
        return execute(setting, transaction -> {
            return transaction.executeAndGetCount(ps, parameter);
        });
    }
}
