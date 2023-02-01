package com.tsurugidb.iceaxe.transaction.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionTask;
import com.tsurugidb.iceaxe.transaction.manager.event.TgTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi Transaction Manager
 * <p>
 * Thread Safe (excluding setTimeout)
 * </p>
 */
@ThreadSafe
public class TsurugiTransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTransactionManager.class);

    private final TsurugiSession ownerSession;
    private final TgTmSetting defaultSetting;
    private final List<TgTmEventListener> eventListenerList = new ArrayList<>();

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
    public TsurugiTransactionManager addEventListener(TgTmEventListener listener) {
        eventListenerList.add(listener);
        return this;
    }

    /**
     * execute transaction
     *
     * @param action action
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public void execute(TsurugiTransactionAction action) throws IOException {
        execute(defaultSetting(), action);
    }

    /**
     * execute transaction
     *
     * @param setting transaction manager settings
     * @param action  action
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public void execute(TgTmSetting setting, TsurugiTransactionAction action) throws IOException {
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
     * @see TsurugiPreparedStatement
     */
    public <R> R execute(TsurugiTransactionTask<R> action) throws IOException {
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
     * @see TsurugiPreparedStatement
     */
    public <R> R execute(TgTmSetting setting, TsurugiTransactionTask<R> action) throws IOException {
        LOG.trace("tm.execute start");
        if (setting == null) {
            throw new IllegalArgumentException("setting is not specified");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }

        var option = setting.getFirstTransactionOption();
        {
            var finalOption = option;
            event(setting, null, listener -> listener.executeStart(finalOption));
        }
        for (int attempt = 0;; attempt++) {
            LOG.trace("tm.execute attempt={}, tx={}", attempt, option);
            {
                int finalAttempt = attempt;
                var finalOption = option;
                event(setting, null, listener -> listener.transactionBefore(finalAttempt, finalOption));
            }

            TsurugiTransaction lastTransaction = null;
            try (var transaction = ownerSession.createTransaction(option)) {
                lastTransaction = transaction;
                transaction.setOwner(this, attempt);
                setting.initializeTransaction(transaction);
                event(setting, null, listener -> listener.transactionCreated(transaction));

                try {
                    R r = action.run(transaction);
                    if (transaction.isRollbacked()) {
                        LOG.trace("tm.execute end (rollbacked)");
                        event(setting, null, listener -> listener.executeEndSuccess(transaction, false, r));
                        return r;
                    }
                    var info = ownerSession.getSessionInfo();
                    var commitType = setting.getCommitType(info);
                    transaction.commit(commitType);
                    LOG.trace("tm.execute end (committed)");
                    event(setting, null, listener -> listener.executeEndSuccess(transaction, true, r));
                    return r;
                } catch (TsurugiTransactionException e) {
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    option = processTransactionException(setting, transaction, e, attempt, option, e);
                    continue;
                } catch (TsurugiTransactionRuntimeException e) {
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    var c = e.getCause();
                    option = processTransactionException(setting, transaction, e, attempt, option, c);
                    continue;
                } catch (Exception e) {
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    var c = findTransactionException(e);
                    if (c == null) {
                        LOG.trace("tm.execute error", e);
                        rollback(setting, transaction, e);
                        throw e;
                    }
                    option = processTransactionException(setting, transaction, e, attempt, option, c);
                    continue;
                } catch (Throwable e) {
                    LOG.trace("tm.execute error", e);
                    event(setting, e, listener -> listener.transactionException(transaction, e));
                    rollback(setting, transaction, e);
                    throw e;
                }
            } catch (Throwable e) {
                {
                    var finalOption = option;
                    var finalTransaction = lastTransaction;
                    event(setting, e, listener -> listener.executeEndFail(finalOption, finalTransaction, e));
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

    private TgTxOption processTransactionException(TgTmSetting setting, TsurugiTransaction transaction, Exception cause, int attempt, TgTxOption option, TsurugiTransactionException e)
            throws IOException {
        boolean calledRollback = false;
        try {
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

                var nextOption = nextTmOption.getOption();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("tm.execute retry{}. e={}, nextTx={}", attempt + 1, e.getMessage(), nextOption);
                }
                event(setting, cause, listener -> listener.transactionRetry(transaction, cause, nextOption));
                return nextOption;
            }

            LOG.trace("tm.execute error", e);
            if (nextTmOption.isRetryOver()) {
                event(setting, cause, listener -> listener.transactionRetryOver(transaction, cause));
                throw new TsurugiTransactionRetryOverIOException(attempt, option, cause);
            } else {
                throw new TsurugiTransactionIOException(cause.getMessage(), attempt, option, cause);
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

    private void event(TgTmSetting setting, Throwable occurred, Consumer<TgTmEventListener> action) {
        try {
            for (var listener : eventListenerList) {
                action.accept(listener);
            }
            for (var listener : setting.getEventListener()) {
                action.accept(listener);
            }
        } catch (Throwable e) {
            if (occurred != null) {
                e.addSuppressed(occurred);
            }
            throw e;
        }
    }

    // execute statement

    /**
     * execute ddl.
     *
     * @param sql DDL
     * @throws IOException
     */
    public void executeDdl(String sql) throws IOException {
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
     */
    public void executeDdl(TgTmSetting setting, String sql) throws IOException {
        execute(setting, transaction -> {
            transaction.executeDdl(sql);
        });
    }

    /**
     * execute query.
     *
     * @param sql SQL
     * @return record
     * @throws IOException
     */
    public Optional<TsurugiResultEntity> executeAndFindRecord(String sql) throws IOException {
        return executeAndFindRecord(defaultSetting(), sql, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param setting transaction manager settings
     * @param sql     SQL
     * @return record
     * @throws IOException
     */
    public Optional<TsurugiResultEntity> executeAndFindRecord(TgTmSetting setting, String sql) throws IOException {
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
     */
    public <R> Optional<R> executeAndFindRecord(String sql, TgResultMapping<R> resultMapping) throws IOException {
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
     */
    public <R> Optional<R> executeAndFindRecord(TgTmSetting setting, String sql, TgResultMapping<R> resultMapping) throws IOException {
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
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
     */
    public <P> Optional<TsurugiResultEntity> executeAndFindRecord(String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException {
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
     */
    public <P> Optional<TsurugiResultEntity> executeAndFindRecord(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException {
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
     */
    public <P, R> Optional<R> executeAndFindRecord(String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping) throws IOException {
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
     */
    public <P, R> Optional<R> executeAndFindRecord(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping) throws IOException {
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
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
     */
    public <R> Optional<R> executeAndFindRecord(TsurugiPreparedStatementQuery0<R> ps) throws IOException {
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
     */
    public <R> Optional<R> executeAndFindRecord(TgTmSetting setting, TsurugiPreparedStatementQuery0<R> ps) throws IOException {
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
     */
    public <P, R> Optional<R> executeAndFindRecord(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) throws IOException {
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
     */
    public <P, R> Optional<R> executeAndFindRecord(TgTmSetting setting, TsurugiPreparedStatementQuery1<P, R> ps, P parameter) throws IOException {
        return execute(setting, transaction -> {
            return transaction.executeAndFindRecord(ps, parameter);
        });
    }

    /**
     * execute query.
     *
     * @param sql SQL
     * @return list of record
     * @throws IOException
     */
    public List<TsurugiResultEntity> executeAndGetList(String sql) throws IOException {
        return executeAndGetList(defaultSetting(), sql, TgResultMapping.DEFAULT);
    }

    /**
     * execute query.
     *
     * @param setting transaction manager settings
     * @param sql     SQL
     * @return list of record
     * @throws IOException
     */
    public List<TsurugiResultEntity> executeAndGetList(TgTmSetting setting, String sql) throws IOException {
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
     */
    public <R> List<R> executeAndGetList(String sql, TgResultMapping<R> resultMapping) throws IOException {
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
     */
    public <R> List<R> executeAndGetList(TgTmSetting setting, String sql, TgResultMapping<R> resultMapping) throws IOException {
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
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
     */
    public <P> List<TsurugiResultEntity> executeAndGetList(String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException {
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
     */
    public <P> List<TsurugiResultEntity> executeAndGetList(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException {
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
     */
    public <P, R> List<R> executeAndGetList(String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping) throws IOException {
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
     */
    public <P, R> List<R> executeAndGetList(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter, TgResultMapping<R> resultMapping) throws IOException {
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
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
     */
    public <R> List<R> executeAndGetList(TsurugiPreparedStatementQuery0<R> ps) throws IOException {
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
     */
    public <R> List<R> executeAndGetList(TgTmSetting setting, TsurugiPreparedStatementQuery0<R> ps) throws IOException {
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
     */
    public <P, R> List<R> executeAndGetList(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) throws IOException {
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
     */
    public <P, R> List<R> executeAndGetList(TgTmSetting setting, TsurugiPreparedStatementQuery1<P, R> ps, P parameter) throws IOException {
        return execute(setting, transaction -> {
            return transaction.executeAndGetList(ps, parameter);
        });
    }

    /**
     * execute statement.
     *
     * @param sql SQL
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(String sql) throws IOException {
        return executeAndGetCount(defaultSetting(), sql);
    }

    /**
     * execute statement.
     *
     * @param setting transaction manager settings
     * @param sql     SQL
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TgTmSetting setting, String sql) throws IOException {
        var session = getSession();
        try (var ps = session.createPreparedStatement(sql)) {
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
     */
    public <P> int executeAndGetCount(String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException {
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
     */
    public <P> int executeAndGetCount(TgTmSetting setting, String sql, TgParameterMapping<P> parameterMapping, P parameter) throws IOException {
        var session = getSession();
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            return executeAndGetCount(setting, ps, parameter);
        }
    }

    /**
     * execute statement.
     *
     * @param ps PreparedStatement
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TsurugiPreparedStatementUpdate0 ps) throws IOException {
        return executeAndGetCount(defaultSetting(), ps);
    }

    /**
     * execute statement.
     *
     * @param setting transaction manager settings
     * @param ps      PreparedStatement
     * @return row count
     * @throws IOException
     */
    public int executeAndGetCount(TgTmSetting setting, TsurugiPreparedStatementUpdate0 ps) throws IOException {
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
     */
    public <P> int executeAndGetCount(TsurugiPreparedStatementUpdate1<P> ps, P parameter) throws IOException {
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
     */
    public <P> int executeAndGetCount(TgTmSetting setting, TsurugiPreparedStatementUpdate1<P> ps, P parameter) throws IOException {
        return execute(setting, transaction -> {
            return transaction.executeAndGetCount(ps, parameter);
        });
    }
}
