package com.tsurugidb.iceaxe.transaction;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

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

    // internal
    public TsurugiTransactionManager(TsurugiSession session, TgTmSetting defaultSetting) {
        this.ownerSession = session;
        this.defaultSetting = defaultSetting;
    }

    protected final TgTmSetting defaultSetting() {
        if (this.defaultSetting == null) {
            throw new IllegalStateException("defaultSetting is not specified");
        }
        return this.defaultSetting;
    }

    @FunctionalInterface
    public interface TsurugiTransactionConsumer {
        public void accept(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException;
    }

    /**
     * execute transaction
     * 
     * @param action action
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public void execute(TsurugiTransactionConsumer action) throws IOException {
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
    public void execute(TgTmSetting setting, TsurugiTransactionConsumer action) throws IOException {
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }
        execute(setting, transaction -> {
            action.accept(transaction);
            return null;
        });
    }

    @FunctionalInterface
    public interface TsurugiTransactionFuntion<R> {
        public R apply(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException;
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
    public <R> R execute(TsurugiTransactionFuntion<R> action) throws IOException {
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
    public <R> R execute(TgTmSetting setting, TsurugiTransactionFuntion<R> action) throws IOException {
        LOG.trace("tm.execute start");
        if (setting == null) {
            throw new IllegalArgumentException("setting is not specified");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }

        var option = setting.getTransactionOption(0, null).getOption();
        LOG.trace("tm.execute tx={}", option);
        for (int i = 0;; i++) {
            try (var transaction = ownerSession.createTransaction(option)) {
                setting.initializeTransaction(transaction);

                try {
                    var r = action.apply(transaction);
                    if (transaction.isRollbacked()) {
                        LOG.trace("tm.execute end (rollbacked)");
                        return r;
                    }
                    var info = ownerSession.getSessionInfo();
                    var commitType = setting.getCommitType(info);
                    transaction.commit(commitType);
                    LOG.trace("tm.execute end (committed)");
                    return r;
                } catch (TsurugiTransactionException e) {
                    option = processTransactionException(setting, transaction, e, i, option, e);
                    continue;
                } catch (TsurugiTransactionRuntimeException e) {
                    var c = e.getCause();
                    option = processTransactionException(setting, transaction, e, i, option, c);
                    continue;
                } catch (Exception e) {
                    var c = findTransactionException(e);
                    if (c == null) {
                        LOG.trace("tm.execute error", e);
                        rollback(transaction, e);
                        throw e;
                    }
                    option = processTransactionException(setting, transaction, e, i, option, c);
                    continue;
                } catch (Throwable e) {
                    LOG.trace("tm.execute error", e);
                    rollback(transaction, e);
                    throw e;
                }
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

    private TgTxOption processTransactionException(TgTmSetting setting, TsurugiTransaction transaction, Exception cause, int i, TgTxOption option, TsurugiTransactionException e) throws IOException {
        boolean calledRollback = false;
        try {
            TgTxState nextState;
            try {
                nextState = setting.getTransactionOption(i + 1, e);
            } catch (Throwable t) {
                t.addSuppressed(cause);
                throw t;
            }

            if (nextState.isExecute()) {
                try {
                    // リトライ可能なabortの場合でもrollbackは呼ぶ
                    rollback(transaction, null);
                } finally {
                    calledRollback = true;
                }

                var nextOption = nextState.getOption();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("tm.execute retry{}. e={}, nextTx={}", i + 1, e.getMessage(), nextOption);
                }
                return nextOption;
            }

            LOG.trace("tm.execute error", e);
            throw createException(nextState, i, option, cause);
        } catch (Throwable t) {
            if (!calledRollback) {
                rollback(transaction, t);
            }
            throw t;
        }
    }

    private IOException createException(TgTxState state, int attemt, TgTxOption option, Exception cause) throws IOException {
        if (state.isRetryOver()) {
            return new TsurugiTransactionRetryOverIOException(attemt, option, cause);
        } else {
            return new TsurugiTransactionIOException(cause.getMessage(), attemt, option, cause);
        }
    }

    private void rollback(TsurugiTransaction transaction, Throwable save) throws IOException {
        try {
            if (transaction.available()) {
                transaction.rollback();
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
                throw new IOException(e);
            }
        }
    }
}
