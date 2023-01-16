package com.tsurugidb.iceaxe.transaction.manager;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRetryOverIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionTask;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTxState;
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

    /**
     * Listener called when retrying
     */
    @FunctionalInterface
    public interface TsurugiTransactionManagerRetryListener {
        /**
         * Listener called when retrying
         *
         * @param transaction transaction
         * @param e           exception
         * @param nextOption  next transaction option
         */
        void accept(TsurugiTransaction transaction, Exception e, TgTxOption nextOption);
    }

    /**
     * Listener called on finish
     */
    @FunctionalInterface
    public interface TsurugiTransactionManagerFinishListener {
        /**
         * Listener called on finish
         *
         * @param transaction transaction
         * @param committed   {@code true} committed, {@code false} rollbacked
         * @param returnValue action return value
         */
        void accept(TsurugiTransaction transaction, boolean committed, Object returnValue);
    }

    private final TsurugiSession ownerSession;
    private final TgTmSetting defaultSetting;
    private Consumer<TsurugiTransaction> startHook;
    private TsurugiTransactionManagerRetryListener retryListener;
    private TsurugiTransactionManagerFinishListener finishListener;
    private BiConsumer<TsurugiTransaction, Throwable> rollbackListener;

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
     * set start hook
     *
     * @param hook start hook
     */
    public void setStartHook(@Nullable Consumer<TsurugiTransaction> hook) {
        this.startHook = hook;
    }

    /**
     * set retry listener
     *
     * @param listener retry listener
     */
    public void setRetryListener(@Nullable TsurugiTransactionManagerRetryListener listener) {
        this.retryListener = listener;
    }

    /**
     * set finish listener
     *
     * @param listener finish listener
     */
    public void setFinishListener(@Nullable TsurugiTransactionManagerFinishListener listener) {
        this.finishListener = listener;
    }

    /**
     * set rollback listener
     *
     * @param listener rollback listener
     */
    public void setRollbackListener(@Nullable BiConsumer<TsurugiTransaction, Throwable> listener) {
        this.rollbackListener = listener;
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

        var state = setting.getTransactionOption(0, null, null);
        assert state.isExecute();
        var option = state.getOption();
        LOG.trace("tm.execute tx={}", option);
        for (int i = 0;; i++) {
            try (var transaction = ownerSession.createTransaction(option)) {
                transaction.setOwner(this, i);
                setting.initializeTransaction(transaction);
                if (this.startHook != null) {
                    startHook.accept(transaction);
                }

                try {
                    R r = action.run(transaction);
                    if (transaction.isRollbacked()) {
                        LOG.trace("tm.execute end (rollbacked)");
                        if (this.finishListener != null) {
                            finishListener.accept(transaction, false, r);
                        }
                        return r;
                    }
                    var info = ownerSession.getSessionInfo();
                    var commitType = setting.getCommitType(info);
                    transaction.commit(commitType);
                    LOG.trace("tm.execute end (committed)");
                    if (this.finishListener != null) {
                        finishListener.accept(transaction, true, r);
                    }
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
                nextState = setting.getTransactionOption(i + 1, transaction, e);
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
                if (this.retryListener != null) {
                    retryListener.accept(transaction, cause, nextOption);
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
                if (this.rollbackListener != null) {
                    rollbackListener.accept(transaction, save);
                }
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
}
