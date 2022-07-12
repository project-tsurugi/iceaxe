package com.tsurugidb.iceaxe.transaction;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatement;

/**
 * Tsurugi Transaction Manager
 * <p>
 * Thread Safe (excluding setTimeout)
 * </p>
 */
@ThreadSafe
public class TsurugiTransactionManager {

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
        if (setting == null) {
            throw new IllegalArgumentException("setting is not specified");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }

        var option = setting.getTransactionOption(0, null);
        assert option != null;
        for (int i = 0;; i++) {
            try (var transaction = ownerSession.createTransaction(option)) {
                setting.initializeTransaction(transaction);

                try {
                    var r = action.apply(transaction);
                    if (transaction.isRollbacked()) {
                        return null;
                    }
                    var info = ownerSession.getSessionInfo();
                    var commitType = setting.getCommitType(info);
                    transaction.commit(commitType);
                    return r;
                } catch (TsurugiTransactionException e) {
                    var prevOption = option;
                    option = setting.getTransactionOption(i + 1, e);
                    if (option != null) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ
                        rollback(transaction, null);
                        continue;
                    }
                    var ioe = new TsurugiTransactionRetryOverIOException(i, prevOption, e);
                    rollback(transaction, ioe);
                    throw ioe;
                } catch (TsurugiTransactionRuntimeException e) {
                    var c = e.getCause();
                    var prevOption = option;
                    option = setting.getTransactionOption(i + 1, c);
                    if (option != null) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ
                        rollback(transaction, null);
                        continue;
                    }
                    var ioe = new TsurugiTransactionRetryOverIOException(i, prevOption, e);
                    rollback(transaction, ioe);
                    throw ioe;
                } catch (Exception e) {
                    var c = findTransactionException(e);
                    if (c != null) {
                        var prevOption = option;
                        option = setting.getTransactionOption(i + 1, c);
                        if (option != null) {
                            // リトライ可能なabortの場合でもrollbackは呼ぶ
                            rollback(transaction, null);
                            continue;
                        }
                        var ioe = new TsurugiTransactionRetryOverIOException(i, prevOption, e);
                        rollback(transaction, ioe);
                        throw ioe;
                    }
                    rollback(transaction, e);
                    throw e;
                } catch (Throwable e) {
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
