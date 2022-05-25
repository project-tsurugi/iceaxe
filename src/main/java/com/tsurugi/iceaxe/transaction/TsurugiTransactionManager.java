package com.tsurugi.iceaxe.transaction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugi.iceaxe.util.IoConsumer;
import com.tsurugi.iceaxe.util.IoFunction;

/**
 * Tsurugi Transaction Manager
 */
public class TsurugiTransactionManager {

    private final TsurugiSession ownerSession;
    private final List<TgTransactionOption> defaultTransactionOptionList;

    // internal
    public TsurugiTransactionManager(TsurugiSession session, List<TgTransactionOption> defaultTransactionOptionList) {
        this.ownerSession = session;
        this.defaultTransactionOptionList = defaultTransactionOptionList;
    }

    /**
     * execute transaction
     * 
     * @param action action
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public void execute(IoConsumer<TsurugiTransaction> action) throws IOException {
        execute(defaultTransactionOptionList, action);
    }

    /**
     * execute transaction
     * 
     * @param transactionOptionList transaction option
     * @param action                action
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public void execute(List<TgTransactionOption> transactionOptionList, IoConsumer<TsurugiTransaction> action) throws IOException {
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }
        execute(transactionOptionList, transaction -> {
            action.accept(transaction);
            return null;
        });
    }

    /**
     * execute transaction
     * 
     * @param <R>                   return type
     * @param transactionOptionList transaction option
     * @param action                action
     * @return return value (null if transaction is rollbacked)
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public <R> R execute(IoFunction<TsurugiTransaction, R> action) throws IOException {
        var transactionOptionList = this.defaultTransactionOptionList;
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            throw new IllegalStateException("defaultTransactionOptionList is not specified");
        }
        return execute(transactionOptionList, action);
    }

    /**
     * execute transaction
     * 
     * @param <R>                   return type
     * @param transactionOptionList transaction option
     * @param action                action
     * @return return value (null if transaction is rollbacked)
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public <R> R execute(List<TgTransactionOption> transactionOptionList, IoFunction<TsurugiTransaction, R> action) throws IOException {
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            throw new IllegalArgumentException("transactionOptionList is not specified");
        }
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }
        for (var option : transactionOptionList) {
            try (var transaction = ownerSession.createTransaction(option)) {
                boolean doRollback = true;
                try {
                    var r = action.apply(transaction);
                    if (transaction.isRollbacked()) {
                        doRollback = false;
                        return null;
                    }
                    transaction.commit();
                    doRollback = false;
                    return r;
                } catch (TsurugiTransactionIOException e) {
                    if (isRetryable(e)) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ（doRollback=true）
                        continue;
                    }
                    throw e;
                } catch (UncheckedIOException e) {
                    var c = e.getCause();
                    if (isRetryable(c)) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ（doRollback=true）
                        continue;
                    }
                    throw c;
                } catch (Exception e) {
                    if (isRetryable(e)) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ（doRollback=true）
                        continue;
                    }
                    throw e;
                } finally {
                    if (doRollback) {
                        transaction.rollback();
                    }
                }
            }
        }
        throw new TsurugiTransactionIOException("transaction retry over");
    }

    protected boolean isRetryable(Exception e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TsurugiTransactionIOException) {
                return isRetryable((TsurugiTransactionIOException) t);
            }
        }
        return false;
    }

    protected boolean isRetryable(TsurugiTransactionIOException e) {
        return e.isRetryable();
    }
}
