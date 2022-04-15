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
     * @return return value
     * @throws IOException
     * @see TsurugiPreparedStatement
     */
    public <R> R execute(List<TgTransactionOption> transactionOptionList, IoFunction<TsurugiTransaction, R> action) throws IOException {
        if (action == null) {
            throw new IllegalArgumentException("action is not specified");
        }
        if (transactionOptionList == null || transactionOptionList.isEmpty()) {
            transactionOptionList = this.defaultTransactionOptionList;
            if (transactionOptionList == null || transactionOptionList.isEmpty()) {
                throw new IllegalArgumentException("transactionOptionList is not specified");
            }
        }
        for (var option : transactionOptionList) {
            try (var transaction = ownerSession.createTransaction(option)) {
                boolean doRollback = true;
                try {
                    var r = action.apply(transaction);
                    transaction.commit();
                    doRollback = false;
                    return r;
                } catch (TsurugiTransactionIOException | TsurugiTransactionUncheckedIOException e) {
                    if (e.isRetryable()) {
                        // リトライ可能なabortの場合でもrollbackは呼ぶ
                        continue;
                    }
                    if (e instanceof TsurugiTransactionUncheckedIOException) {
                        throw ((TsurugiTransactionUncheckedIOException) e).getCause();
                    }
                    throw e;
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                } finally {
                    if (doRollback) {
                        transaction.rollback();
                    }
                }
            }
        }
        throw new TsurugiTransactionIOException("transaction retry over");
    }
}
