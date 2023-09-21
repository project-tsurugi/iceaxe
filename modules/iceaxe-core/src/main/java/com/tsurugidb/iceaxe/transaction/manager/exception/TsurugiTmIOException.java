package com.tsurugidb.iceaxe.transaction.manager.exception;

import java.io.IOException;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi TransactionManager IOException.
 */
@SuppressWarnings("serial")
public class TsurugiTmIOException extends TsurugiIOException {

    private final TsurugiTransaction transaction;
    private final TgTmTxOption nextTmOption;

    /**
     * Creates a new instance.
     *
     * @param message      the detail message
     * @param transaction  transaction
     * @param cause        the cause
     * @param nextTmOption next transaction option
     */
    @IceaxeInternal
    public TsurugiTmIOException(String message, TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        super(createMessage(message, transaction, nextTmOption), cause);
        this.transaction = transaction;
        this.nextTmOption = nextTmOption;
    }

    private static String createMessage(String message, TsurugiTransaction transaction, TgTmTxOption nextTmOption) {
        return message + ". " + transaction + ", nextTx=" + nextTmOption;
    }

    @Override
    public int getIceaxeTxId() {
        return transaction.getIceaxeTxId();
    }

    @Override
    public int getIceaxeTmExecuteId() {
        return transaction.getIceaxeTmExecuteId();
    }

    @Override
    public int getAttempt() {
        return transaction.getAttempt();
    }

    @Override
    public TgTxOption getTransactionOption() {
        return transaction.getTransactionOption();
    }

    @Override
    public String getTransactionId() {
        try {
            return transaction.getTransactionId();
        } catch (IOException | InterruptedException e) {
            return null;
        }
    }

    /**
     * get next transaction option.
     *
     * @return transaction option
     */
    public TgTmTxOption getNextTmOption() {
        return this.nextTmOption;
    }
}
