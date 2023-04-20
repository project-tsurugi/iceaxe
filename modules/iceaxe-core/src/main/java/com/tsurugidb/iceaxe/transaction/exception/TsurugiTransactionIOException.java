package com.tsurugidb.iceaxe.transaction.exception;

import java.io.IOException;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi Transaction IOException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionIOException extends TsurugiIOException {

    private final TsurugiTransaction transaction;

    // internal
    public TsurugiTransactionIOException(String message, TsurugiTransaction transaction, Exception cause) {
        super(createMessage(message, transaction), cause);
        this.transaction = transaction;
    }

    private static String createMessage(String message, TsurugiTransaction transaction) {
        return message + ". " + transaction;
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
}
