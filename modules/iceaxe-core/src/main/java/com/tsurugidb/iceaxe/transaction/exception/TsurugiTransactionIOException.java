package com.tsurugidb.iceaxe.transaction.exception;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi Transaction IOException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionIOException extends TsurugiIOException {

    private final int iceaxeTxId;
    private final int iceaxeTmExecuteId;
    private final int attempt;
    private final TgTxOption txOption;

    // internal
    public TsurugiTransactionIOException(String message, TsurugiTransaction transaction, Exception cause) {
        super(createMessage(message, transaction), cause);
        this.iceaxeTxId = transaction.getIceaxeTxId();
        this.iceaxeTmExecuteId = transaction.getIceaxeTmExecuteId();
        this.attempt = transaction.getAttempt();
        this.txOption = transaction.getTransactionOption();
    }

    private static String createMessage(String message, TsurugiTransaction transaction) {
        return message + ". " + transaction;
    }

    @Override
    public int getIceaxeTxId() {
        return this.iceaxeTxId;
    }

    @Override
    public int getIceaxeTmExecuteId() {
        return this.iceaxeTmExecuteId;
    }

    @Override
    public int getAttempt() {
        return this.attempt;
    }

    @Override
    public TgTxOption getTransactionOption() {
        return this.txOption;
    }
}
