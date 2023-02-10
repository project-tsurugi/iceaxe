package com.tsurugidb.iceaxe.transaction.exception;

import java.text.MessageFormat;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi Transaction IOException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionIOException extends TsurugiIOException {

    private final int iceaxeTransactionId;
    private final int iceaxeTmExecuteId;
    private final int attempt;
    private final TgTxOption option;

    // internal
    public TsurugiTransactionIOException(String message, TsurugiTransaction transaction, Exception cause) {
        super(createMessage(message, transaction), cause);
        this.iceaxeTransactionId = transaction.getIceaxeTransactionId();
        this.iceaxeTmExecuteId = transaction.getIceaxeTmExecuteId();
        this.attempt = transaction.getAttempt();
        this.option = transaction.getTransactionOption();
    }

    private static String createMessage(String message, TsurugiTransaction transaction) {
        int iceaxeTransactionId = transaction.getIceaxeTransactionId();
        int iceaxeTmExecuteId = transaction.getIceaxeTmExecuteId();
        int attempt = transaction.getAttempt();
        var option = transaction.getTransactionOption();
        return message + MessageFormat.format(". iceaxeTxId={0}, iceaxeTmExecuteId={1}, attempt={2}, tx={3}", iceaxeTransactionId, iceaxeTmExecuteId, attempt, option);
    }

    /**
     * get iceaxe transactionId
     *
     * @return iceaxe transactionId
     */
    public int getIceaxeTransactionId() {
        return this.iceaxeTransactionId;
    }

    /**
     * get iceaxe tm executeId
     *
     * @return iceaxe tm executeId
     */
    public int getIceaxeTmExecuteId() {
        return this.iceaxeTmExecuteId;
    }

    /**
     * get attempt number
     *
     * @return attempt number
     * @see TgTmTxOptionSupplier#get(int, TsurugiTransactionException)
     */
    public int getAttempt() {
        return this.attempt;
    }

    /**
     * get transaction option
     *
     * @return transaction option
     */
    public TgTxOption getTransactionOption() {
        return this.option;
    }
}
