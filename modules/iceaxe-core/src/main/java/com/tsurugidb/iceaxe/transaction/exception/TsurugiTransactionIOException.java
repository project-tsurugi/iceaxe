package com.tsurugidb.iceaxe.transaction.exception;

import java.text.MessageFormat;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi Transaction IOException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionIOException extends TsurugiIOException {

    private final int iceaxeExecuteId;
    private final int attempt;
    private final TgTxOption option;

    // internal
    public TsurugiTransactionIOException(String message, int iceaxeExecuteId, int attempt, TgTxOption option, Exception cause) {
        super(createMessage(message, iceaxeExecuteId, attempt, option), cause);
        this.iceaxeExecuteId = iceaxeExecuteId;
        this.attempt = attempt;
        this.option = option;
    }

    private static String createMessage(String message, int iceaxeExecuteId, int attempt, TgTxOption option) {
        return message + MessageFormat.format(". iceaxeExecuteId={0}, attempt={1}, tx={2}", iceaxeExecuteId, attempt, option);
    }

    /**
     * get iceaxe executeId
     *
     * @return iceaxe executeId
     */
    public int getIceaxeExecuteId() {
        return this.iceaxeExecuteId;
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
