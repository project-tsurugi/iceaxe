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

    private final int attempt;
    private final TgTxOption option;

    // internal
    public TsurugiTransactionIOException(String message, int attempt, TgTxOption option, Exception cause) {
        super(createMessage(message, attempt, option), cause);
        this.attempt = attempt;
        this.option = option;
    }

    private static String createMessage(String message, int attempt, TgTxOption option) {
        return message + MessageFormat.format(". attempt={0}, option={1}", attempt, option);
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
