package com.tsurugidb.iceaxe.transaction;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * Tsurugi Transaction Retry Over Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionRetryOverIOException extends IOException {

    private final int attempt;
    private final TgTxOption option;

    // internal
    public TsurugiTransactionRetryOverIOException(int attempt, TgTxOption option, Exception cause) {
        super(createMessage(attempt, option), cause);
        this.attempt = attempt;
        this.option = option;
    }

    private static String createMessage(int attempt, TgTxOption option) {
        return MessageFormat.format("transaction retry over. last attempt={0}, option={1}", attempt, option);
    }

    /**
     * get attempt number
     * 
     * @return attempt number
     * @see TgTxOptionSupplier#get(int, TsurugiTransactionException)
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
