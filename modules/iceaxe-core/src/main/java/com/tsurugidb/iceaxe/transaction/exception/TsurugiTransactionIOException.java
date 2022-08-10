package com.tsurugidb.iceaxe.transaction.exception;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Optional;

import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgTxOptionSupplier;

/**
 * Tsurugi Transaction IOException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionIOException extends IOException {

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
     * get Transaction Exception
     * 
     * @return Transaction Exception
     */
    public Optional<TsurugiTransactionException> findTransactionException() {
        for (Throwable t = getCause(); t != null; t = t.getCause()) {
            if (t instanceof TsurugiTransactionException) {
                return Optional.of((TsurugiTransactionException) t);
            }
        }
        return Optional.empty();
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
