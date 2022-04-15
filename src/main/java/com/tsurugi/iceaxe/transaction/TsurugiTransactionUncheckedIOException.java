package com.tsurugi.iceaxe.transaction;

import java.io.UncheckedIOException;

/**
 * Tsurugi Transaction UncheckedIOException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionUncheckedIOException extends UncheckedIOException implements TsurugiTransactionException {

    // internal
    public TsurugiTransactionUncheckedIOException(TsurugiTransactionIOException cause) {
        super(cause);
    }

    @Override
    public TsurugiTransactionIOException getCause() {
        return (TsurugiTransactionIOException) super.getCause();
    }

    @Override
    public boolean isRetryable() {
        return getCause().isRetryable();
    }
}
