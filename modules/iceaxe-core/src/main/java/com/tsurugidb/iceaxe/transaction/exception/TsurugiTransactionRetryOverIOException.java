package com.tsurugidb.iceaxe.transaction.exception;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

/**
 * Tsurugi Transaction Retry Over Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionRetryOverIOException extends TsurugiTransactionIOException {

    // internal
    public TsurugiTransactionRetryOverIOException(TsurugiTransaction transaction, Exception cause) {
        super("transaction retry over", transaction, cause);
    }
}
