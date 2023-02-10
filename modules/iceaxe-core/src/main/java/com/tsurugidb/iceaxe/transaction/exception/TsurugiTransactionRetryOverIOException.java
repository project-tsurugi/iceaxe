package com.tsurugidb.iceaxe.transaction.exception;

import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Tsurugi Transaction Retry Over Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionRetryOverIOException extends TsurugiTransactionIOException {

    // internal
    public TsurugiTransactionRetryOverIOException(int iceaxeExecuteId, int attempt, TgTxOption option, Exception cause) {
        super("transaction retry over", iceaxeExecuteId, attempt, option, cause);
    }
}
