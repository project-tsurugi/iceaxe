package com.tsurugidb.iceaxe.transaction.manager.exception;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;

/**
 * Tsurugi TransactionManager Retry Over Exception
 */
@SuppressWarnings("serial")
public class TsurugiTmRetryOverIOException extends TsurugiTmIOException {

    // internal
    public TsurugiTmRetryOverIOException(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        super("transaction retry over", transaction, cause, nextTmOption);
    }
}
