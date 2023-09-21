package com.tsurugidb.iceaxe.transaction.manager.exception;

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi TransactionManager Retry Over Exception.
 */
@SuppressWarnings("serial")
public class TsurugiTmRetryOverIOException extends TsurugiTmIOException {

    /**
     * Creates a new instance.
     *
     * @param transaction  transaction
     * @param cause        the cause
     * @param nextTmOption next transaction option
     */
    @IceaxeInternal
    public TsurugiTmRetryOverIOException(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
        super("transaction retry over", transaction, cause, nextTmOption);
    }
}
