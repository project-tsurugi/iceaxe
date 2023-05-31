package com.tsurugidb.iceaxe.transaction.manager.retry;

/**
 * Tsurugi TransactionManager retry code.
 */
public enum TgTmRetryStandardCode implements TgTmRetryCode {

    /** not retryable */
    NOT_RETRYABLE(false),

    /** retryable. by ERR_ABORTED_RETRYABLE */
    RETRYABLE(true),
    /** retryable to LTX. by ERR_CONFLICT_ON_WRITE_PRESERVE */
    RETRYABLE_LTX(true),

    ;

    private final boolean retryable;

    private TgTmRetryStandardCode(boolean retryable) {
        this.retryable = retryable;
    }

    @Override
    public boolean isRetryable() {
        return this.retryable;
    }
}
