package com.tsurugidb.iceaxe.transaction.manager.retry;

/**
 * Tsurugi TransactionManager retry code.
 */
public enum TgTmRetryStandardCode implements TgTmRetryCode {

    /** not retryable */
    NOT_RETRYABLE(false),

    /** retryable. by serialization failure */
    RETRYABLE(true),
    /** retryable to LTX. by conflict on write preserve */
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
