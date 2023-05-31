package com.tsurugidb.iceaxe.transaction.manager.retry;

/**
 * Tsurugi TransactionManager retry code.
 */
public interface TgTmRetryCode {

    /**
     * whether to retry
     *
     * @return {@code true}: retryable
     */
    public boolean isRetryable();
}
