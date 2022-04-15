package com.tsurugi.iceaxe.transaction;

// FIXME わざわざTsurugiTransactionExceptionインターフェースを設けず、判定の際はTTUnchekcedIOExceptionからTTIOExceptionを取得する？
/**
 * Tsurugi Transaction Exception
 */
public interface TsurugiTransactionException {

    /**
     * is retryable
     * 
     * @return true if retryable
     */
    public boolean isRetryable();
}
