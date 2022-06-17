package com.tsurugidb.iceaxe.transaction;

/**
 * Tsurugi Transaction RuntimeException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionRuntimeException extends RuntimeException {

    /**
     * Tsurugi Transaction RuntimeException
     * 
     * @param cause TsurugiTransactionException
     */
    public TsurugiTransactionRuntimeException(TsurugiTransactionException cause) {
        super(cause);
        assert cause != null;
    }

    @Override
    public TsurugiTransactionException getCause() {
        return (TsurugiTransactionException) super.getCause();
    }
}
