package com.tsurugidb.iceaxe.transaction.exception;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;

/**
 * Tsurugi Transaction RuntimeException
 */
@SuppressWarnings("serial")
public class TsurugiTransactionRuntimeException extends RuntimeException implements TsurugiDiagnosticCodeProvider {

    /**
     * Tsurugi Transaction RuntimeException
     * 
     * @param cause TsurugiTransactionException
     */
    public TsurugiTransactionRuntimeException(TsurugiTransactionException cause) {
        super(cause.getMessage(), cause);
    }

    @Override
    public TsurugiTransactionException getCause() {
        return (TsurugiTransactionException) super.getCause();
    }

    @Override
    public DiagnosticCode getLowDiagnosticCode() {
        return getCause().getLowDiagnosticCode();
    }
}
