package com.tsurugidb.iceaxe.transaction.exception;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;

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
