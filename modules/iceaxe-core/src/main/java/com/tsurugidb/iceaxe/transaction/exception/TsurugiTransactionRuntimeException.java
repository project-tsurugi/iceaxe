package com.tsurugidb.iceaxe.transaction.exception;

import java.util.Optional;

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
    public DiagnosticCode getDiagnosticCode() {
        return getCause().getDiagnosticCode();
    }

    @Override
    public Optional<TsurugiTransactionException> findTransactionException() {
        return Optional.of(getCause());
    }
}
