package com.tsurugidb.iceaxe.transaction.exception;

import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Tsurugi Transaction Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionException extends Exception implements TsurugiDiagnosticCodeProvider {

    // internal
    public TsurugiTransactionException(ServerException cause) {
        super(TsurugiDiagnosticCodeProvider.createMessage(cause), cause);
    }

    /**
     * Creates a new instance.
     *
     * @param message the detail message
     * @param code    diagnostic code
     * @param cause   the cause
     */
    public TsurugiTransactionException(String message, DiagnosticCode code, Throwable cause) {
        super(message, new ServerException(TsurugiDiagnosticCodeProvider.createMessage(code), cause) {
            @Override
            public DiagnosticCode getDiagnosticCode() {
                return code;
            }
        });
    }

    @Override
    public ServerException getCause() {
        return (ServerException) super.getCause();
    }

    @Override
    public DiagnosticCode getDiagnosticCode() {
        return getCause().getDiagnosticCode();
    }
}
