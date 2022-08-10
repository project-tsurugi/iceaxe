package com.tsurugidb.iceaxe.transaction.exception;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;
import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * Tsurugi Transaction Exception
 */
@SuppressWarnings("serial")
public class TsurugiTransactionException extends Exception {

    private final DiagnosticCode diagnosticCode;

    // internal
    public TsurugiTransactionException(ServerException cause) {
        super(createMessage(cause), cause);
        this.diagnosticCode = cause.getDiagnosticCode();
    }

    /**
     * Tsurugi Transaction Exception
     * 
     * @param code structured diagnostic code
     */
    public TsurugiTransactionException(DiagnosticCode code) {
        super(createMessage(code));
        this.diagnosticCode = code;
    }

    private static String createMessage(ServerException e) {
        return createMessage(e.getDiagnosticCode());
    }

    private static String createMessage(DiagnosticCode code) {
        return code.toString();
    }

    public DiagnosticCode getDiagnosticCode() {
        return this.diagnosticCode;
    }
}
