package com.tsurugidb.iceaxe.exception;

import java.io.IOException;
import java.util.Optional;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Tsurugi IOException
 */
@SuppressWarnings("serial")
public class TsurugiIOException extends IOException implements TsurugiDiagnosticCodeProvider {

    private Optional<DiagnosticCode> diagnosticCode;

    // internal
    public TsurugiIOException(String message, Throwable cause) {
        super(message, cause);
    }

    // internal
    public TsurugiIOException(ServerException cause) {
        super(TsurugiDiagnosticCodeProvider.createMessage(cause), cause);
    }

    // internal
    public TsurugiIOException(IceaxeErrorCode code) {
        super(code.getMessage());
        this.diagnosticCode = Optional.of(code);
    }

    @Override
    public DiagnosticCode getDiagnosticCode() {
        if (this.diagnosticCode == null) {
            this.diagnosticCode = TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(getCause()).map(e -> e.getDiagnosticCode());
        }
        return diagnosticCode.orElse(null);
    }
}
