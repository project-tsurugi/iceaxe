package com.tsurugidb.iceaxe.exception;

import java.io.IOException;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Tsurugi IOException
 */
@SuppressWarnings("serial")
public class TsurugiIOException extends IOException implements TsurugiDiagnosticCodeProvider {

    public TsurugiIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public TsurugiIOException(ServerException cause) {
        super(TsurugiDiagnosticCodeProvider.createMessage(cause), cause);
    }

    @Override
    public DiagnosticCode getLowDiagnosticCode() {
        return TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(getCause()).map(e -> e.getLowDiagnosticCode()).orElse(null);
    }
}
