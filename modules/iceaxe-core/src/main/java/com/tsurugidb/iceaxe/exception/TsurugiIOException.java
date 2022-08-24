package com.tsurugidb.iceaxe.exception;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;
import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * Tsurugi IOException
 */
@SuppressWarnings("serial")
public class TsurugiIOException extends IOException implements TsurugiDiagnosticCodeProvider {

    public TsurugiIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public TsurugiIOException(ServerException e) {
        super(TsurugiDiagnosticCodeProvider.createMessage(e), e);
    }

    @Override
    public DiagnosticCode getLowDiagnosticCode() {
        return TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(getCause()).map(e -> e.getLowDiagnosticCode()).orElse(null);
    }
}
